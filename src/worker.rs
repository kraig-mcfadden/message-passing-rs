use crate::{
    Message, MessageClient, MessageConsumer, MessageConsumerFactory, MessageConsumptionError,
    MessageConsumptionOutcome,
};
use std::sync::Arc;

pub struct Worker<M: Message> {
    message_client: Arc<dyn MessageClient<M>>,
    message_consumer_factory: Arc<dyn MessageConsumerFactory<M>>,
}

impl<M: Message> Worker<M> {
    pub fn new(
        message_client: Arc<dyn MessageClient<M>>,
        message_consumer_factory: Arc<dyn MessageConsumerFactory<M>>,
    ) -> Self {
        Self {
            message_client,
            message_consumer_factory,
        }
    }

    // TODO: make number of worker processes to start be configurable -- or have a way to instantiate multiple workers
    pub async fn run(&self) {
        loop {
            match self.message_client.get_messages().await {
                Ok(messages) => {
                    self.process_messages(messages).await;
                }
                // TODO: add backoff here so it doesn't just fail as fast as it can in the event
                // of a network outage
                Err(e) => log::error!("Failed to get messages with error: {e:?}"),
            }
        }
    }

    // TODO: allow concurrency -- i.e. how many messages to process at once -- to be configurable
    async fn process_messages(&self, messages: Vec<M>) {
        for message in messages {
            let maybe_consumer = self
                .message_consumer_factory
                .consumer(message.message_type());

            if maybe_consumer.is_none() {
                log::warn!("No consumer found for message {message:?}");
                continue;
            };

            let res = self.process_message(maybe_consumer.unwrap(), message).await;
            if let Err(msg) = res {
                log::error!("Error during message consumption: {msg}");
            }
        }
    }

    async fn process_message(
        &self,
        consumer: &dyn MessageConsumer<M>,
        message: M,
    ) -> Result<(), String> {
        let message_id = message.message_id().clone();
        match consumer.consume(message).await {
            Ok(MessageConsumptionOutcome::Succeeded) => {
                log::info!("Successfully processed message {message_id:?}. Deleting.");
                self.message_client
                    .delete_message(&message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to delete message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Ok(MessageConsumptionOutcome::Ignored) => {
                log::debug!("Ignoring message {message_id:?}. Deleting.");
                self.message_client
                    .delete_message(&message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to delete message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Err(MessageConsumptionError::Transient) => {
                log::info!("Retrying transient error for message {message_id:?}.");
                self.message_client
                    .requeue_message(&message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to requeue message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Err(MessageConsumptionError::Unrecoverable) => {
                log::info!("Sending unprocessable message {message_id:?} to dead letter queue.");
                self.message_client
                    .dlq_message(&message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to send message {message_id:?} to DLQ with error {e:?}")
                    })?;
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        MockMessage, MockMessageClient, MockMessageConsumer, MockMessageConsumerFactory,
    };

    #[tokio::test]
    async fn test_process_success() {
        // given
        let mf = Arc::new(MockMessageClient::new());
        let mcf = Arc::new(MockMessageConsumerFactory::new(
            MockMessageConsumer::return_ok(MessageConsumptionOutcome::Succeeded),
        ));
        let msg = MockMessage;
        let worker = Worker::new(mf.clone(), mcf.clone());

        // when
        let res = worker
            .process_message(mcf.consumer(&"foo").unwrap(), msg)
            .await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.deleted().len(), 1);
        assert_eq!(mf.deleted().first().unwrap(), msg.message_id());
    }

    #[tokio::test]
    async fn test_process_ignore() {
        // given
        let mf = Arc::new(MockMessageClient::new());
        let mcf = Arc::new(MockMessageConsumerFactory::new(
            MockMessageConsumer::return_ok(MessageConsumptionOutcome::Ignored),
        ));
        let msg = MockMessage;
        let worker = Worker::new(mf.clone(), mcf.clone());

        // when
        let res = worker
            .process_message(mcf.consumer(&"foo").unwrap(), msg)
            .await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.deleted().len(), 1);
        assert_eq!(mf.deleted().first().unwrap(), msg.message_id());
    }

    #[tokio::test]
    async fn test_process_err_transient() {
        // given
        let mf = Arc::new(MockMessageClient::new());
        let mcf = Arc::new(MockMessageConsumerFactory::new(
            MockMessageConsumer::return_err(MessageConsumptionError::Transient),
        ));
        let msg = MockMessage;
        let worker = Worker::new(mf.clone(), mcf.clone());

        // when
        let res = worker
            .process_message(mcf.consumer(&"foo").unwrap(), msg)
            .await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.requeued().len(), 1);
        assert_eq!(mf.requeued().first().unwrap(), msg.message_id());
    }

    #[tokio::test]
    async fn test_process_err_unrecoverable() {
        // given
        let mf = Arc::new(MockMessageClient::new());
        let mcf = Arc::new(MockMessageConsumerFactory::new(
            MockMessageConsumer::return_err(MessageConsumptionError::Unrecoverable),
        ));
        let msg = MockMessage;
        let worker = Worker::new(mf.clone(), mcf.clone());

        // when
        let res = worker
            .process_message(mcf.consumer(&"foo").unwrap(), msg)
            .await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.dlq().len(), 1);
        assert_eq!(mf.dlq().first().unwrap(), msg.message_id());
    }
}
