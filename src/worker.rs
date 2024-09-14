use crate::{
    Message, MessageConsumer, MessageConsumptionError, MessageConsumptionOutcome, MessageSubClient,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Worker {
    message_sub_client: Arc<dyn MessageSubClient>,
    message_consumer: Arc<dyn MessageConsumer>,
}

impl Worker {
    pub fn new(
        message_sub_client: Arc<dyn MessageSubClient>,
        message_consumer: Arc<dyn MessageConsumer>,
    ) -> Self {
        Self {
            message_sub_client,
            message_consumer,
        }
    }

    // TODO: make number of worker processes to start be configurable -- or have a way to instantiate multiple workers
    pub async fn run(&self) {
        loop {
            match self.message_sub_client.get_messages().await {
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
    async fn process_messages(&self, messages: Vec<Message>) {
        for message in messages {
            let res = self.process_message(&message).await;
            if let Err(msg) = res {
                log::error!("Error during message consumption: {msg}");
            }
        }
    }

    async fn process_message(&self, message: &Message) -> Result<(), String> {
        let message_id = message.id();
        match self.message_consumer.consume(message).await {
            Ok(MessageConsumptionOutcome::Succeeded) => {
                log::info!("Successfully processed message {message_id:?}. Deleting.");
                self.message_sub_client
                    .delete_message(message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to delete message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Ok(MessageConsumptionOutcome::Ignored) => {
                log::debug!("Ignoring message {message_id:?}. Deleting.");
                self.message_sub_client
                    .delete_message(message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to delete message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Err(MessageConsumptionError::Transient) => {
                log::info!("Retrying transient error for message {message_id:?}.");
                self.message_sub_client
                    .requeue_message(message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to requeue message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Err(MessageConsumptionError::Unrecoverable) => {
                log::info!("Sending unprocessable message {message_id:?} to dead letter queue.");
                self.message_sub_client
                    .dlq_message(message)
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
    use crate::test_utils::{MockMessageConsumer, MockMessageSubClient};

    #[tokio::test]
    async fn test_process_success() {
        // given
        let mf = Arc::new(MockMessageSubClient::new());
        let mc = Arc::new(MockMessageConsumer::return_ok(
            MessageConsumptionOutcome::Succeeded,
        ));
        let msg = Message::new("foo", "bar");
        let worker = Worker::new(mf.clone(), mc);

        // when
        let res = worker.process_message(&msg).await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.deleted().len(), 1);
        assert_eq!(mf.deleted().first().unwrap(), msg.id());
    }

    #[tokio::test]
    async fn test_process_ignore() {
        // given
        let mf = Arc::new(MockMessageSubClient::new());
        let mc = Arc::new(MockMessageConsumer::return_ok(
            MessageConsumptionOutcome::Ignored,
        ));
        let msg = Message::new("foo", "bar");
        let worker = Worker::new(mf.clone(), mc);

        // when
        let res = worker.process_message(&msg).await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.deleted().len(), 1);
        assert_eq!(mf.deleted().first().unwrap(), msg.id());
    }

    #[tokio::test]
    async fn test_process_err_transient() {
        // given
        let mf = Arc::new(MockMessageSubClient::new());
        let mc = Arc::new(MockMessageConsumer::return_err(
            MessageConsumptionError::Transient,
        ));
        let msg = Message::new("foo", "bar");
        let worker = Worker::new(mf.clone(), mc);

        // when
        let res = worker.process_message(&msg).await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.requeued().len(), 1);
        assert_eq!(mf.requeued().first().unwrap(), msg.id());
    }

    #[tokio::test]
    async fn test_process_err_unrecoverable() {
        // given
        let mf = Arc::new(MockMessageSubClient::new());
        let mc = Arc::new(MockMessageConsumer::return_err(
            MessageConsumptionError::Unrecoverable,
        ));
        let msg = Message::new("foo", "bar");
        let worker = Worker::new(mf.clone(), mc);

        // when
        let res = worker.process_message(&msg).await;

        // then
        assert!(res.is_ok());
        assert_eq!(mf.dlq().len(), 1);
        assert_eq!(mf.dlq().first().unwrap(), msg.id());
    }
}
