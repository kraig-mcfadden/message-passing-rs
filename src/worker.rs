use crate::{
    Message, MessageConsumer, MessageConsumerFactory, MessageConsumptionError,
    MessageConsumptionOutcome, MessageFactory,
};
use std::sync::Arc;

pub struct Worker<M: Message> {
    message_factory: Arc<dyn MessageFactory<M>>,
    message_consumer_factory: Arc<dyn MessageConsumerFactory<M>>,
}

impl<M: Message> Worker<M> {
    pub fn new(
        message_factory: Arc<dyn MessageFactory<M>>,
        message_consumer_factory: Arc<dyn MessageConsumerFactory<M>>,
    ) -> Self {
        Self {
            message_factory,
            message_consumer_factory,
        }
    }

    // TODO: make number of worker processes to start be configurable -- or have a way to instantiate multiple workers
    pub async fn run(&self) {
        loop {
            match self.message_factory.get_messages().await {
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
                self.message_factory
                    .delete_message(&message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to delete message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Ok(MessageConsumptionOutcome::Ignored) => {
                log::debug!("Ignoring message {message_id:?}. Deleting.");
                self.message_factory
                    .delete_message(&message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to delete message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Err(MessageConsumptionError::Transient) => {
                log::info!("Retrying transient error for message {message_id:?}.");
                self.message_factory
                    .requeue_message(&message_id)
                    .await
                    .map_err(|e| {
                        format!("Failed to requeue message with id {message_id:?} with error {e:?}")
                    })?;
            }
            Err(MessageConsumptionError::Unrecoverable) => {
                log::info!("Sending unprocessable message {message_id:?} to dead letter queue.");
                self.message_factory
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
    use std::sync::Mutex;

    use super::*;
    use crate::MessageFactoryError;
    use async_trait::async_trait;

    #[tokio::test]
    async fn test_process_success() {
        // given
        let mf = Arc::new(MockMessageFactory::new());
        let mcf = Arc::new(MockMessageConsumerFactory {
            consumer: MockMessageConsumer {
                should_err: false,
                err: MessageConsumptionError::Transient,
                success: MessageConsumptionOutcome::Succeeded,
            },
        });
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
        let mf = Arc::new(MockMessageFactory::new());
        let mcf = Arc::new(MockMessageConsumerFactory {
            consumer: MockMessageConsumer {
                should_err: false,
                err: MessageConsumptionError::Transient,
                success: MessageConsumptionOutcome::Ignored,
            },
        });
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
        let mf = Arc::new(MockMessageFactory::new());
        let mcf = Arc::new(MockMessageConsumerFactory {
            consumer: MockMessageConsumer {
                should_err: true,
                err: MessageConsumptionError::Transient,
                success: MessageConsumptionOutcome::Succeeded,
            },
        });
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
        let mf = Arc::new(MockMessageFactory::new());
        let mcf = Arc::new(MockMessageConsumerFactory {
            consumer: MockMessageConsumer {
                should_err: true,
                err: MessageConsumptionError::Unrecoverable,
                success: MessageConsumptionOutcome::Succeeded,
            },
        });
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

    #[derive(Debug, Clone, Copy)]
    struct MockMessage;

    impl Message for MockMessage {
        type MessageId = u32;
        type MessageType = &'static str;
        type MessageContent = MockMessage;

        fn message_id(&self) -> &Self::MessageId {
            &1
        }

        fn message_type(&self) -> &Self::MessageType {
            &"foo"
        }

        fn content(&self) -> &Self::MessageContent {
            self
        }
    }

    struct MockMessageFactory {
        deleted: Mutex<Vec<<MockMessage as Message>::MessageId>>,
        requeued: Mutex<Vec<<MockMessage as Message>::MessageId>>,
        dlq: Mutex<Vec<<MockMessage as Message>::MessageId>>,
    }

    impl MockMessageFactory {
        fn new() -> Self {
            Self {
                deleted: Mutex::new(Vec::new()),
                requeued: Mutex::new(Vec::new()),
                dlq: Mutex::new(Vec::new()),
            }
        }

        fn deleted(&self) -> Vec<<MockMessage as Message>::MessageId> {
            self.deleted.lock().unwrap().clone()
        }

        fn requeued(&self) -> Vec<<MockMessage as Message>::MessageId> {
            self.requeued.lock().unwrap().clone()
        }

        fn dlq(&self) -> Vec<<MockMessage as Message>::MessageId> {
            self.dlq.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl MessageFactory<MockMessage> for MockMessageFactory {
        async fn get_messages(&self) -> Result<Vec<MockMessage>, MessageFactoryError>
        where
            MockMessage: 'async_trait,
        {
            return Ok(vec![MockMessage]);
        }

        async fn delete_message(
            &self,
            message_id: &<MockMessage as Message>::MessageId,
        ) -> Result<(), MessageFactoryError>
        where
            MockMessage: 'async_trait,
        {
            self.deleted.lock().unwrap().push(*message_id);
            Ok(())
        }

        // in some concrete technologies this will not require any action
        async fn requeue_message(
            &self,
            message_id: &<MockMessage as Message>::MessageId,
        ) -> Result<(), MessageFactoryError>
        where
            MockMessage: 'async_trait,
        {
            self.requeued.lock().unwrap().push(*message_id);
            Ok(())
        }

        async fn dlq_message(
            &self,
            message_id: &<MockMessage as Message>::MessageId,
        ) -> Result<(), MessageFactoryError>
        where
            MockMessage: 'async_trait,
        {
            self.dlq.lock().unwrap().push(*message_id);
            Ok(())
        }
    }

    struct MockMessageConsumer {
        should_err: bool,
        err: MessageConsumptionError,
        success: MessageConsumptionOutcome,
    }

    #[async_trait]
    impl MessageConsumer<MockMessage> for MockMessageConsumer {
        async fn consume(
            &self,
            _message: MockMessage,
        ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>
        where
            MockMessage: 'async_trait,
        {
            if self.should_err {
                Err(self.err.clone())
            } else {
                Ok(self.success.clone())
            }
        }
    }

    struct MockMessageConsumerFactory {
        consumer: MockMessageConsumer,
    }

    impl MessageConsumerFactory<MockMessage> for MockMessageConsumerFactory {
        fn consumer(
            &self,
            _message_type: &<MockMessage as Message>::MessageType,
        ) -> Option<&dyn MessageConsumer<MockMessage>> {
            Some(&self.consumer)
        }
    }
}
