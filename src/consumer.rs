use crate::Message;
use async_trait::async_trait;

/// Distinguishing between messages that were actually processed vs ignored
/// can be helpful for tracing and reporting. Both will be deleted from the
/// queue.
#[derive(Debug, Clone)]
pub enum MessageConsumptionOutcome {
    Succeeded,
    Ignored,
}

/// Depending on the error, a message might be retryable. Use these variants to tell
/// the worker what to do with the message
#[derive(Debug, Clone)]
pub enum MessageConsumptionError {
    // can be retried in a bit, like a network issue, so it will go back on the queue immediately
    Transient,

    // requires human intervention, like a config issue, so it will go on the DLQ for the dev to retry later
    Unrecoverable,
}

#[async_trait]
pub trait MessageConsumer<M: Message>: Send + Sync {
    async fn consume(
        &self,
        message: M,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>
    where
        M: 'async_trait;
}

// handy implementation of a noop worker that will just ignore any message given to it
pub struct IgnoreMessageConsumer;

#[async_trait]
impl<M: Message> MessageConsumer<M> for IgnoreMessageConsumer {
    async fn consume(
        &self,
        _message: M,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>
    where
        M: 'async_trait,
    {
        Ok(MessageConsumptionOutcome::Ignored)
    }
}

pub trait MessageConsumerFactory<M: Message>: Send + Sync {
    fn consumer(&self, message_type: &M::MessageType) -> Option<&dyn MessageConsumer<M>>;
}

#[derive(Debug)]
pub enum MessageFactoryError {
    Transient,     // can be retried in a bit, like a network issue
    Unrecoverable, // requires human intervention, like a config issue
}

// This trait abstracts away fetching and parsing logic. Implementations will need to
// know the concrete queue or topic API as well as the message format
#[async_trait]
pub trait MessageFactory<M: Message>: Send + Sync {
    async fn get_messages(&self) -> Result<Vec<M>, MessageFactoryError>
    where
        M: 'async_trait;

    async fn delete_message(&self, message_id: &M::MessageId) -> Result<(), MessageFactoryError>
    where
        M: 'async_trait;

    // in some concrete technologies this will not require any action
    async fn requeue_message(&self, message_id: &M::MessageId) -> Result<(), MessageFactoryError>
    where
        M: 'async_trait;

    async fn dlq_message(&self, message_id: &M::MessageId) -> Result<(), MessageFactoryError>
    where
        M: 'async_trait;
}
