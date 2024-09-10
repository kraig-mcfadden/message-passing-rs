use async_trait::async_trait;

/// Distinguishing between messages that were actually processed vs ignored
/// can be helpful for tracing and reporting. Both will be deleted from the
/// queue.
pub enum MessageConsumptionOutcome {
    Succeeded,
    Ignored,
}

/// Depending on the error, a message might be retryable. Use these variants to tell
/// the worker what to do with the message
pub enum MessageConsumptionError {
    Retryable,            // will go back on the queue immediately
    DeadLetterQueueable,  // will go on the DLQ for the dev to retry later
    Unrecoverable,        // will be deleted
}

#[async_trait]
pub trait MessageConsumer<M: Message>: Send + Sync {
    async fn consume(&self, message: M) -> Result<MessageConsumptionOutcome, MessageConsumptionError>;
}

#[async_trait]
pub trait MessageConsumerFactory<M: Message>: Send + Sync {
    async fn consumer(&self, message_type: &M::MessageType) -> Option<impl MessageConsumer<M>>;
}

pub enum MessageFactoryError {
    Transient,      // can be retried in a bit, like a network issue
    Unrecoverable,  // requires human intervention, like a config issue
}

// This trait abstracts away fetching and parsing logic. Implementations will need to
// know the concrete queue or topic API as well as the message format
#[async_trait]
pub trait MessageFactory<M: Message>: Send + Sync {
    async fn get_messages(&self) -> Result<Vec<M>, MessageFactoryError>;
}

pub trait Message: Send + Sync {
    type MessageType;
    type MessageContent;  // you probably want this to be an enum with struct variants

    fn message_type(&self) -> &Self::MessageType;
    fn content(&self) -> &Self::MessageContent;
}
