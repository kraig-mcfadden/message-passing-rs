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
pub trait MessageConsumer: Send + Sync {
    async fn consume(
        &self,
        message: &Message,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>;
}
