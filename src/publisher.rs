use crate::Message;
use async_trait::async_trait;

#[derive(Debug)]
pub enum MessagePublisherError {
    Transient,     // can be retried in a bit, like a network issue
    Unrecoverable, // requires human intervention, like a config issue
}

#[async_trait]
pub trait MessagePublisher<M: Message>: Send + Sync {
    async fn publish(&self, message: M) -> Result<(), MessagePublisherError>
    where
        M: 'async_trait;

    // default implementation in case concrete technologies don't allow batch publishing
    async fn publish_batch(&self, messages: Vec<M>) -> Vec<Result<(), MessagePublisherError>>
    where
        M: 'async_trait,
    {
        let mut results = Vec::with_capacity(messages.len());
        for message in messages {
            results.push(self.publish(message).await);
        }
        results
    }
}
