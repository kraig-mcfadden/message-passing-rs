use crate::Message;
use async_trait::async_trait;

#[derive(Debug)]
pub enum MessageClientError {
    Transient,     // can be retried in a bit, like a network issue
    Unrecoverable, // requires human intervention, like a config issue
}

// This trait abstracts away message publishing, with serialization/deserialization.
// Implementations will need to know the concrete queue or topic API as well as the message format
#[async_trait]
pub trait MessagePubClient<M: Message>: Send + Sync {
    async fn publish_message(&self, message: M::MessageContent) -> Result<(), MessageClientError>
    where
        M: 'async_trait;

    // default implementation in case concrete technologies don't allow batch publishing
    async fn publish_messages(
        &self,
        messages: Vec<M::MessageContent>,
    ) -> Vec<Result<(), MessageClientError>>
    where
        M: 'async_trait,
    {
        let mut results = Vec::with_capacity(messages.len());
        for message in messages {
            results.push(self.publish_message(message).await);
        }
        results
    }
}

// This trait abstracts away message retrieval and disposal, with serialization/deserialization.
// Implementations will need to know the concrete queue or topic API as well as the message format
#[async_trait]
pub trait MessageSubClient<M: Message>: Send + Sync {
    async fn get_messages(&self) -> Result<Vec<M>, MessageClientError>
    where
        M: 'async_trait;

    async fn delete_message(&self, message_id: &M::MessageId) -> Result<(), MessageClientError>
    where
        M: 'async_trait;

    // in some concrete technologies this will not require any action
    async fn requeue_message(&self, message_id: &M::MessageId) -> Result<(), MessageClientError>
    where
        M: 'async_trait;

    async fn dlq_message(&self, message_id: &M) -> Result<(), MessageClientError>
    where
        M: 'async_trait;
}
