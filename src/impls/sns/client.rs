use super::MessageImplSns;
use crate::{Message, MessageClientError, MessagePubClient};
use async_trait::async_trait;
use aws_sdk_sns::{types::PublishBatchRequestEntry, Client};

pub struct MessageClientImplSns {
    sns_client: Client,
    topic_arn: String,
}

impl MessageClientImplSns {
    pub async fn init(topic_arn: String) -> Self {
        let config = aws_config::load_from_env().await;
        Self {
            sns_client: Client::new(&config),
            topic_arn,
        }
    }
}

#[async_trait]
impl MessagePubClient<MessageImplSns> for MessageClientImplSns {
    // TODO: error handling in unrecoverable case
    async fn publish_message(
        &self,
        message: <MessageImplSns as Message>::MessageContent,
    ) -> Result<(), MessageClientError>
    where
        MessageImplSns: 'async_trait,
    {
        self.sns_client
            .publish()
            .topic_arn(&self.topic_arn)
            .message(message)
            .send()
            .await
            .map_err(|_| MessageClientError::Transient)?;
        Ok(())
    }

    // TODO: error handling in unrecoverable case
    async fn publish_messages(
        &self,
        messages: Vec<<MessageImplSns as Message>::MessageContent>,
    ) -> Vec<Result<(), MessageClientError>>
    where
        MessageImplSns: 'async_trait,
    {
        let entries: Vec<PublishBatchRequestEntry> = messages
            .iter()
            .map(|message| {
                PublishBatchRequestEntry::builder()
                    .message(message)
                    .build()
                    .unwrap()
            })
            .collect();
        let res = self
            .sns_client
            .publish_batch()
            .topic_arn(&self.topic_arn)
            .set_publish_batch_request_entries(Some(entries))
            .send()
            .await
            .map(|_| ())
            .map_err(|_| MessageClientError::Transient);
        vec![res]
    }
}
