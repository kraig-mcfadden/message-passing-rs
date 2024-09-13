use super::MessageImplSqs;
use crate::{Message, MessageClientError, MessagePubClient, MessageSubClient};
use async_trait::async_trait;
use aws_sdk_sqs::{types::SendMessageBatchRequestEntry, Client};

pub struct MessageClientImplSqs {
    sqs_client: Client,
    queue_url: String,
    dlq_url: String,
    max_number_of_messages: i32,
    wait_time_seconds: i32,
}

impl MessageClientImplSqs {
    pub async fn init(queue_url: impl Into<String>, dlq_url: impl Into<String>) -> Self {
        let config = aws_config::load_from_env().await;
        Self {
            sqs_client: Client::new(&config),
            queue_url: queue_url.into(),
            dlq_url: dlq_url.into(),
            max_number_of_messages: 10,
            wait_time_seconds: 20,
        }
    }
}

#[async_trait]
impl MessagePubClient<MessageImplSqs> for MessageClientImplSqs {
    // TODO: error handling in unrecoverable case
    async fn publish_message(
        &self,
        message: <MessageImplSqs as Message>::MessageContent,
    ) -> Result<(), MessageClientError>
    where
        MessageImplSqs: 'async_trait,
    {
        self.sqs_client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(message)
            .send()
            .await
            .map_err(|_| MessageClientError::Transient)?;
        Ok(())
    }

    // TODO: error handling in unrecoverable case
    async fn publish_messages(
        &self,
        messages: Vec<<MessageImplSqs as Message>::MessageContent>,
    ) -> Vec<Result<(), MessageClientError>>
    where
        MessageImplSqs: 'async_trait,
    {
        let entries: Vec<SendMessageBatchRequestEntry> = messages
            .iter()
            .map(|message| {
                SendMessageBatchRequestEntry::builder()
                    .message_body(message)
                    .build()
                    .unwrap()
            })
            .collect();
        let res = self
            .sqs_client
            .send_message_batch()
            .queue_url(&self.queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map(|_| ())
            .map_err(|_| MessageClientError::Transient);
        vec![res]
    }
}

#[async_trait]
impl MessageSubClient<MessageImplSqs> for MessageClientImplSqs {
    // TODO: error handling in unrecoverable case
    async fn get_messages(&self) -> Result<Vec<MessageImplSqs>, MessageClientError>
    where
        MessageImplSqs: 'async_trait,
    {
        let messages = self
            .sqs_client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(self.max_number_of_messages)
            .wait_time_seconds(self.wait_time_seconds)
            .send()
            .await
            .map_err(|_| MessageClientError::Transient)?
            .messages
            .unwrap_or_default()
            .drain(..)
            .map(MessageImplSqs::from)
            .collect();
        Ok(messages)
    }

    // TODO: error handling in unrecoverable case
    async fn delete_message(
        &self,
        message_id: &<MessageImplSqs as Message>::MessageId,
    ) -> Result<(), MessageClientError>
    where
        MessageImplSqs: 'async_trait,
    {
        self.sqs_client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(message_id)
            .send()
            .await
            .map_err(|_| MessageClientError::Transient)?;
        Ok(())
    }

    async fn requeue_message(
        &self,
        _message_id: &<MessageImplSqs as Message>::MessageId,
    ) -> Result<(), MessageClientError>
    where
        MessageImplSqs: 'async_trait,
    {
        // don't need to do anything for SQS to requeue
        Ok(())
    }

    async fn dlq_message(&self, message: &MessageImplSqs) -> Result<(), MessageClientError>
    where
        MessageImplSqs: 'async_trait,
    {
        self.sqs_client
            .send_message()
            .queue_url(&self.dlq_url)
            .message_body(message.content())
            .send()
            .await
            .map_err(|_| MessageClientError::Transient)?;
        Ok(())
    }
}
