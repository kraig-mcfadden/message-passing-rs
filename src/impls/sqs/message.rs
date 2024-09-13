use crate::Message;
use aws_sdk_sqs::types::Message as SqsMessage;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct MessageImplSqs {
    id: String,
    content: String,
}

impl Message for MessageImplSqs {
    type MessageId = String;
    type MessageContent = String;

    fn message_id(&self) -> &Self::MessageId {
        &self.id
    }

    fn content(&self) -> &Self::MessageContent {
        &self.content
    }
}

impl From<SqsMessage> for MessageImplSqs {
    fn from(value: SqsMessage) -> Self {
        Self {
            id: value.message_id.unwrap_or_default(),
            content: value.body.unwrap_or_default(),
        }
    }
}
