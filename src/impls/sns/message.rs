use crate::Message;
use aws_sdk_sns::operation::publish::PublishOutput;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct MessageImplSns {
    id: String,
    content: String,
}

impl Message for MessageImplSns {
    type MessageId = String;
    type MessageContent = String;

    fn message_id(&self) -> &Self::MessageId {
        &self.id
    }

    fn content(&self) -> &Self::MessageContent {
        &self.content
    }
}

impl From<(String, PublishOutput)> for MessageImplSns {
    fn from(value: (String, PublishOutput)) -> Self {
        Self {
            id: value.1.message_id.unwrap_or_default(),
            content: value.0,
        }
    }
}
