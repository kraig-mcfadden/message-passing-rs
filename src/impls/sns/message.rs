use crate::Message;
use aws_sdk_sns::operation::publish::PublishOutput;

impl From<(String, PublishOutput)> for Message {
    fn from(value: (String, PublishOutput)) -> Self {
        Message::new(value.1.message_id.unwrap_or_default(), value.0)
    }
}
