use crate::Message;
use aws_sdk_sqs::types::Message as SqsMessage;

impl From<SqsMessage> for Message {
    fn from(value: SqsMessage) -> Self {
        Message::new_with_receipt_handle(
            value.message_id.unwrap_or_default(),
            value.receipt_handle.unwrap_or_default(),
            value.body.unwrap_or_default(),
        )
    }
}
