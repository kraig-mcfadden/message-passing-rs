use std::fmt::Debug;

pub trait Message: Send + Sync + Debug + Clone {
    type MessageId: Send + Sync + Debug + Clone;
    type MessageContent: Send + Sync + Debug + Clone;

    fn message_id(&self) -> &Self::MessageId;
    fn content(&self) -> &Self::MessageContent;
}
