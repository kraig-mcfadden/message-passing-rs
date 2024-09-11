use std::fmt::Debug;

pub trait Message: Send + Sync + Debug {
    type MessageId: Debug + Clone;
    type MessageType;
    type MessageContent; // you probably want this to be an enum with struct variants

    fn message_id(&self) -> &Self::MessageId;
    fn message_type(&self) -> &Self::MessageType;
    fn content(&self) -> &Self::MessageContent;
}
