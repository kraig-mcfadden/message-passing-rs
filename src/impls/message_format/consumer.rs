use crate::{Message, MessageConsumer, MessageConsumptionError, MessageConsumptionOutcome};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::hash::Hash;
use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

pub struct MessageConsumerImpl<MessageType, MessageData> {
    message_content_consumer_factory: MessageContentConsumerFactory<MessageType, MessageData>,
}

impl<MessageType, MessageData> MessageConsumerImpl<MessageType, MessageData> {
    pub fn new(
        message_content_consumer_factory: MessageContentConsumerFactory<MessageType, MessageData>,
    ) -> Self {
        Self {
            message_content_consumer_factory,
        }
    }
}

#[async_trait]
impl<
        M: Message,
        MessageType: Send + Sync + Eq + PartialEq + Hash + Serialize + DeserializeOwned,
        MessageData: Send + Sync + Serialize + DeserializeOwned,
    > MessageConsumer<M> for MessageConsumerImpl<MessageType, MessageData>
where
    M::MessageContent: ToString,
{
    async fn consume(
        &self,
        message: &M,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>
    where
        M: 'async_trait,
    {
        if let Ok(message_content) = MessageContent::from_json(&message.content().to_string()) {
            if let Some(consumer) = self
                .message_content_consumer_factory
                .consumer(&message_content.event_type)
            {
                consumer.consume(message_content).await
            } else {
                Ok(MessageConsumptionOutcome::Ignored)
            }
        } else {
            Err(MessageConsumptionError::Unrecoverable)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageContent<MessageType, MessageData> {
    id: Uuid,
    event_type: MessageType,
    event_at: DateTime<Utc>,
    data: MessageData,
}

impl<MT: Serialize + DeserializeOwned, MD: Serialize + DeserializeOwned> MessageContent<MT, MD> {
    pub fn from_json(json_str: &str) -> Result<Self, String> {
        serde_json::from_str(json_str).map_err(|e| e.to_string())
    }

    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|e| e.to_string())
    }
}

#[async_trait]
pub trait MessageContentConsumer<MessageType, MessageData>: Send + Sync {
    async fn consume(
        &self,
        msg: MessageContent<MessageType, MessageData>,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>;
}

type ThreadSafeMutableState<T> = Arc<Mutex<T>>;
type Consumers<MessageType, MessageData> =
    HashMap<MessageType, Arc<dyn MessageContentConsumer<MessageType, MessageData>>>;

#[derive(Default)]
pub struct MessageContentConsumerFactory<MessageType, MessageData> {
    consumers: ThreadSafeMutableState<Consumers<MessageType, MessageData>>,
}

impl<MessageType: PartialEq + Eq + Hash, MessageData>
    MessageContentConsumerFactory<MessageType, MessageData>
{
    pub fn new() -> Self {
        Self {
            consumers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_consumer(
        &mut self,
        message_type: MessageType,
        consumer: Arc<dyn MessageContentConsumer<MessageType, MessageData>>,
    ) -> &mut Self {
        self.consumers
            .lock()
            .unwrap()
            .insert(message_type, consumer);
        self
    }

    pub fn consumer(
        &self,
        message_type: &MessageType,
    ) -> Option<Arc<dyn MessageContentConsumer<MessageType, MessageData>>> {
        let map = self.consumers.lock().unwrap();
        map.get(message_type).cloned()
    }
}
