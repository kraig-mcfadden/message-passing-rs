use crate::{Message, MessageConsumer, MessageConsumptionError, MessageConsumptionOutcome};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

pub struct MessageConsumerImpl<MessageData> {
    message_content_consumer_factory: MessageContentConsumerFactory<MessageData>,
}

impl<MessageData> MessageConsumerImpl<MessageData> {
    pub fn new(
        message_content_consumer_factory: MessageContentConsumerFactory<MessageData>,
    ) -> Self {
        Self {
            message_content_consumer_factory,
        }
    }
}

#[async_trait]
impl<MessageData: Send + Sync + Serialize + DeserializeOwned> MessageConsumer
    for MessageConsumerImpl<MessageData>
{
    async fn consume(
        &self,
        message: &Message,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError> {
        if let Ok(message_content) = MessageContent::from_json(message.content()) {
            if let Some(consumer) = self
                .message_content_consumer_factory
                .consumer(&message_content.message_type)
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
pub struct MessageContent<MessageData> {
    id: Uuid,
    message_type: String,
    message_at: DateTime<Utc>, // when the action took place that triggered the message
    published_at: DateTime<Utc>, // when the message was actually published (may be later than the action itself)
    data: MessageData,
}

impl<MessageData> MessageContent<MessageData> {
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    pub fn message_at(&self) -> &DateTime<Utc> {
        &self.message_at
    }

    pub fn published_at(&self) -> &DateTime<Utc> {
        &self.published_at
    }

    pub fn data(&self) -> &MessageData {
        &self.data
    }
}

impl<MessageData: Serialize + DeserializeOwned> MessageContent<MessageData> {
    // will assign an id, so be sure to clone if you intend to reuse the same message
    pub fn create(
        message_type: impl Into<String>,
        message_at: DateTime<Utc>,
        published_at: DateTime<Utc>,
        data: MessageData,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            message_type: message_type.into(),
            message_at,
            published_at,
            data,
        }
    }

    pub fn from_json(json_str: &str) -> Result<Self, String> {
        serde_json::from_str(json_str).map_err(|e| e.to_string())
    }

    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|e| e.to_string())
    }
}

#[async_trait]
pub trait MessageContentConsumer<MessageData>: Send + Sync {
    async fn consume(
        &self,
        msg: MessageContent<MessageData>,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>;
}

type ThreadSafeMutableState<T> = Arc<Mutex<T>>;
type Consumers<MessageData> = HashMap<String, Arc<dyn MessageContentConsumer<MessageData>>>;

#[derive(Default)]
pub struct MessageContentConsumerFactory<MessageData> {
    consumers: ThreadSafeMutableState<Consumers<MessageData>>,
}

impl<MessageData> MessageContentConsumerFactory<MessageData> {
    pub fn new() -> Self {
        Self {
            consumers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_consumer(
        self,
        message_type: impl Into<String>,
        consumer: Arc<dyn MessageContentConsumer<MessageData>>,
    ) -> Self {
        self.consumers
            .lock()
            .unwrap()
            .insert(message_type.into(), consumer);
        self
    }

    pub fn consumer(
        &self,
        message_type: &str,
    ) -> Option<Arc<dyn MessageContentConsumer<MessageData>>> {
        let map = self.consumers.lock().unwrap();
        map.get(message_type).cloned()
    }
}
