use crate::{
    Message, MessageClientError, MessageConsumer, MessageConsumptionError,
    MessageConsumptionOutcome, MessageSubClient,
};
use async_trait::async_trait;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub(crate) struct MockMessage;

impl Message for MockMessage {
    type MessageId = u32;
    type MessageContent = MockMessage;

    fn message_id(&self) -> &Self::MessageId {
        &1
    }
    fn content(&self) -> &Self::MessageContent {
        self
    }
}

pub(crate) struct MockMessageSubClient {
    deleted: Mutex<Vec<<MockMessage as Message>::MessageId>>,
    requeued: Mutex<Vec<<MockMessage as Message>::MessageId>>,
    dlq: Mutex<Vec<<MockMessage as Message>::MessageId>>,
}

impl MockMessageSubClient {
    pub(crate) fn new() -> Self {
        Self {
            deleted: Mutex::new(Vec::new()),
            requeued: Mutex::new(Vec::new()),
            dlq: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn deleted(&self) -> Vec<<MockMessage as Message>::MessageId> {
        self.deleted.lock().unwrap().clone()
    }

    pub(crate) fn requeued(&self) -> Vec<<MockMessage as Message>::MessageId> {
        self.requeued.lock().unwrap().clone()
    }

    pub(crate) fn dlq(&self) -> Vec<<MockMessage as Message>::MessageId> {
        self.dlq.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageSubClient<MockMessage> for MockMessageSubClient {
    async fn get_messages(&self) -> Result<Vec<MockMessage>, MessageClientError>
    where
        MockMessage: 'async_trait,
    {
        return Ok(vec![MockMessage]);
    }

    async fn delete_message(
        &self,
        message_id: &<MockMessage as Message>::MessageId,
    ) -> Result<(), MessageClientError>
    where
        MockMessage: 'async_trait,
    {
        self.deleted.lock().unwrap().push(*message_id);
        Ok(())
    }

    // in some concrete technologies this will not require any action
    async fn requeue_message(
        &self,
        message_id: &<MockMessage as Message>::MessageId,
    ) -> Result<(), MessageClientError>
    where
        MockMessage: 'async_trait,
    {
        self.requeued.lock().unwrap().push(*message_id);
        Ok(())
    }

    async fn dlq_message(&self, message: &MockMessage) -> Result<(), MessageClientError>
    where
        MockMessage: 'async_trait,
    {
        self.dlq.lock().unwrap().push(*message.message_id());
        Ok(())
    }
}

pub(crate) struct MockMessageConsumer {
    should_err: bool,
    err: MessageConsumptionError,
    ok: MessageConsumptionOutcome,
}

impl MockMessageConsumer {
    pub(crate) fn return_err(err: MessageConsumptionError) -> Self {
        Self {
            should_err: true,
            err,
            ok: MessageConsumptionOutcome::Ignored,
        }
    }

    pub(crate) fn return_ok(ok: MessageConsumptionOutcome) -> Self {
        Self {
            should_err: false,
            err: MessageConsumptionError::Transient,
            ok,
        }
    }
}

#[async_trait]
impl MessageConsumer<MockMessage> for MockMessageConsumer {
    async fn consume(
        &self,
        _message: &MockMessage,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError>
    where
        MockMessage: 'async_trait,
    {
        if self.should_err {
            Err(self.err.clone())
        } else {
            Ok(self.ok.clone())
        }
    }
}
