use crate::{
    Message, MessageClientError, MessageConsumer, MessageConsumptionError,
    MessageConsumptionOutcome, MessageSubClient,
};
use async_trait::async_trait;
use std::sync::Mutex;

pub(crate) struct MockMessageSubClient {
    deleted: Mutex<Vec<String>>,
    requeued: Mutex<Vec<String>>,
    dlq: Mutex<Vec<String>>,
}

impl MockMessageSubClient {
    pub(crate) fn new() -> Self {
        Self {
            deleted: Mutex::new(Vec::new()),
            requeued: Mutex::new(Vec::new()),
            dlq: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn deleted(&self) -> Vec<String> {
        self.deleted.lock().unwrap().clone()
    }

    pub(crate) fn requeued(&self) -> Vec<String> {
        self.requeued.lock().unwrap().clone()
    }

    pub(crate) fn dlq(&self) -> Vec<String> {
        self.dlq.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageSubClient for MockMessageSubClient {
    async fn get_messages(&self) -> Result<Vec<Message>, MessageClientError> {
        return Ok(vec![Message::new("foo", "bar")]);
    }

    async fn delete_message(&self, message_id: &str) -> Result<(), MessageClientError> {
        self.deleted.lock().unwrap().push(message_id.to_string());
        Ok(())
    }

    // in some concrete technologies this will not require any action
    async fn requeue_message(&self, message_id: &str) -> Result<(), MessageClientError> {
        self.requeued.lock().unwrap().push(message_id.to_string());
        Ok(())
    }

    async fn dlq_message(&self, message: &Message) -> Result<(), MessageClientError> {
        self.dlq.lock().unwrap().push(message.id().to_string());
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
impl MessageConsumer for MockMessageConsumer {
    async fn consume(
        &self,
        _message: &Message,
    ) -> Result<MessageConsumptionOutcome, MessageConsumptionError> {
        if self.should_err {
            Err(self.err.clone())
        } else {
            Ok(self.ok.clone())
        }
    }
}
