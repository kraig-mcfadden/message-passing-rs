use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Message {
    id: String,
    receipt_handle: Option<String>, // defaults to id if not present
    content: String,
}

impl Message {
    pub fn new(id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            receipt_handle: None,
            content: content.into(),
        }
    }

    pub fn new_with_receipt_handle(
        id: impl Into<String>,
        receipt_handle: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            receipt_handle: Some(receipt_handle.into()),
            content: content.into(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn receipt_handle(&self) -> &str {
        if let Some(receipt_handle) = &self.receipt_handle {
            receipt_handle
        } else {
            self.id()
        }
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}
