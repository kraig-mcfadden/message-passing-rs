use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Message {
    id: String,
    content: String,
}

impl Message {
    pub fn new(id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            content: content.into(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}
