use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Opaque binary payload. Callers own encoding/decoding.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Payload {
    pub data: Bytes,
    pub metadata: std::collections::HashMap<String, String>,
}

impl Payload {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_bytes(data: impl Into<Bytes>) -> Self {
        Self {
            data: data.into(),
            metadata: Default::default(),
        }
    }

    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        let data = serde_json::to_vec(value)?;
        Ok(Self {
            data: Bytes::from(data),
            metadata: [("encoding".into(), "json".into())].into(),
        })
    }

    pub fn as_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.data)
    }
}
