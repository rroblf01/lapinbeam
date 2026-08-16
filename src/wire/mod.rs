//! Wire format: `WireMessage` + length-prefixed framing.
//!
//! On the wire every frame is `[u32 LE length][bincode(WireMessage)]`.

pub mod framing;

pub const PROTOCOL_VERSION: u8 = 1;

/// Maximum accepted frame payload size (16 MiB).
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Kind of a wire message. Used to route the frame at the receiving end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MessageKind {
    /// Peer-to-peer handshake exchanged right after the TCP connection.
    Handshake,
    /// Application payload delivered to an actor mailbox.
    Data,
    /// Liveness probe; peers must answer with `Heartbeat`.
    Heartbeat,
    /// Error reported by a peer (e.g. unknown destination actor).
    Error,
}

/// A single message exchanged between two nodes over the wire.
///
/// The application `payload` travels as raw JSON bytes (`serde_json`) inside
/// the bincode envelope. bincode 2 cannot round-trip `serde_json::Value`
/// directly (no `deserialize_any`), so JSON stays self-contained in bytes.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WireMessage {
    pub version: u8,
    pub msg_id: u64,
    /// Sender node id (`node_a@host:port`).
    pub src: String,
    /// Destination actor name.
    pub dst_actor: String,
    pub kind: MessageKind,
    /// JSON bytes of the application payload (JSON-compatible types only).
    pub payload: Vec<u8>,
    /// Optional actor to reply to.
    pub reply_to: Option<String>,
    /// Optional id correlating a request with its response.
    pub correlation_id: Option<u64>,
}

impl WireMessage {
    /// Builds an application `Data` message carrying a JSON payload.
    pub fn data(
        msg_id: u64,
        src: impl Into<String>,
        dst_actor: impl Into<String>,
        payload: serde_json::Value,
        reply_to: Option<String>,
        correlation_id: Option<u64>,
    ) -> Self {
        WireMessage {
            version: PROTOCOL_VERSION,
            msg_id,
            src: src.into(),
            dst_actor: dst_actor.into(),
            kind: MessageKind::Data,
            payload: serde_json::to_vec(&payload)
                .expect("payload must be a JSON-serializable value"),
            reply_to,
            correlation_id,
        }
    }

    /// Builds a liveness probe.
    pub fn heartbeat(msg_id: u64, src: impl Into<String>) -> Self {
        WireMessage {
            version: PROTOCOL_VERSION,
            msg_id,
            src: src.into(),
            dst_actor: String::new(),
            kind: MessageKind::Heartbeat,
            payload: Vec::new(),
            reply_to: None,
            correlation_id: None,
        }
    }

    /// Decodes the JSON payload back into a `serde_json::Value`.
    /// An empty payload is treated as JSON `null`.
    pub fn payload_json(&self) -> serde_json::Result<serde_json::Value> {
        if self.payload.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_slice(&self.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> WireMessage {
        WireMessage::data(
            42,
            "node_a@10.0.0.1:9001",
            "processor",
            serde_json::json!({
                "type": "TASK",
                "payload_id": 7,
                "reply_to": "ingestor",
            }),
            Some("ingestor".into()),
            Some(1),
        )
    }

    #[test]
    fn bincode_roundtrip_preserves_all_fields() {
        let msg = sample();
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (WireMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(decoded.payload_json().unwrap(), msg.payload_json().unwrap());
    }

    #[test]
    fn roundtrip_supports_all_json_payload_types() {
        let payloads = vec![
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(42),
            serde_json::json!(3.5),
            serde_json::json!("text"),
            serde_json::json!([1, "two", 3.0]),
            serde_json::json!({"nested": {"list": [1, 2, 3]}}),
        ];
        for payload in payloads {
            let msg = WireMessage {
                payload: serde_json::to_vec(&payload).unwrap(),
                ..sample()
            };
            let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
            let (decoded, _): (WireMessage, usize) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
            assert_eq!(decoded, msg);
            assert_eq!(decoded.payload_json().unwrap(), payload);
        }
    }

    #[test]
    fn roundtrip_supports_optional_fields() {
        let mut msg = sample();
        msg.reply_to = None;
        msg.correlation_id = None;
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (WireMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(decoded.reply_to, None);
        assert_eq!(decoded.correlation_id, None);
    }

    #[test]
    fn roundtrip_all_message_kinds() {
        for kind in [
            MessageKind::Handshake,
            MessageKind::Data,
            MessageKind::Heartbeat,
            MessageKind::Error,
        ] {
            let msg = WireMessage { kind, ..sample() };
            let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
            let (decoded, _): (WireMessage, usize) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
            assert_eq!(decoded, msg);
        }
    }

    #[test]
    fn heartbeat_payload_is_empty_json_null() {
        let hb = WireMessage::heartbeat(1, "node_a@10.0.0.1:9001");
        assert_eq!(hb.kind, MessageKind::Heartbeat);
        assert_eq!(hb.payload_json().unwrap(), serde_json::Value::Null);
    }
}
