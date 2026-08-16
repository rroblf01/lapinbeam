//! Length-prefixed framing: `[u32 LE length][bincode bytes]`.
//!
//! `FrameDecoder` is a pure buffering decoder over `bytes::BytesMut`, so it
//! can be tested without touching any network or Python code.

use bytes::{Buf, BytesMut};

use super::{WireMessage, MAX_FRAME_SIZE};

/// Encodes a message into a single frame (`length` prefix + bincode body).
pub fn encode_frame(msg: &WireMessage) -> Result<Vec<u8>, bincode::error::EncodeError> {
    let body = bincode::serde::encode_to_vec(msg, bincode::config::standard())?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Incremental decoder that returns complete messages as they become available.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buf: BytesMut,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    /// Feeds the buffer with raw bytes and extracts as many complete frames
    /// as possible.
    pub fn decode(&mut self, data: &[u8]) -> Result<Vec<WireMessage>, FrameError> {
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        loop {
            if self.buf.len() < 4 {
                break;
            }
            let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap()) as usize;
            if len > MAX_FRAME_SIZE as usize {
                return Err(FrameError::TooLarge(len));
            }
            if self.buf.len() < 4 + len {
                break;
            }
            self.buf.advance(4);
            let body = self.buf.split_to(len);
            let (msg, consumed): (WireMessage, usize) =
                bincode::serde::decode_from_slice(&body, bincode::config::standard())
                    .map_err(FrameError::Decode)?;
            debug_assert_eq!(consumed, len);
            out.push(msg);
        }
        Ok(out)
    }
}

#[derive(Debug)]
pub enum FrameError {
    /// Declared frame length exceeds `MAX_FRAME_SIZE`.
    TooLarge(usize),
    /// The frame body is not a valid bincode `WireMessage`.
    Decode(bincode::error::DecodeError),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::TooLarge(n) => write!(f, "frame of {n} bytes exceeds limit"),
            FrameError::Decode(e) => write!(f, "frame decode failed: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> WireMessage {
        WireMessage::data(
            1,
            "node_a@10.0.0.1:9001",
            "actor",
            serde_json::json!({"hello": "world"}),
            None,
            None,
        )
    }

    #[test]
    fn encode_frame_has_length_prefix_and_roundtrips() {
        let msg = sample();
        let frame = encode_frame(&msg).unwrap();
        assert_eq!(frame.len(), 4 + frame.len().saturating_sub(4));
        let prefix = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(prefix, frame.len() - 4);

        let mut dec = FrameDecoder::new();
        let msgs = dec.decode(&frame).unwrap();
        assert_eq!(msgs, vec![msg]);
    }

    #[test]
    fn decoder_handles_partial_frames() {
        let msg = sample();
        let frame = encode_frame(&msg).unwrap();

        // Split into tiny chunks: 1 byte at a time.
        let mut dec = FrameDecoder::new();
        let mut collected = Vec::new();
        for byte in &frame {
            let msgs = dec.decode(&[*byte]).unwrap();
            collected.extend(msgs);
        }
        assert_eq!(collected, vec![msg]);
    }

    #[test]
    fn decoder_handles_multiple_messages_in_one_buffer() {
        let msgs = (0..10)
            .map(|i| WireMessage {
                msg_id: i,
                ..sample()
            })
            .collect::<Vec<_>>();
        let mut blob = Vec::new();
        for m in &msgs {
            blob.extend(encode_frame(m).unwrap());
        }
        let mut dec = FrameDecoder::new();
        let got = dec.decode(&blob).unwrap();
        assert_eq!(got, msgs);
    }

    #[test]
    fn decoder_handles_split_across_batches() {
        let msgs = vec![
            sample(),
            WireMessage {
                msg_id: 2,
                ..sample()
            },
        ];
        let mut blob = Vec::new();
        for m in &msgs {
            blob.extend(encode_frame(m).unwrap());
        }
        // Feed in two halves.
        let mid = blob.len() / 2;
        let mut dec = FrameDecoder::new();
        let mut got = dec.decode(&blob[..mid]).unwrap();
        got.extend(dec.decode(&blob[mid..]).unwrap());
        assert_eq!(got, msgs);
    }

    #[test]
    fn decoder_rejects_oversized_frames() {
        let frame: Vec<u8> = {
            let mut f = vec![0u8; 8];
            let big = (MAX_FRAME_SIZE as u64 + 1) as u32;
            f[..4].copy_from_slice(&big.to_le_bytes());
            f
        };
        let mut dec = FrameDecoder::new();
        match dec.decode(&frame) {
            Err(FrameError::TooLarge(_)) => {}
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    #[test]
    fn decoder_rejects_garbage_body() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&4u32.to_le_bytes());
        frame.extend_from_slice(&[0xff, 0xfe, 0xfd, 0xfc]);
        let mut dec = FrameDecoder::new();
        assert!(matches!(dec.decode(&frame), Err(FrameError::Decode(_))));
    }

    #[test]
    fn decoder_skips_leading_noise_after_error_keeps_buffer() {
        // A truncated frame followed later by a complete frame must decode the
        // complete one once enough bytes arrive.
        let msg = sample();
        let full = encode_frame(&msg).unwrap();
        let prefix_len = 4;
        let mut dec = FrameDecoder::new();
        let partial = &full[..prefix_len + 2];
        assert!(dec.decode(partial).unwrap().is_empty());
        let rest = &full[prefix_len + 2..];
        let got = dec.decode(rest).unwrap();
        assert_eq!(got, vec![msg]);
    }
}
