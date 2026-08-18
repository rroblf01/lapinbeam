//! Shared-secret handshake authentication ("cluster cookie", Erlang-style).
//!
//! Scope, honestly stated: this only proves the *dialer* knows the shared
//! secret before the *acceptor* registers the connection as a peer. It does
//! **not** authenticate the acceptor back to the dialer (a full mutual
//! scheme would also need the acceptor to prove itself), and — since
//! traffic is unencrypted — it does **not** resist a network-position
//! attacker who can capture and replay a previously observed handshake.
//! Both of those would need a real challenge-response protocol or TLS. What
//! this does close is the gap described in docs/index.md's "Security"
//! section: without a matching secret, a handshake is rejected outright, so
//! an arbitrary process reaching the listening port can no longer register
//! itself as a peer just by claiming an id.

use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const NONCE_LEN: usize = 16;
const PROOF_LEN: usize = 32; // HMAC-SHA256 output size

/// Builds a handshake auth payload: a fresh random nonce followed by
/// `HMAC-SHA256(secret, nonce)`. Verified on the other end with
/// [`verify_proof`] using the same secret.
pub fn build_proof(secret: &[u8]) -> Vec<u8> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce);
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&nonce);
    let proof = mac.finalize().into_bytes();

    let mut out = Vec::with_capacity(NONCE_LEN + PROOF_LEN);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&proof);
    out
}

/// Verifies a handshake auth payload built by [`build_proof`] against `secret`.
pub fn verify_proof(secret: &[u8], payload: &[u8]) -> bool {
    if payload.len() != NONCE_LEN + PROOF_LEN {
        return false;
    }
    let (nonce, proof) = payload.split_at(NONCE_LEN);
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(nonce);
    // `verify_slice` does a constant-time comparison, unlike `==` on a Vec.
    mac.verify_slice(proof).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_secret_verifies() {
        let secret = b"cluster-secret";
        let proof = build_proof(secret);
        assert!(verify_proof(secret, &proof));
    }

    #[test]
    fn mismatched_secret_fails() {
        let proof = build_proof(b"cluster-secret");
        assert!(!verify_proof(b"different-secret", &proof));
    }

    #[test]
    fn tampered_proof_fails() {
        let secret = b"cluster-secret";
        let mut proof = build_proof(secret);
        let last = proof.len() - 1;
        proof[last] ^= 0xFF;
        assert!(!verify_proof(secret, &proof));
    }

    #[test]
    fn wrong_length_payload_fails() {
        assert!(!verify_proof(b"cluster-secret", b"too short"));
        assert!(!verify_proof(b"cluster-secret", &[]));
    }

    #[test]
    fn each_proof_uses_a_fresh_nonce() {
        let secret = b"cluster-secret";
        let a = build_proof(secret);
        let b = build_proof(secret);
        assert_ne!(a, b, "nonces (and thus proofs) must not repeat");
    }
}
