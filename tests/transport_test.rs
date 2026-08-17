//! Integration tests for the multiplexed TCP transport.
//!
//! These exercise real sockets on 127.0.0.1 with ephemeral ports, so they
//! validate the actual wire behaviour end to end.

use std::future::Future;
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

use _core::runtime::NodeId;
use _core::transport::{Event, Transport, TransportConfig};
use _core::wire::{encode_frame, MessageKind, WireMessage, PROTOCOL_VERSION};

fn fast_config() -> TransportConfig {
    TransportConfig {
        heartbeat_interval: Duration::from_millis(50),
        peer_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}

fn reconnect_config() -> TransportConfig {
    TransportConfig {
        reconnect_interval: Duration::from_millis(100),
        ..fast_config()
    }
}

fn secured_config(secret: &[u8]) -> TransportConfig {
    TransportConfig {
        cluster_secret: Some(secret.to_vec()),
        ..fast_config()
    }
}

async fn wait_until<F, Fut>(mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cond().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition not met in time");
}

/// Waits until `events` yields an event matching `pred`, skipping others.
async fn wait_for_event<P>(mut events: broadcast::Receiver<Event>, pred: P) -> Event
where
    P: Fn(&Event) -> bool,
{
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match events.recv().await {
                Ok(ev) if pred(&ev) => return ev,
                Ok(_) => continue,
                Err(_) => panic!("event channel closed"),
            }
        }
    })
    .await
    .expect("event not received in time")
}

#[tokio::test]
async fn two_peers_exchange_data_bidirectionally() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    let (tx_a, mut rx_a) = mpsc::channel(16);
    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_a.register_actor("ingestor".into(), tx_a).await;
    node_b.register_actor("processor".into(), tx_b).await;

    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    node_a
        .send_data(
            &node_b.local_id(),
            "processor",
            json!({"type": "TASK", "payload_id": 1}),
            Some("ingestor"),
            Some(7),
        )
        .await
        .unwrap();

    let recv_result = tokio::time::timeout(Duration::from_secs(2), rx_b.recv()).await;
    let msg_b = recv_result
        .expect("node_b mailbox timeout")
        .expect("mailbox closed");
    assert_eq!(msg_b.dst_actor, "processor");
    assert_eq!(msg_b.src, node_a.local_id().to_full());
    assert_eq!(msg_b.correlation_id, Some(7));
    assert_eq!(
        msg_b.payload_json().unwrap(),
        json!({"type": "TASK", "payload_id": 1})
    );

    node_b
        .send_data(
            &node_a.local_id(),
            "ingestor",
            json!({"type": "ACK", "result": 2}),
            None,
            None,
        )
        .await
        .unwrap();

    let msg_a = tokio::time::timeout(Duration::from_secs(2), rx_a.recv())
        .await
        .expect("node_a mailbox timeout")
        .expect("mailbox closed");
    assert_eq!(msg_a.dst_actor, "ingestor");
    assert_eq!(msg_a.src, node_b.local_id().to_full());
    assert_eq!(
        msg_a.payload_json().unwrap(),
        json!({"type": "ACK", "result": 2})
    );
}

#[tokio::test]
async fn multiplexes_two_actors_over_one_connection() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    let (tx_alpha, mut rx_alpha) = mpsc::channel(16);
    let (tx_beta, mut rx_beta) = mpsc::channel(16);
    node_b.register_actor("alpha".into(), tx_alpha).await;
    node_b.register_actor("beta".into(), tx_beta).await;

    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    for i in 0..5 {
        node_a
            .send_data(&node_b.local_id(), "alpha", json!({"i": i}), None, None)
            .await
            .unwrap();
        node_a
            .send_data(&node_b.local_id(), "beta", json!({"i": i}), None, None)
            .await
            .unwrap();
    }

    for i in 0..5 {
        let m_alpha = tokio::time::timeout(Duration::from_secs(2), rx_alpha.recv())
            .await
            .expect("alpha timeout")
            .unwrap();
        assert_eq!(m_alpha.dst_actor, "alpha");
        assert_eq!(m_alpha.payload_json().unwrap(), json!({"i": i}));

        let m_beta = tokio::time::timeout(Duration::from_secs(2), rx_beta.recv())
            .await
            .expect("beta timeout")
            .unwrap();
        assert_eq!(m_beta.dst_actor, "beta");
        assert_eq!(m_beta.payload_json().unwrap(), json!({"i": i}));
    }
}

#[tokio::test]
async fn unknown_actor_triggers_error_frame() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    node_b
        .register_actor("processor".into(), mpsc::channel(4).0)
        .await;

    let events = node_a.event_stream();
    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    node_a
        .send_data(&node_b.local_id(), "ghost", json!({"x": 1}), None, Some(42))
        .await
        .unwrap();

    match wait_for_event(events, |ev| matches!(ev, Event::ErrorReceived { .. })).await {
        Event::ErrorReceived {
            from,
            detail,
            correlation_id,
        } => {
            assert_eq!(from, node_b.local_id());
            assert!(detail.contains("actor_not_found"), "detail was {detail}");
            assert_eq!(correlation_id, Some(42));
        }
        other => panic!("expected ErrorReceived, got {other:?}"),
    }
}

#[tokio::test]
async fn protocol_version_mismatch_drops_connection() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    let mut sock = TcpStream::connect(node_a.local_id().address())
        .await
        .unwrap();
    let bad_handshake = WireMessage {
        version: PROTOCOL_VERSION + 1,
        msg_id: 1,
        src: "future@127.0.0.1:9999".into(),
        dst_actor: String::new(),
        kind: MessageKind::Handshake,
        payload: Vec::new(),
        reply_to: None,
        correlation_id: None,
    };
    sock.write_all(&encode_frame(&bad_handshake).unwrap())
        .await
        .unwrap();

    // node_a must never register a peer whose handshake failed the version check.
    let future_peer = NodeId::parse("future@127.0.0.1:9999").unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!node_a.has_peer(&future_peer).await);

    // ...and must have closed its side of the connection instead of leaving
    // it open waiting for more (possibly-misinterpreted) frames.
    let mut buf = [0u8; 8];
    let n = tokio::time::timeout(Duration::from_millis(500), sock.read(&mut buf))
        .await
        .expect("connection was not closed after a protocol version mismatch")
        .expect("read error");
    assert_eq!(n, 0, "expected EOF after a protocol version mismatch");
}

#[tokio::test]
async fn protocol_version_mismatch_after_registration_evicts_peer() {
    // Same defect class as `protocol_version_mismatch_drops_connection`
    // above, but for a peer that already completed a valid handshake: a
    // *later* frame with a mismatched version used to be dropped via
    // `return` instead of `break`, skipping the post-loop cleanup that
    // evicts the `peers` map entry and fires `PeerDisconnected` — leaking
    // the connection and making `has_peer()` lie forever.
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    let mut sock = TcpStream::connect(node_a.local_id().address())
        .await
        .unwrap();
    let ghost = NodeId::parse("ghost@127.0.0.1:9999").unwrap();
    sock.write_all(&encode_frame(&WireMessage::handshake(1, ghost.to_full())).unwrap())
        .await
        .unwrap();
    wait_until(|| async { node_a.has_peer(&ghost).await }).await;

    let events = node_a.event_stream();
    let bad_frame = WireMessage {
        version: PROTOCOL_VERSION + 1,
        msg_id: 2,
        src: ghost.to_full(),
        dst_actor: String::new(),
        kind: MessageKind::Heartbeat,
        payload: Vec::new(),
        reply_to: None,
        correlation_id: None,
    };
    sock.write_all(&encode_frame(&bad_frame).unwrap())
        .await
        .unwrap();

    match wait_for_event(events, |ev| matches!(ev, Event::PeerDisconnected(_))).await {
        Event::PeerDisconnected(id) => assert_eq!(id, ghost),
        other => panic!("expected PeerDisconnected, got {other:?}"),
    }
    assert!(!node_a.has_peer(&ghost).await);
}

#[tokio::test]
async fn matching_cluster_secret_connects_normally() {
    let node_a = Transport::listen(
        NodeId::parse("node_a@127.0.0.1:0").unwrap(),
        secured_config(b"the-shared-secret"),
    )
    .await
    .unwrap();
    let node_b = Transport::listen(
        NodeId::parse("node_b@127.0.0.1:0").unwrap(),
        secured_config(b"the-shared-secret"),
    )
    .await
    .unwrap();

    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_b.register_actor("sink".into(), tx_b).await;

    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    node_a
        .send_data(&node_b.local_id(), "sink", json!({"ok": true}), None, None)
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .expect("message never arrived despite matching secrets")
        .expect("mailbox closed");
    assert_eq!(msg.payload_json().unwrap(), json!({"ok": true}));
}

#[tokio::test]
async fn mismatched_cluster_secret_rejects_connection() {
    let node_a = Transport::listen(
        NodeId::parse("node_a@127.0.0.1:0").unwrap(),
        secured_config(b"node-a-secret"),
    )
    .await
    .unwrap();
    let node_b = Transport::listen(
        NodeId::parse("node_b@127.0.0.1:0").unwrap(),
        secured_config(b"a-completely-different-secret"),
    )
    .await
    .unwrap();

    // node_a dials with its own (wrong, from node_b's point of view) proof.
    // `connect()` itself succeeds (the TCP connection opens fine, and it
    // has no way to know the handshake will be rejected) — the rejection
    // happens on node_b's side, silently from node_a's perspective, same
    // as any other unauthenticated peer would be dropped.
    node_a.connect(node_b.local_id()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !node_b.has_peer(&node_a.local_id()).await,
        "node_b must not register a peer whose secret didn't match"
    );
    assert!(
        !node_a.has_peer(&node_b.local_id()).await,
        "node_a's side of the rejected connection must also be gone"
    );
}

#[tokio::test]
async fn unauthenticated_handshake_rejected_when_secret_required() {
    // A raw client that never proves knowledge of the secret at all (as
    // opposed to proving the wrong one) must be rejected the same way.
    let node_a = Transport::listen(
        NodeId::parse("node_a@127.0.0.1:0").unwrap(),
        secured_config(b"the-shared-secret"),
    )
    .await
    .unwrap();

    let mut sock = TcpStream::connect(node_a.local_id().address())
        .await
        .unwrap();
    let plain_handshake = WireMessage::handshake(1, "ghost@127.0.0.1:9999");
    sock.write_all(&encode_frame(&plain_handshake).unwrap())
        .await
        .unwrap();

    let ghost = NodeId::parse("ghost@127.0.0.1:9999").unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!node_a.has_peer(&ghost).await);

    let mut buf = [0u8; 8];
    let n = tokio::time::timeout(Duration::from_millis(500), sock.read(&mut buf))
        .await
        .expect("connection was not closed after failing authentication")
        .expect("read error");
    assert_eq!(n, 0, "expected EOF after a failed authentication attempt");
}

#[tokio::test]
async fn heartbeat_keeps_connection_alive() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_a.has_peer(&node_b.local_id()).await }).await;

    // Longer than peer_timeout: heartbeats must keep the connection alive.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(node_a.has_peer(&node_b.local_id()).await);
    assert!(node_b.has_peer(&node_a.local_id()).await);
}

#[tokio::test]
async fn silent_peer_is_disconnected() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let events = node_a.event_stream();

    // Raw client connects and handshakes, then stays silent (never answers heartbeats).
    let mut sock = TcpStream::connect(node_a.local_id().address())
        .await
        .unwrap();
    let hs = WireMessage::handshake(1, "ghost@127.0.0.1:9999");
    sock.write_all(&encode_frame(&hs).unwrap()).await.unwrap();
    let ghost = NodeId::parse("ghost@127.0.0.1:9999").unwrap();
    wait_until(|| async { node_a.has_peer(&ghost).await }).await;

    match wait_for_event(events, |ev| matches!(ev, Event::PeerDisconnected(_))).await {
        Event::PeerDisconnected(pid) => assert_eq!(pid, ghost),
        other => panic!("expected PeerDisconnected, got {other:?}"),
    }
    assert!(!node_a.has_peer(&ghost).await);
    drop(sock);
}

#[tokio::test]
async fn peer_is_removed_when_connection_closes() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let events = node_a.event_stream();

    let mut sock = TcpStream::connect(node_a.local_id().address())
        .await
        .unwrap();
    let hs = WireMessage::handshake(1, "bye@127.0.0.1:9998");
    sock.write_all(&encode_frame(&hs).unwrap()).await.unwrap();
    let bye = NodeId::parse("bye@127.0.0.1:9998").unwrap();
    wait_until(|| async { node_a.has_peer(&bye).await }).await;

    // Close the raw socket: node_a must observe EOF and drop the peer.
    drop(sock);

    match wait_for_event(events, |ev| matches!(ev, Event::PeerDisconnected(_))).await {
        Event::PeerDisconnected(pid) => assert_eq!(pid, bye),
        other => panic!("expected PeerDisconnected, got {other:?}"),
    }
    assert!(!node_a.has_peer(&bye).await);
}

#[tokio::test]
async fn slow_mailbox_does_not_block_other_traffic() {
    // A saturated actor mailbox must not stall the read loop: other actors on
    // the same connection, and heartbeat replies, must keep flowing. A full
    // mailbox drops the message rather than blocking for room — see
    // `Event::MailboxFull` — which is what actually bounds how much memory a
    // stuck or merely-slow actor can pin.
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    // Tiny capacity so it saturates almost immediately and is never drained.
    let (tx_slow, _rx_slow_not_drained) = mpsc::channel(1);
    let (tx_fast, mut rx_fast) = mpsc::channel(16);
    node_b.register_actor("slow".into(), tx_slow).await;
    node_b.register_actor("fast".into(), tx_fast).await;

    let events = node_a.event_stream();
    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    // Flood the slow actor well past its mailbox capacity.
    for i in 0..50 {
        node_a
            .send_data(&node_b.local_id(), "slow", json!({"i": i}), None, Some(i))
            .await
            .unwrap();
    }
    // The fast actor must still receive promptly, not stuck behind "slow".
    node_a
        .send_data(&node_b.local_id(), "fast", json!({"ok": true}), None, None)
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_millis(500), rx_fast.recv())
        .await
        .expect("fast actor was blocked behind the slow one's full mailbox")
        .expect("mailbox closed");
    assert_eq!(msg.payload_json().unwrap(), json!({"ok": true}));

    // The sender (node_a) is told each dropped send failed, correlation_id
    // and all, via the same Error-frame mechanism used for an unknown actor.
    match wait_for_event(events, |ev| matches!(ev, Event::ErrorReceived { .. })).await {
        Event::ErrorReceived { detail, .. } => {
            assert!(detail.contains("mailbox_full:slow"), "detail was {detail}");
        }
        other => panic!("expected ErrorReceived, got {other:?}"),
    }

    // Heartbeats must also still be flowing on the same connection.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(node_a.has_peer(&node_b.local_id()).await);
    assert!(node_b.has_peer(&node_a.local_id()).await);
}

#[tokio::test]
async fn full_mailbox_fires_local_mailbox_full_event() {
    // The node whose actor mailbox overflowed sees it too — not just the
    // remote sender — since it's just as relevant to whoever operates that
    // node regardless of where the flood came from.
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    let (tx_slow, _rx_not_drained) = mpsc::channel(1);
    node_b.register_actor("slow".into(), tx_slow).await;

    let events_b = node_b.event_stream();
    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    for i in 0..10 {
        node_a
            .send_data(&node_b.local_id(), "slow", json!({"i": i}), None, None)
            .await
            .unwrap();
    }

    match wait_for_event(events_b, |ev| matches!(ev, Event::MailboxFull { .. })).await {
        Event::MailboxFull { actor } => assert_eq!(actor, "slow"),
        other => panic!("expected MailboxFull, got {other:?}"),
    }
}

#[tokio::test]
async fn simultaneous_dial_resolves_to_exactly_one_connection() {
    // Both sides dial each other at once. Without the tiebreak, this would
    // leave two independent live sockets between the same pair of nodes —
    // one of them a silently wasted duplicate forever.
    let node_a = Transport::listen(NodeId::parse("aaa@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("bbb@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();

    let (tx_a, mut rx_a) = mpsc::channel(16);
    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_a.register_actor("sink".into(), tx_a).await;
    node_b.register_actor("sink".into(), tx_b).await;

    // Genuinely concurrent: both futures are polled interleaved, so both
    // TCP connects and handshakes are in flight around the same time.
    let (r1, r2) = tokio::join!(
        node_a.connect(node_b.local_id()),
        node_b.connect(node_a.local_id()),
    );
    r1.unwrap();
    r2.unwrap();

    wait_until(|| async { node_a.has_peer(&node_b.local_id()).await }).await;
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    // `has_peer` only means "some connection currently exists" — one side
    // can observe its own outbound dial as already connected before the
    // other side's handshake has arrived and the tiebreak has resolved
    // which connection actually wins. A message sent into that window, on
    // what turns out to be the losing connection, is dropped when that
    // connection is torn down — no different from any other transient
    // drop in a system with no delivery guarantees (see docs/index.md's
    // "Limitations"). Give the race a moment to fully settle before
    // relying on sends succeeding, exactly as a real caller should.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Exactly one surviving connection each way, not two.
    assert_eq!(
        node_a.peer_count().await,
        1,
        "node_a should see exactly one peer"
    );
    assert_eq!(
        node_b.peer_count().await,
        1,
        "node_b should see exactly one peer"
    );

    // The survivor must actually work in both directions.
    node_a
        .send_data(
            &node_b.local_id(),
            "sink",
            json!({"probe": "a-to-b"}),
            None,
            None,
        )
        .await
        .unwrap();
    node_b
        .send_data(
            &node_a.local_id(),
            "sink",
            json!({"probe": "b-to-a"}),
            None,
            None,
        )
        .await
        .unwrap();

    let msg_b = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .expect("node_b never received the probe")
        .expect("mailbox closed");
    assert_eq!(msg_b.payload_json().unwrap(), json!({"probe": "a-to-b"}));

    let msg_a = tokio::time::timeout(Duration::from_secs(2), rx_a.recv())
        .await
        .expect("node_a never received the probe")
        .expect("mailbox closed");
    assert_eq!(msg_a.payload_json().unwrap(), json!({"probe": "b-to-a"}));

    // Give the losing connection's teardown time to finish, then confirm
    // the survivor is still the only entry — a mis-cleaned-up loser must
    // not evict the winner (this is exactly the corruption bug fixed
    // earlier, being re-checked here under a real race instead of a
    // simulated one).
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(node_a.peer_count().await, 1);
    assert_eq!(node_b.peer_count().await, 1);
}

#[tokio::test]
async fn stale_connection_cleanup_does_not_evict_fresh_one() {
    // Simulates two connections racing for the same peer id (e.g. both sides
    // dialing at once): the second handshake supersedes the first in the
    // peers map. When the first (stale) connection later dies, it must not
    // remove the second (current) connection's entry.
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let dup = NodeId::parse("dup@127.0.0.1:9997").unwrap();

    let mut first = TcpStream::connect(node_a.local_id().address())
        .await
        .unwrap();
    first
        .write_all(&encode_frame(&WireMessage::handshake(1, dup.to_full())).unwrap())
        .await
        .unwrap();
    wait_until(|| async { node_a.has_peer(&dup).await }).await;

    let mut second = TcpStream::connect(node_a.local_id().address())
        .await
        .unwrap();
    second
        .write_all(&encode_frame(&WireMessage::handshake(1, dup.to_full())).unwrap())
        .await
        .unwrap();
    // Give the second handshake time to be processed and supersede the first.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(node_a.has_peer(&dup).await);

    // Keep connection 2 alive on its own (independent of connection 1): feed
    // it bytes faster than `peer_timeout` so it never times out by itself,
    // isolating the effect under test (connection 1's belated cleanup).
    let keepalive = tokio::spawn(async move {
        loop {
            let hb = encode_frame(&WireMessage::heartbeat(1, "dup@127.0.0.1:9997")).unwrap();
            if second.write_all(&hb).await.is_err() {
                return second;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    });

    // Kill the stale (first) connection only.
    drop(first);
    // Long enough for the stale read loop to notice EOF/timeout and clean up.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The fresh (second) connection's entry must have survived.
    assert!(
        node_a.has_peer(&dup).await,
        "stale cleanup evicted the fresh connection"
    );
    keepalive.abort();
}

#[tokio::test]
async fn connect_retries_after_initial_dial_failure() {
    // A peer that drops *after* connecting is retried by `reconnect_supervisor`
    // reacting to `PeerDisconnected`. A peer that was never reachable in the
    // first place never fires that event, so this exercises the other path:
    // `connect()` itself must schedule a retry when the very first dial fails.

    // Reserve a real port, then free it immediately, so the first dial
    // attempt below fails outright (nothing listening yet) instead of
    // racing to find a port nobody happens to be using.
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let b_id = NodeId::parse(&format!("node_b@127.0.0.1:{}", addr.port())).unwrap();

    let node_a = Transport::listen(
        NodeId::parse("node_a@127.0.0.1:0").unwrap(),
        reconnect_config(),
    )
    .await
    .unwrap();

    assert!(
        node_a.connect(b_id.clone()).await.is_err(),
        "nothing should be listening at b_id yet"
    );

    // The peer must still be retried automatically once it comes up —
    // without ever calling `connect()` again.
    let node_b = Transport::listen(b_id.clone(), reconnect_config())
        .await
        .unwrap();
    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_b.register_actor("processor".into(), tx_b).await;

    wait_until(|| async { node_a.has_peer(&b_id).await }).await;

    node_a
        .send_data(&b_id, "processor", json!({"ping": 1}), None, None)
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .expect("node_b mailbox timeout")
        .expect("mailbox closed");
    assert_eq!(msg.payload_json().unwrap(), json!({"ping": 1}));
}

#[tokio::test]
async fn reconnects_to_desired_peer() {
    let node_a = Transport::listen(
        NodeId::parse("node_a@127.0.0.1:0").unwrap(),
        reconnect_config(),
    )
    .await
    .unwrap();
    let node_b = Transport::listen(
        NodeId::parse("node_b@127.0.0.1:0").unwrap(),
        reconnect_config(),
    )
    .await
    .unwrap();
    let b_id = node_b.local_id();

    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_b
        .register_actor("processor".into(), tx_b.clone())
        .await;

    node_a.connect(b_id.clone()).await.unwrap();
    wait_until(|| async { node_a.has_peer(&b_id).await }).await;

    // Node B goes away entirely.
    let events = node_a.event_stream();
    node_b.shutdown().await;
    match wait_for_event(events, |ev| matches!(ev, Event::PeerDisconnected(_))).await {
        Event::PeerDisconnected(pid) => assert_eq!(pid, b_id),
        other => panic!("expected PeerDisconnected, got {other:?}"),
    }

    // Node B comes back on the same address; node A must reconnect.
    let node_b2 = Transport::listen(b_id.clone(), reconnect_config())
        .await
        .unwrap();
    node_b2.register_actor("processor".into(), tx_b).await;
    wait_until(|| async { node_a.has_peer(&b_id).await }).await;

    node_a
        .send_data(&b_id, "processor", json!({"ping": 1}), None, None)
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .expect("no message after reconnect")
        .expect("mailbox closed");
    assert_eq!(msg.payload_json().unwrap(), json!({"ping": 1}));

    let _ = node_b2;
}

#[tokio::test]
async fn reconnect_gives_up_after_max_attempts() {
    let node_a = Transport::listen(
        NodeId::parse("node_a@127.0.0.1:0").unwrap(),
        TransportConfig {
            reconnect_interval: Duration::from_millis(30),
            reconnect_max_attempts: Some(3),
            ..fast_config()
        },
    )
    .await
    .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let b_id = node_b.local_id();

    node_a.connect(b_id.clone()).await.unwrap();
    wait_until(|| async { node_a.has_peer(&b_id).await }).await;

    // Node B goes away for good — nothing will ever answer at this address
    // again, so every reconnect attempt after this fails outright.
    let events = node_a.event_stream();
    node_b.shutdown().await;

    match wait_for_event(events, |ev| matches!(ev, Event::ReconnectGaveUp(_))).await {
        Event::ReconnectGaveUp(pid) => assert_eq!(pid, b_id),
        other => panic!("expected ReconnectGaveUp, got {other:?}"),
    }

    // Given up for good: waiting longer must not bring it back, and it
    // must no longer be tracked as desired (that's the actual leak fix —
    // checked indirectly here since `desired` isn't exposed directly).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!node_a.has_peer(&b_id).await);
}

#[tokio::test]
async fn forget_peer_drops_connection_and_stops_reconnecting() {
    let node_a = Transport::listen(
        NodeId::parse("node_a@127.0.0.1:0").unwrap(),
        TransportConfig {
            reconnect_interval: Duration::from_millis(30),
            ..fast_config()
        },
    )
    .await
    .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), fast_config())
        .await
        .unwrap();
    let b_id = node_b.local_id();

    node_a.connect(b_id.clone()).await.unwrap();
    wait_until(|| async { node_a.has_peer(&b_id).await }).await;

    node_a.forget_peer(&b_id).await;
    assert!(
        !node_a.has_peer(&b_id).await,
        "forget_peer should drop the live connection immediately"
    );

    // node_b is still alive and reachable — if node_a still considered it
    // desired, the fast reconnect_interval would reconnect almost
    // immediately. It must not, since we explicitly forgot it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !node_a.has_peer(&b_id).await,
        "a forgotten peer must not be auto-reconnected"
    );
}
