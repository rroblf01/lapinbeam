//! Integration tests for the multiplexed TCP transport.
//!
//! These exercise real sockets on 127.0.0.1 with ephemeral ports, so they
//! validate the actual wire behaviour end to end.

use std::future::Future;
use std::time::Duration;

use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};

use _core::runtime::NodeId;
use _core::transport::{Event, Transport, TransportConfig};
use _core::wire::{encode_frame, WireMessage};

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
    assert_eq!(msg_a.payload_json().unwrap(), json!({"type": "ACK", "result": 2}));
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

    node_b.register_actor("processor".into(), mpsc::channel(4).0).await;

    let events = node_a.event_stream();
    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    node_a
        .send_data(&node_b.local_id(), "ghost", json!({"x": 1}), None, None)
        .await
        .unwrap();

    match wait_for_event(events, |ev| matches!(ev, Event::ErrorReceived { .. })).await {
        Event::ErrorReceived { from, detail } => {
            assert_eq!(from, node_b.local_id());
            assert!(detail.contains("actor_not_found"), "detail was {detail}");
        }
        other => panic!("expected ErrorReceived, got {other:?}"),
    }
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
    // the same connection, and heartbeat replies, must keep flowing.
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

    node_a.connect(node_b.local_id()).await.unwrap();
    wait_until(|| async { node_b.has_peer(&node_a.local_id()).await }).await;

    // Flood the slow actor well past its mailbox capacity.
    for i in 0..50 {
        node_a
            .send_data(&node_b.local_id(), "slow", json!({"i": i}), None, None)
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

    // Heartbeats must also still be flowing on the same connection.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(node_a.has_peer(&node_b.local_id()).await);
    assert!(node_b.has_peer(&node_a.local_id()).await);
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

    let mut first = TcpStream::connect(node_a.local_id().address()).await.unwrap();
    first
        .write_all(&encode_frame(&WireMessage::handshake(1, dup.to_full())).unwrap())
        .await
        .unwrap();
    wait_until(|| async { node_a.has_peer(&dup).await }).await;

    let mut second = TcpStream::connect(node_a.local_id().address()).await.unwrap();
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
    assert!(node_a.has_peer(&dup).await, "stale cleanup evicted the fresh connection");
    keepalive.abort();
}

#[tokio::test]
async fn reconnects_to_desired_peer() {
    let node_a = Transport::listen(NodeId::parse("node_a@127.0.0.1:0").unwrap(), reconnect_config())
        .await
        .unwrap();
    let node_b = Transport::listen(NodeId::parse("node_b@127.0.0.1:0").unwrap(), reconnect_config())
        .await
        .unwrap();
    let b_id = node_b.local_id();

    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_b.register_actor("processor".into(), tx_b.clone()).await;

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
