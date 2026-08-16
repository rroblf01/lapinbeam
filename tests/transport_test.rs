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
