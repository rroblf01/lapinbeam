"""`cluster_secret` handshake authentication."""

from helpers import wait_until
from lapinbeam import Node, Supervisor, actor


async def test_matching_cluster_secret_connects():
    received = []

    @actor(name="sink")
    class Sink:
        async def receive(self, msg):
            received.append(msg)

    node_a = Node("node_a@127.0.0.1:0", cluster_secret="shared-secret")
    node_b = Node("node_b@127.0.0.1:0", cluster_secret="shared-secret")
    await node_a.start()
    await node_b.start()
    try:
        Supervisor(node=node_b).spawn(Sink)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "sink")
        await remote.send({"ok": True})
        await wait_until(lambda: len(received) == 1)
        assert received[0] == {"ok": True}
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_mismatched_cluster_secret_rejects_connection():
    # `connect_peer` itself does not raise here: node_a's own dial
    # optimistically registers on node_a's side the moment the TCP socket
    # opens, before node_b has had a chance to accept or reject the
    # handshake (see docs/index.md — this is the same "connected on my side
    # doesn't yet mean accepted on theirs" window the simultaneous-dial
    # tiebreak also has to account for). node_b rejects the mismatched
    # handshake and closes its side; node_a discovers this once its own
    # connection notices the peer went silent.
    node_a = Node("node_a@127.0.0.1:0", cluster_secret="node-a-secret")
    node_b = Node("node_b@127.0.0.1:0", cluster_secret="a-totally-different-secret")
    await node_a.start()
    await node_b.start()
    try:
        await node_a.connect_peer(node_b.local_id)
        assert not node_b.has_peer(node_a.local_id)
        await wait_until(lambda: not node_a.has_peer(node_b.local_id), timeout=6.0)
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_unsecured_dialer_rejected_by_secured_acceptor():
    node_a = Node("node_a@127.0.0.1:0")  # no secret at all
    node_b = Node("node_b@127.0.0.1:0", cluster_secret="required-secret")
    await node_a.start()
    await node_b.start()
    try:
        await node_a.connect_peer(node_b.local_id)
        assert not node_b.has_peer(node_a.local_id)
        await wait_until(lambda: not node_a.has_peer(node_b.local_id), timeout=6.0)
    finally:
        await node_a.stop()
        await node_b.stop()
