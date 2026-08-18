import asyncio

import pytest

from helpers import wait_until
from lapinbeam import Exit, Node, Supervisor, actor, link, register_links, trap_exit, unlink


async def test_link_kills_partner_when_actor_gives_up():
    @actor(name="b_linked")
    class B:
        async def receive(self, msg):
            pass

    @actor(name="a_linked")
    class A:
        def __init__(self, other):
            self.other = other

        async def receive(self, msg):
            await link(self.other)
            raise RuntimeError("a boom")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        # Different Supervisors — proves links aren't tree-scoped.
        sup_a = Supervisor(node=node, max_restarts=0)
        sup_b = Supervisor(node=node, max_restarts=0)
        ref_b = sup_b.spawn(B)
        ref_a = sup_a.spawn(A, ref_b)
        await ref_a.send({})
        with pytest.raises(RuntimeError, match="a boom"):
            await asyncio.wait_for(ref_a.task, timeout=5.0)
        # The link-triggered kill routes through B's own Supervisor's
        # normal crash/restart machinery — with max_restarts=0, B gives up
        # immediately too, instead of lingering as an orphaned task.
        with pytest.raises(RuntimeError, match="linked actor 'a_linked' exited"):
            await asyncio.wait_for(ref_b.task, timeout=5.0)
    finally:
        await node.stop()


async def test_link_trap_exit_delivers_exit_message():
    received = []

    @actor(name="a_trap_target")
    class A:
        async def receive(self, msg):
            raise RuntimeError("a boom trap")

    @actor(name="b_trapper")
    class B:
        def __init__(self, other):
            self.other = other

        async def receive(self, msg):
            if isinstance(msg, Exit):
                received.append(msg)
                return
            trap_exit(True)
            await link(self.other)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup_a = Supervisor(node=node, max_restarts=0)
        sup_b = Supervisor(node=node)
        ref_a = sup_a.spawn(A)
        ref_b = sup_b.spawn(B, ref_a)
        await ref_b.send({"setup": True})
        await ref_a.send({})
        with pytest.raises(RuntimeError, match="a boom trap"):
            await asyncio.wait_for(ref_a.task, timeout=5.0)
        await wait_until(lambda: len(received) == 1)
        assert received[0].actor == "a_trap_target"
        assert "a boom trap" in received[0].reason
        # B trapped the exit instead of being killed by it.
        assert not ref_b.task.done()
    finally:
        await node.stop()


async def test_link_does_not_propagate_on_in_place_restart():
    b_received = []

    @actor(name="a_restart_ok")
    class A:
        async def receive(self, msg):
            raise RuntimeError("transient")

    @actor(name="b_restart_ok")
    class B:
        def __init__(self, other):
            self.other = other

        async def receive(self, msg):
            if isinstance(msg, Exit):
                b_received.append(msg)
                return
            await link(self.other)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup_a = Supervisor(node=node, max_restarts=3)  # stays within budget
        sup_b = Supervisor(node=node)
        ref_a = sup_a.spawn(A)
        ref_b = sup_b.spawn(B, ref_a)
        await ref_b.send({})  # B's first message links it to A
        await ref_a.send({})  # crashes but stays within budget: restarts in place
        await asyncio.sleep(0.2)
        # B never traps, so a real exit would have killed it — receiving
        # nothing here (and B still running) is the whole point of the
        # "links don't survive an in-place restart" decision.
        assert b_received == []
        assert not ref_a.task.done()
        assert not ref_b.task.done()
    finally:
        await node.stop()


async def test_unlink_stops_future_propagation():
    b_received = []

    @actor(name="a_unlink")
    class A:
        async def receive(self, msg):
            raise RuntimeError("a boom unlink")

    @actor(name="b_unlink")
    class B:
        def __init__(self, other):
            self.other = other

        async def receive(self, msg):
            if isinstance(msg, Exit):
                b_received.append(msg)
                return
            if msg.get("setup"):
                await link(self.other)
            elif msg.get("teardown"):
                await unlink(self.other)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup_a = Supervisor(node=node, max_restarts=0)
        sup_b = Supervisor(node=node)
        ref_a = sup_a.spawn(A)
        ref_b = sup_b.spawn(B, ref_a)
        await ref_b.send({"setup": True})
        await ref_b.send({"teardown": True})
        await asyncio.sleep(0.05)
        await ref_a.send({})
        with pytest.raises(RuntimeError, match="a boom unlink"):
            await asyncio.wait_for(ref_a.task, timeout=5.0)
        await asyncio.sleep(0.2)
        assert b_received == []
    finally:
        await node.stop()


async def test_cross_node_link_propagates_exit():
    @actor(name="remote_crasher")
    class Remote:
        async def receive(self, msg):
            raise RuntimeError("remote boom")

    @actor(name="local_linker")
    class Local:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            other = self.node.get_remote_actor(self.peer_id, "remote_crasher")
            await link(other)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a, max_restarts=0)
        sup_b = Supervisor(node=node_b, max_restarts=0)
        register_links(node_a, sup_a)
        register_links(node_b, sup_b)
        ref_remote = sup_b.spawn(Remote)
        ref_local = sup_a.spawn(Local, node_a, node_b.local_id)

        await node_a.connect_peer(node_b.local_id)
        await ref_local.send({"setup": True})
        await asyncio.sleep(0.2)  # let the cross-node link_request land
        await ref_remote.send({})
        with pytest.raises(RuntimeError, match="remote boom"):
            await asyncio.wait_for(ref_remote.task, timeout=5.0)
        with pytest.raises(RuntimeError, match="remote_crasher"):
            await asyncio.wait_for(ref_local.task, timeout=5.0)
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_cross_node_link_delivers_noconnection_on_peer_disconnect():
    @actor(name="remote_stub")
    class Remote:
        async def receive(self, msg):
            pass

    @actor(name="local_watcher")
    class Local:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            other = self.node.get_remote_actor(self.peer_id, "remote_stub")
            await link(other)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a, max_restarts=0)
        sup_b = Supervisor(node=node_b)
        register_links(node_a, sup_a)
        register_links(node_b, sup_b)
        sup_b.spawn(Remote)
        ref_local = sup_a.spawn(Local, node_a, node_b.local_id)

        await node_a.connect_peer(node_b.local_id)
        await ref_local.send({"setup": True})
        await asyncio.sleep(0.2)

        # Simulate an abrupt disconnect (network partition, remote process
        # killed without a chance to notify) directly, rather than via a
        # graceful `node_b.stop()` — which, being graceful, actually
        # manages to send its own "shutdown" exit signal first (a real,
        # separate, already-covered code path — see the give-up tests
        # above). This is what a genuine `peer_disconnected` looks like
        # when nothing was said first.
        node_a._on_core_event({"kind": "peer_disconnected", "peer": node_b.local_id})
        with pytest.raises(RuntimeError, match="noconnection"):
            await asyncio.wait_for(ref_local.task, timeout=5.0)
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_link_to_nonexistent_remote_actor_is_graceful():
    @actor(name="linker_degrade")
    class Local:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            other = self.node.get_remote_actor(self.peer_id, "does_not_exist")
            await link(other)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a, max_restarts=0)
        # node_b never calls register_links — proves a peer without the
        # feature degrades gracefully instead of breaking the connection.
        ref_local = sup_a.spawn(Local, node_a, node_b.local_id)
        await node_a.connect_peer(node_b.local_id)
        await ref_local.send({})
        await asyncio.sleep(0.2)
        assert node_a.has_peer(node_b.local_id)
        assert not ref_local.task.done()
    finally:
        await node_a.stop()
        await node_b.stop()
