import asyncio

import pytest

from helpers import wait_until
from lapinbeam import Down, Node, Supervisor, actor, demonitor, monitor, register_monitors


async def test_monitor_delivers_down_on_final_give_up_without_killing_watcher():
    received = []

    @actor(name="target_gives_up")
    class Target:
        async def receive(self, msg):
            raise RuntimeError("target boom")

    @actor(name="watcher_survives")
    class Watcher:
        def __init__(self, other):
            self.other = other

        async def receive(self, msg):
            if isinstance(msg, Down):
                received.append(msg)
                return
            await monitor(self.other)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup_target = Supervisor(node=node, max_restarts=0)
        sup_watcher = Supervisor(node=node)
        ref_target = sup_target.spawn(Target)
        ref_watcher = sup_watcher.spawn(Watcher, ref_target)
        await ref_watcher.send({"setup": True})
        await ref_target.send({})
        with pytest.raises(RuntimeError, match="target boom"):
            await asyncio.wait_for(ref_target.task, timeout=5.0)
        await wait_until(lambda: len(received) == 1)
        assert received[0].actor == "target_gives_up"
        assert "target boom" in received[0].reason
        # The whole point of monitor() over link(): the watcher is never
        # touched by the target's death.
        assert not ref_watcher.task.done()
    finally:
        await node.stop()


async def test_monitor_does_not_fire_on_in_place_restart():
    received = []

    @actor(name="target_restarts_ok")
    class Target:
        async def receive(self, msg):
            raise RuntimeError("transient")

    @actor(name="watcher_restart_ok")
    class Watcher:
        def __init__(self, other):
            self.other = other

        async def receive(self, msg):
            if isinstance(msg, Down):
                received.append(msg)
                return
            await monitor(self.other)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup_target = Supervisor(node=node, max_restarts=3)  # stays within budget
        sup_watcher = Supervisor(node=node)
        ref_target = sup_target.spawn(Target)
        ref_watcher = sup_watcher.spawn(Watcher, ref_target)
        await ref_watcher.send({})  # links watcher to the current generation
        await ref_target.send({})  # crashes but restarts in place
        await asyncio.sleep(0.2)
        assert received == []
        assert not ref_target.task.done()
        assert not ref_watcher.task.done()
    finally:
        await node.stop()


async def test_demonitor_stops_future_notification():
    received = []

    @actor(name="target_demonitor")
    class Target:
        async def receive(self, msg):
            raise RuntimeError("boom demonitor")

    @actor(name="watcher_demonitor")
    class Watcher:
        def __init__(self, other):
            self.other = other
            self.ref = None

        async def receive(self, msg):
            if isinstance(msg, Down):
                received.append(msg)
                return
            if msg.get("setup"):
                self.ref = await monitor(self.other)
            elif msg.get("teardown"):
                await demonitor(self.ref)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup_target = Supervisor(node=node, max_restarts=0)
        sup_watcher = Supervisor(node=node)
        ref_target = sup_target.spawn(Target)
        ref_watcher = sup_watcher.spawn(Watcher, ref_target)
        await ref_watcher.send({"setup": True})
        await ref_watcher.send({"teardown": True})
        await asyncio.sleep(0.05)
        await ref_target.send({})
        with pytest.raises(RuntimeError, match="boom demonitor"):
            await asyncio.wait_for(ref_target.task, timeout=5.0)
        await asyncio.sleep(0.2)
        assert received == []
    finally:
        await node.stop()


async def test_monitor_to_nonexistent_local_actor_delivers_immediate_noproc():
    received = []

    @actor(name="watcher_local_noproc")
    class Watcher:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            if isinstance(msg, Down):
                received.append(msg)
                return
            from lapinbeam import ActorRef
            await monitor(ActorRef(self.node, "does_not_exist_locally"))

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref_watcher = sup.spawn(Watcher, node)
        await ref_watcher.send({})
        await wait_until(lambda: len(received) == 1)
        assert received[0].actor == "does_not_exist_locally"
        assert received[0].reason == "noproc"
    finally:
        await node.stop()


async def test_cross_node_monitor_delivers_down():
    @actor(name="remote_target")
    class Remote:
        async def receive(self, msg):
            raise RuntimeError("remote boom monitor")

    @actor(name="local_watcher_cross")
    class Local:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id
            self.received = []

        async def receive(self, msg):
            if isinstance(msg, Down):
                self.received.append(msg)
                return
            other = self.node.get_remote_actor(self.peer_id, "remote_target")
            await monitor(other)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b, max_restarts=0)
        register_monitors(node_a, sup_a)
        register_monitors(node_b, sup_b)
        ref_remote = sup_b.spawn(Remote)
        ref_local = sup_a.spawn(Local, node_a, node_b.local_id)

        await node_a.connect_peer(node_b.local_id)
        await ref_local.send({"setup": True})
        await asyncio.sleep(0.2)
        await ref_remote.send({})
        with pytest.raises(RuntimeError, match="remote boom monitor"):
            await asyncio.wait_for(ref_remote.task, timeout=5.0)
        # Watcher must survive — monitor() never kills.
        await asyncio.sleep(0.3)
        assert not ref_local.task.done()
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_cross_node_monitor_delivers_noconnection_on_peer_disconnect():
    received = []

    @actor(name="remote_stub_monitor")
    class Remote:
        async def receive(self, msg):
            pass

    @actor(name="local_watcher_disconnect")
    class Local:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            if isinstance(msg, Down):
                received.append(msg)
                return
            other = self.node.get_remote_actor(self.peer_id, "remote_stub_monitor")
            await monitor(other)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b)
        register_monitors(node_a, sup_a)
        register_monitors(node_b, sup_b)
        sup_b.spawn(Remote)
        ref_local = sup_a.spawn(Local, node_a, node_b.local_id)

        await node_a.connect_peer(node_b.local_id)
        await ref_local.send({"setup": True})
        await asyncio.sleep(0.2)

        node_a._on_core_event({"kind": "peer_disconnected", "peer": node_b.local_id})
        await wait_until(lambda: len(received) == 1)
        assert received[0].reason == "noconnection"
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_monitor_to_nonexistent_remote_actor_without_registration_is_graceful():
    @actor(name="watcher_remote_degrade")
    class Local:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            other = self.node.get_remote_actor(self.peer_id, "does_not_exist")
            await monitor(other)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a, max_restarts=0)
        # node_b never calls register_monitors — proves a peer without the
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
