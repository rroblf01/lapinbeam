import asyncio

from helpers import wait_until
from lapinbeam import ActorRef, Node, RemoteRef, Supervisor, actor, join_group, members, register_groups


async def test_local_join_group_returns_actor_refs():
    @actor(name="worker_local")
    class Worker:
        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        # join_group() defaults to the currently-running actor via
        # current_actor_ref() — pass ref= explicitly here instead, since
        # this test drives it from outside any actor.
        ref = sup.spawn(Worker)
        await join_group(node, "workers", ref=ref)
        found = members(node, "workers")
        assert len(found) == 1
        assert isinstance(found[0], ActorRef)
        assert found[0].name == "worker_local"
    finally:
        await node.stop()


async def test_join_group_from_inside_an_actor():
    @actor(name="self_joiner")
    class Worker:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await join_group(self.node, "workers")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(Worker, node)
        await ref.send({})
        await wait_until(lambda: len(members(node, "workers")) == 1)
        assert members(node, "workers")[0].name == "self_joiner"
    finally:
        await node.stop()


async def test_cross_node_group_membership_converges():
    @actor(name="worker_a")
    class WorkerA:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await join_group(self.node, "workers")

    @actor(name="worker_b")
    class WorkerB:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await join_group(self.node, "workers")

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b)
        register_groups(node_a, sup_a)
        register_groups(node_b, sup_b)
        ref_a = sup_a.spawn(WorkerA, node_a)
        ref_b = sup_b.spawn(WorkerB, node_b)

        await node_a.connect_peer(node_b.local_id)
        await ref_a.send({})
        await ref_b.send({})

        await wait_until(lambda: len(members(node_a, "workers")) == 2)
        await wait_until(lambda: len(members(node_b, "workers")) == 2)

        def ref_name(ref):
            return ref.name if isinstance(ref, ActorRef) else ref.actor_name

        names_a = {ref_name(m) for m in members(node_a, "workers")}
        names_b = {ref_name(m) for m in members(node_b, "workers")}
        assert names_a == names_b == {"worker_a", "worker_b"}
        # node_a's own worker is local; node_b's is remote from node_a's view.
        kinds = {ref_name(m): type(m).__name__ for m in members(node_a, "workers")}
        assert kinds["worker_a"] == "ActorRef"
        assert kinds["worker_b"] == "RemoteRef"
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_late_joining_node_converges_via_snapshot():
    @actor(name="worker_early")
    class WorkerEarly:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await join_group(self.node, "workers")

    @actor(name="worker_late")
    class WorkerLate:
        async def receive(self, msg):
            pass

    node_a = Node("node_a@127.0.0.1:0")
    node_c = Node("node_c@127.0.0.1:0")
    await node_a.start()
    try:
        sup_a = Supervisor(node=node_a)
        register_groups(node_a, sup_a)
        ref_early = sup_a.spawn(WorkerEarly, node_a)
        await ref_early.send({})
        await wait_until(lambda: len(members(node_a, "workers")) == 1)

        # node_c joins the cluster only *after* "workers" already has a
        # member on node_a — it must still converge, via the snapshot
        # exchanged on connect, not just future deltas.
        await node_c.start()
        sup_c = Supervisor(node=node_c)
        register_groups(node_c, sup_c)
        await node_c.connect_peer(node_a.local_id)

        await wait_until(lambda: len(members(node_c, "workers")) == 1)
        found = members(node_c, "workers")
        assert isinstance(found[0], RemoteRef)
        assert found[0].actor_name == "worker_early"
    finally:
        await node_a.stop()
        await node_c.stop()


async def test_restart_drops_group_membership():
    @actor(name="flaky_member")
    class Flaky:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await join_group(self.node, "workers")
            raise RuntimeError("boom")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node, max_restarts=3)
        ref = sup.spawn(Flaky, node)
        await ref.send({})
        # It joined, then immediately crashed — membership is pid-scoped,
        # so the crash (even though it's within budget and restarts in
        # place) must drop it again, not leave a stale entry behind.
        await wait_until(lambda: len(members(node, "workers")) == 0)
        assert not ref.task.done()  # confirms it actually restarted, not gave up
    finally:
        await node.stop()


async def test_peer_disconnect_removes_its_remote_members():
    @actor(name="worker_remote")
    class Worker:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await join_group(self.node, "workers")

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b)
        register_groups(node_a, sup_a)
        register_groups(node_b, sup_b)
        ref_b = sup_b.spawn(Worker, node_b)

        await node_a.connect_peer(node_b.local_id)
        await ref_b.send({})
        await wait_until(lambda: len(members(node_a, "workers")) == 1)

        node_a._on_core_event({"kind": "peer_disconnected", "peer": node_b.local_id})
        assert members(node_a, "workers") == []
    finally:
        await node_a.stop()
        await node_b.stop()
