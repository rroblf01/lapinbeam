import asyncio

import pytest

from helpers import wait_until
from lapinbeam import (
    ActorRef,
    Node,
    RemoteRef,
    Supervisor,
    actor,
    register_name,
    register_registry,
    unregister_name,
    whereis_name,
)


async def test_local_register_name_returns_owner_via_whereis():
    @actor(name="owner_local")
    class Owner:
        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(Owner)
        await register_name(node, "leader", ref=ref)
        found = whereis_name(node, "leader")
        assert isinstance(found, ActorRef)
        assert found.name == "owner_local"
    finally:
        await node.stop()


async def test_register_name_from_inside_an_actor():
    @actor(name="self_registerer")
    class Owner:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await register_name(self.node, "leader")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(Owner, node)
        await ref.send({})
        await wait_until(lambda: whereis_name(node, "leader") is not None)
        assert whereis_name(node, "leader").name == "self_registerer"
    finally:
        await node.stop()


async def test_register_name_conflict_raises_value_error():
    @actor(name="owner_one")
    class OwnerOne:
        async def receive(self, msg):
            pass

    @actor(name="owner_two")
    class OwnerTwo:
        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref_one = sup.spawn(OwnerOne)
        ref_two = sup.spawn(OwnerTwo)
        await register_name(node, "leader", ref=ref_one)
        with pytest.raises(ValueError, match="leader"):
            await register_name(node, "leader", ref=ref_two)
        # First claim wins; the rejected one changed nothing.
        assert whereis_name(node, "leader").name == "owner_one"
    finally:
        await node.stop()


async def test_register_name_is_idempotent_for_the_same_owner():
    @actor(name="owner_idempotent")
    class Owner:
        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(Owner)
        await register_name(node, "leader", ref=ref)
        await register_name(node, "leader", ref=ref)  # re-registering itself is not a conflict
        assert whereis_name(node, "leader").name == "owner_idempotent"
    finally:
        await node.stop()


async def test_unregister_name_releases_it():
    @actor(name="owner_unregister")
    class Owner:
        async def receive(self, msg):
            pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(Owner)
        await register_name(node, "leader", ref=ref)
        await unregister_name(node, "leader", ref=ref)
        assert whereis_name(node, "leader") is None
    finally:
        await node.stop()


async def test_cross_node_whereis_name_converges():
    @actor(name="owner_cross")
    class Owner:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await register_name(self.node, "leader")

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b)
        register_registry(node_a, sup_a)
        register_registry(node_b, sup_b)
        ref_a = sup_a.spawn(Owner, node_a)

        await node_a.connect_peer(node_b.local_id)
        await ref_a.send({})

        await wait_until(lambda: whereis_name(node_b, "leader") is not None)
        found = whereis_name(node_b, "leader")
        assert isinstance(found, RemoteRef)
        assert found.actor_name == "owner_cross"
        # And from node_a's own point of view it's local.
        assert isinstance(whereis_name(node_a, "leader"), ActorRef)
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_late_joining_node_converges_via_snapshot():
    @actor(name="owner_early")
    class OwnerEarly:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await register_name(self.node, "leader")

    node_a = Node("node_a@127.0.0.1:0")
    node_c = Node("node_c@127.0.0.1:0")
    await node_a.start()
    try:
        sup_a = Supervisor(node=node_a)
        register_registry(node_a, sup_a)
        ref_early = sup_a.spawn(OwnerEarly, node_a)
        await ref_early.send({})
        await wait_until(lambda: whereis_name(node_a, "leader") is not None)

        await node_c.start()
        sup_c = Supervisor(node=node_c)
        register_registry(node_c, sup_c)
        await node_c.connect_peer(node_a.local_id)

        await wait_until(lambda: whereis_name(node_c, "leader") is not None)
        found = whereis_name(node_c, "leader")
        assert isinstance(found, RemoteRef)
        assert found.actor_name == "owner_early"
    finally:
        await node_a.stop()
        await node_c.stop()


async def test_restart_releases_registered_name():
    @actor(name="flaky_owner")
    class Flaky:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await register_name(self.node, "leader")
            raise RuntimeError("boom")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node, max_restarts=3)
        ref = sup.spawn(Flaky, node)
        await ref.send({})
        # It registered, then immediately crashed — the name is
        # pid-scoped, so the crash (even within budget, restarting in
        # place) must release it, not leave a stale owner behind.
        await wait_until(lambda: whereis_name(node, "leader") is None)
        assert not ref.task.done()  # confirms it actually restarted, not gave up
    finally:
        await node.stop()


async def test_peer_disconnect_removes_its_remote_registration():
    @actor(name="owner_remote")
    class Owner:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            await register_name(self.node, "leader")

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b)
        register_registry(node_a, sup_a)
        register_registry(node_b, sup_b)
        ref_b = sup_b.spawn(Owner, node_b)

        await node_a.connect_peer(node_b.local_id)
        await ref_b.send({})
        await wait_until(lambda: whereis_name(node_a, "leader") is not None)

        node_a._on_core_event({"kind": "peer_disconnected", "peer": node_b.local_id})
        assert whereis_name(node_a, "leader") is None
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_concurrent_cross_node_claim_surfaces_as_registry_conflict_event():
    @actor(name="owner_a_side")
    class OwnerA:
        async def receive(self, msg):
            pass

    @actor(name="owner_b_side")
    class OwnerB:
        async def receive(self, msg):
            pass

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    events_a, events_b = [], []
    node_a.on_event(lambda e: events_a.append(e) if e["kind"] == "registry_conflict" else None)
    node_b.on_event(lambda e: events_b.append(e) if e["kind"] == "registry_conflict" else None)
    try:
        sup_a = Supervisor(node=node_a)
        sup_b = Supervisor(node=node_b)
        register_registry(node_a, sup_a)
        register_registry(node_b, sup_b)
        ref_a = sup_a.spawn(OwnerA)
        ref_b = sup_b.spawn(OwnerB)

        # Both nodes claim the same name *before* they're connected —
        # each succeeds locally, since neither yet knows about the other.
        await register_name(node_a, "leader", ref=ref_a)
        await register_name(node_b, "leader", ref=ref_b)

        # The snapshot exchanged on connect is what surfaces the
        # disagreement — not silently, and not resolved by magic.
        await node_a.connect_peer(node_b.local_id)
        await wait_until(lambda: len(events_a) == 1 and len(events_b) == 1)

        assert events_a[0]["name"] == "leader"
        assert events_b[0]["name"] == "leader"
        # Each node kept its own first-known claim rather than being
        # silently overwritten by the peer's.
        assert whereis_name(node_a, "leader").name == "owner_a_side"
        assert whereis_name(node_b, "leader").name == "owner_b_side"
    finally:
        await node_a.stop()
        await node_b.stop()
