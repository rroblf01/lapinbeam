import asyncio

from lapinbeam import Node, Supervisor, join_via_seeds, register_discovery
from lapinbeam.discovery import DISCOVERY_ACTOR


async def make_node(name):
    node = Node(f"{name}@127.0.0.1:0")
    await node.start()
    register_discovery(node, Supervisor(node=node))
    return node


async def test_join_via_single_seed_discovers_only_the_seed():
    seed = await make_node("seed")
    joiner = await make_node("joiner")
    try:
        found = await join_via_seeds(joiner, [seed.local_id])
        assert found == {seed.local_id}
        assert joiner.has_peer(seed.local_id)
        assert seed.has_peer(joiner.local_id)
    finally:
        await seed.stop()
        await joiner.stop()


async def test_join_with_no_seeds_is_a_noop():
    node = await make_node("solo")
    try:
        found = await join_via_seeds(node, [])
        assert found == set()
        assert node.peer_count() == 0
    finally:
        await node.stop()


async def test_join_is_idempotent():
    seed = await make_node("seed")
    joiner = await make_node("joiner")
    try:
        first = await join_via_seeds(joiner, [seed.local_id])
        # Calling it again once nothing has changed must not error, and
        # must not somehow disconnect or duplicate anything.
        second = await join_via_seeds(joiner, [seed.local_id])
        assert first == second == {seed.local_id}
        assert joiner.peer_count() == 1
    finally:
        await seed.stop()
        await joiner.stop()


async def test_sequential_joins_reach_a_full_mesh():
    # No race here: each node finishes joining before the next one starts,
    # so a single pass per node is expected to be enough — the second-round
    # pattern in examples/seed_discovery/node_app.py exists specifically
    # for the concurrent case covered below.
    seed = await make_node("seed")
    a = await make_node("a")
    b = await make_node("b")
    c = await make_node("c")
    try:
        await join_via_seeds(a, [seed.local_id])
        await join_via_seeds(b, [seed.local_id])
        await join_via_seeds(c, [seed.local_id])

        all_nodes = {seed, a, b, c}
        all_ids = {n.local_id for n in all_nodes}
        for node in all_nodes:
            expected_peers = all_ids - {node.local_id}
            for peer_id in expected_peers:
                assert node.has_peer(peer_id), f"{node.local_id} never connected to {peer_id}"
    finally:
        for node in (seed, a, b, c):
            await node.stop()


async def test_concurrent_joiners_need_a_second_round():
    # Two nodes joining the same seed "at the same time" — the documented
    # limitation in lapinbeam/discovery.py's module docstring: they may not
    # discover each other on the first pass, since each one's snapshot of
    # the seed's known peers might be taken before the other has
    # registered. A second pass against everything learned so far is what
    # actually guarantees convergence — that's the property under test
    # here, not the exact outcome of the first (inherently racy) pass.
    seed = await make_node("seed")
    a = await make_node("a")
    b = await make_node("b")
    try:
        found_a, found_b = await asyncio.gather(
            join_via_seeds(a, [seed.local_id]),
            join_via_seeds(b, [seed.local_id]),
        )

        found_a |= await join_via_seeds(a, [seed.local_id, *found_a])
        found_b |= await join_via_seeds(b, [seed.local_id, *found_b])

        assert a.has_peer(b.local_id)
        assert b.has_peer(a.local_id)
    finally:
        await seed.stop()
        await a.stop()
        await b.stop()


async def test_discovery_actor_answers_who_do_you_know():
    seed = await make_node("seed")
    joiner = await make_node("joiner")
    try:
        await join_via_seeds(joiner, [seed.local_id])
        reply = await joiner.get_remote_actor(seed.local_id, DISCOVERY_ACTOR).ask(
            {"type": "WHO_DO_YOU_KNOW"}, timeout=2.0
        )
        assert set(reply["peers"]) == {seed.local_id, joiner.local_id}
    finally:
        await seed.stop()
        await joiner.stop()


async def test_unreachable_seed_does_not_raise():
    # A seed that never comes up (or is unreachable) must not blow up the
    # whole join — connect_peer's own ConnectionError is caught, and
    # join_via_seeds simply returns without that seed in the result. A
    # short connect_timeout keeps this test fast instead of waiting out
    # the 5s default.
    node = Node("lonely@127.0.0.1:0", connect_timeout=0.5)
    await node.start()
    register_discovery(node, Supervisor(node=node))
    try:
        ghost = "ghost@127.0.0.1:1"  # nothing listening here
        found = await join_via_seeds(node, [ghost])
        assert found == set()
    finally:
        await node.stop()
