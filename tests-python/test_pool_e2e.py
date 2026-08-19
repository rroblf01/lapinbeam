"""End-to-end correctness checks for Supervisor.spawn_pool() at a scale
and combination of options the smaller, one-feature-at-a-time tests in
test_pool.py don't exercise: several hundred messages, multiple options
combined at once, and repeated create/destroy cycles — the kind of thing
that only shows a race or a leak once real concurrency and repetition are
involved. CPU/RAM behavior under load is covered separately by
bench/bench_pool.py (a manual benchmark, not part of this suite, since
its multi-second/multi-hundred-MB scenarios don't belong in a fast CI
run) — these tests stick to correctness, at a scale large enough to make
that correctness meaningful.
"""

import asyncio

from helpers import wait_until
from lapinbeam import Node, PoolRef, Supervisor, current_message


def _partial_sums(msg):
    return sum(range(msg["n"]))


async def test_sharded_pool_with_queue_capacity_preserves_per_key_order_at_scale():
    """500 messages across 25 keys, routed through a bounded, sharded
    pool — every key's messages must still come out in arrival order,
    and (with a capacity comfortably above the per-key backlog) none
    should be dropped."""
    seen = {}

    async def handler(msg):
        # Deliberately uneven per-key work so a routing bug would show up
        # as interleaving rather than being masked by uniform timing.
        await asyncio.sleep(0.001 * (msg["key"] % 3))
        seen.setdefault(msg["key"], []).append(msg["seq"])

    node = Node("node@127.0.0.1:0")
    dropped = []
    node.on_event(lambda e: dropped.append(e) if e["kind"] == "pool_queue_full" else None)
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(
            handler, 8, name="sharded_e2e",
            key=lambda msg: msg["key"], queue_capacity=200,
        )
        n_keys = 25
        per_key = 20
        for seq in range(per_key):
            for key in range(n_keys):
                await pool.send({"key": key, "seq": seq})
            await asyncio.sleep(0)  # let workers drain between waves

        await wait_until(
            lambda: sum(len(v) for v in seen.values()) == n_keys * per_key,
            timeout=5.0,
        )
        assert not dropped, f"unexpected drops with generous capacity: {dropped}"
        for key in range(n_keys):
            assert seen[key] == list(range(per_key)), f"key {key} out of order: {seen[key]}"
    finally:
        await node.stop()


async def test_executor_process_pool_correct_at_scale():
    """A larger batch through executor='process' — proves results are
    correctly matched back to their `ask()`/`map()` callers even when
    many items are in flight across a real process pool, not just the
    handful covered in test_pool.py."""
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(_partial_sums, 4, name="process_e2e", executor="process")
        items = [{"n": i} for i in range(60)]
        results = await pool.map(items, timeout=10.0)
        assert results == [sum(range(i)) for i in range(60)]
        await pool.stop()
    finally:
        await node.stop()


async def test_pool_survives_many_stop_and_respawn_cycles_with_real_traffic():
    """Not just a leak check (see bench_pool.py for that) — each
    respawned pool under the same name must actually work correctly for
    real traffic, not just start up cleanly."""
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        for cycle in range(15):
            pool = await sup.spawn_pool(_partial_sums, 3, name="churn_e2e", executor="thread")
            results = await pool.map([{"n": cycle}, {"n": cycle + 1}, {"n": cycle + 2}])
            assert results == [sum(range(cycle)), sum(range(cycle + 1)), sum(range(cycle + 2))]
            await pool.stop()
        assert "churn_e2e" not in node._mailboxes
        assert not sup._children
    finally:
        await node.stop()


async def test_cross_node_pool_map_and_stop_end_to_end():
    """The full remote path — get_remote_actor + map() + stop() — not
    just send()/ask() individually as in test_pool.py."""
    async def handler(msg):
        await current_message().reply(msg["n"] * 2)

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_b = Supervisor(node=node_b)
        pool = await sup_b.spawn_pool(handler, 4, name="remote_e2e")

        await node_a.connect_peer(node_b.local_id)
        remote_pool = node_a.get_remote_actor(node_b.local_id, pool.name)

        results = await asyncio.gather(
            *(remote_pool.ask({"n": i}, timeout=5.0) for i in range(30))
        )
        assert sorted(results) == [i * 2 for i in range(30)]

        await pool.stop()
        assert "remote_e2e" not in node_b._mailboxes
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_spawn_pool_context_manager_end_to_end_with_map():
    processed = []

    async def handler(msg):
        processed.append(msg["n"])
        await current_message().reply(msg["n"] ** 2)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        async with sup.spawn_pool(handler, 5, name="ctx_e2e") as pool:
            assert isinstance(pool, PoolRef)
            results = await pool.map([{"n": i} for i in range(40)])
            assert results == [i ** 2 for i in range(40)]
        assert sorted(processed) == list(range(40))
        assert "ctx_e2e" not in node._mailboxes
    finally:
        await node.stop()
