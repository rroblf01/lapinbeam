import asyncio
import time
from dataclasses import dataclass

from helpers import wait_until
from lapinbeam import Node, PoolRef, Supervisor, actor, on


def _square(msg):
    """Module-level (picklable) sync handler for executor='process' tests."""
    return msg["n"] ** 2


def _boom_if_negative(msg):
    if msg["n"] < 0:
        raise ValueError("no negatives")
    return msg["n"]


async def test_pool_processes_messages_concurrently():
    """N_WORKERS actors, each `await asyncio.sleep(...)`ing — the whole
    point of a pool over one actor: real parallelism, not just queuing."""
    finished = []

    async def handler(msg):
        await asyncio.sleep(0.3)
        finished.append(msg["n"])

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(handler, 5, name="pool_concurrent")
        assert isinstance(pool, PoolRef)
        assert pool.size == 5

        start = time.monotonic()
        for i in range(5):
            await pool.send({"n": i})
        await wait_until(lambda: len(finished) == 5, timeout=2.0)
        elapsed = time.monotonic() - start

        # 5 sleeps of 0.3s across 5 workers should finish around 0.3s, not
        # 5 * 0.3s = 1.5s if they were secretly serialized.
        assert elapsed < 1.0
        assert sorted(finished) == [0, 1, 2, 3, 4]
    finally:
        await node.stop()


async def test_pool_shares_work_across_fewer_workers_than_messages():
    """3 workers, 9 messages: work queues instead of failing, and every
    message still gets processed exactly once."""
    finished = []

    async def handler(msg):
        await asyncio.sleep(0.05)
        finished.append(msg["n"])

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(handler, 3, name="pool_share")
        for i in range(9):
            await pool.send({"n": i})
        await wait_until(lambda: len(finished) == 9, timeout=2.0)
        assert sorted(finished) == list(range(9))
    finally:
        await node.stop()


async def test_pool_worker_survives_handler_exception():
    """A handler that raises for one message must not take the worker
    down with it — nothing ever sends a pool worker a second message on
    its own mailbox, so a crashed worker would never restart on its own."""
    processed = []
    events = []

    async def handler(msg):
        if msg["n"] == 1:
            raise RuntimeError("boom")
        processed.append(msg["n"])

    node = Node("node@127.0.0.1:0")
    node.on_event(lambda e: events.append(e) if e["kind"] == "pool_worker_error" else None)
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(handler, 1, name="pool_survives")  # 1 worker: forces reuse
        for i in range(3):
            await pool.send({"n": i})
        await wait_until(lambda: len(processed) == 2, timeout=2.0)
        assert sorted(processed) == [0, 2]
        await wait_until(lambda: len(events) == 1, timeout=2.0)
        assert events[0]["pool"] == "pool_survives"
        assert "boom" in events[0]["detail"]
    finally:
        await node.stop()


async def test_cross_node_pool_is_reachable_by_name():
    finished = []

    async def handler(msg):
        finished.append(msg["n"])

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_b = Supervisor(node=node_b)
        pool = await sup_b.spawn_pool(handler, 3, name="pool_remote")

        await node_a.connect_peer(node_b.local_id)
        remote_pool = node_a.get_remote_actor(node_b.local_id, pool.name)
        for i in range(5):
            await remote_pool.send({"n": i})
        await wait_until(lambda: len(finished) == 5, timeout=2.0)
        assert sorted(finished) == [0, 1, 2, 3, 4]
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_pool_ask_is_answered_by_whichever_worker_picks_it_up():
    from lapinbeam import current_message

    async def handler(msg):
        await current_message().reply(msg["n"] * 2)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(handler, 4, name="pool_ask")
        results = await asyncio.gather(*(pool.ask({"n": i}) for i in range(4)))
        assert sorted(results) == [0, 2, 4, 6]
    finally:
        await node.stop()


async def test_pool_actor_class_constructor_receives_args_and_kwargs():
    """For a class `handler`, `args`/`kwargs` go to the constructor once
    per worker, mirroring `spawn(actor_cls, *args, **kwargs)` — not to
    every message, unlike the function-handler case."""
    from lapinbeam import current_message

    @actor(name="seeded_worker")
    class Seeded:
        def __init__(self, start, step=1):
            self.total = start
            self.step = step

        async def receive(self, _msg):
            self.total += self.step
            await current_message().reply(self.total)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(Seeded, 1, 100, name="pool_seeded", step=5)
        assert await pool.ask({}) == 105
        assert await pool.ask({}) == 110
    finally:
        await node.stop()


async def test_pool_actor_class_dispatches_via_on_handlers():
    from lapinbeam import current_message

    @dataclass
    class Add:
        n: int

    @dataclass
    class Reset:
        pass

    @actor(name="typed_worker")
    class Accumulator:
        def __init__(self):
            self.total = 0

        @on(Add)
        async def handle_add(self, msg):
            self.total += msg.n
            await current_message().reply(self.total)

        @on(Reset)
        async def handle_reset(self, _msg):
            self.total = 0
            await current_message().reply(self.total)

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(Accumulator, 1, name="pool_typed")  # 1 worker: same instance
        assert await pool.ask(Add(2)) == 2
        assert await pool.ask(Add(3)) == 5
        assert await pool.ask(Reset()) == 0
        assert await pool.ask(Add(1)) == 1
    finally:
        await node.stop()


async def test_pool_actor_class_worker_survives_handler_exception_with_state_intact():
    from lapinbeam import current_message

    events = []

    @dataclass
    class Bump:
        by: int
        boom: bool = False

    @actor(name="fragile_worker")
    class Fragile:
        def __init__(self):
            self.total = 0

        async def receive(self, msg):
            if msg.boom:
                raise RuntimeError("boom")
            self.total += msg.by
            await current_message().reply(self.total)

    node = Node("node@127.0.0.1:0")
    node.on_event(lambda e: events.append(e) if e["kind"] == "pool_worker_error" else None)
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(Fragile, 1, name="pool_fragile")
        assert await pool.ask(Bump(by=1)) == 1
        await pool.send(Bump(by=0, boom=True))
        await wait_until(lambda: len(events) == 1, timeout=2.0)
        assert events[0]["pool"] == "pool_fragile"
        # Same instance survives: state from before the crash is untouched,
        # and it keeps accumulating afterward.
        assert await pool.ask(Bump(by=2)) == 3
    finally:
        await node.stop()


async def test_pool_queue_capacity_drops_and_reports_when_full():
    """A bounded pool queue drops an over-limit send and fires
    on_event(kind="pool_queue_full") instead of blocking the sender or
    growing memory unboundedly — same shape as mailbox_full."""
    gate = asyncio.Event()
    started = asyncio.Event()
    processed = []
    events = []

    async def handler(msg):
        started.set()
        await gate.wait()
        processed.append(msg["n"])

    node = Node("node@127.0.0.1:0")
    node.on_event(lambda e: events.append(e) if e["kind"] == "pool_queue_full" else None)
    await node.start()
    try:
        sup = Supervisor(node=node)
        # 1 worker, capacity 1: one message occupies the worker, one more
        # fills the queue, a third must be dropped.
        pool = await sup.spawn_pool(handler, 1, name="pool_bounded", queue_capacity=1)
        await pool.send({"n": 0})
        await asyncio.wait_for(started.wait(), timeout=2.0)
        await pool.send({"n": 1})  # fills the queue
        await pool.send({"n": 2})  # must be dropped
        await wait_until(lambda: len(events) == 1, timeout=2.0)
        assert events[0]["pool"] == "pool_bounded"
        gate.set()
        await wait_until(lambda: len(processed) == 2, timeout=2.0)
        assert sorted(processed) == [0, 1]
    finally:
        await node.stop()


async def test_pool_executor_thread_runs_sync_handler_and_replies():
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(_square, 3, name="pool_thread", executor="thread")
        results = await asyncio.gather(*(pool.ask({"n": i}) for i in range(4)))
        assert sorted(results) == [0, 1, 4, 9]
    finally:
        await node.stop()


async def test_pool_executor_process_runs_sync_handler_and_replies():
    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(_square, 2, name="pool_process", executor="process")
        results = await asyncio.gather(*(pool.ask({"n": i}) for i in range(4)))
        assert sorted(results) == [0, 1, 4, 9]
    finally:
        await node.stop()


async def test_pool_executor_reports_handler_exception_without_crashing_worker():
    events = []
    node = Node("node@127.0.0.1:0")
    node.on_event(lambda e: events.append(e) if e["kind"] == "pool_worker_error" else None)
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(_boom_if_negative, 1, name="pool_executor_err", executor="thread")
        assert await pool.ask({"n": 5}) == 5
        await pool.send({"n": -1})
        await wait_until(lambda: len(events) == 1, timeout=2.0)
        assert "no negatives" in events[0]["detail"]
        # worker keeps serving afterward
        assert await pool.ask({"n": 7}) == 7
    finally:
        await node.stop()


async def test_pool_executor_rejects_actor_class_and_async_handler():
    @actor(name="stateful_worker")
    class Stateful:
        async def receive(self, msg):
            pass

    async def async_handler(msg):
        pass

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        try:
            await sup.spawn_pool(Stateful, 1, name="pool_bad1", executor="thread")
            assert False, "expected TypeError"
        except TypeError as exc:
            assert "@actor class" in str(exc)
        try:
            await sup.spawn_pool(async_handler, 1, name="pool_bad2", executor="thread")
            assert False, "expected TypeError"
        except TypeError as exc:
            assert "synchronous" in str(exc)
        try:
            await sup.spawn_pool(_square, 1, name="pool_bad3", executor="nope")
            assert False, "expected ValueError"
        except ValueError:
            pass
    finally:
        await node.stop()


async def test_pool_key_routes_same_key_to_same_worker_in_order():
    """A sharded pool must process every message for a given key strictly
    in the order it arrived, even while different keys run in parallel."""
    seen = {}

    async def handler(msg):
        # Simulate uneven work so out-of-order delivery within a key would
        # actually show up if routing were wrong.
        await asyncio.sleep(0.02 if msg["key"] == "a" else 0.001)
        seen.setdefault(msg["key"], []).append(msg["seq"])

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(handler, 4, name="pool_sharded", key=lambda msg: msg["key"])
        for seq in range(5):
            await pool.send({"key": "a", "seq": seq})
            await pool.send({"key": "b", "seq": seq})
        await wait_until(lambda: len(seen.get("a", [])) == 5 and len(seen.get("b", [])) == 5, timeout=2.0)
        assert seen["a"] == [0, 1, 2, 3, 4]
        assert seen["b"] == [0, 1, 2, 3, 4]
    finally:
        await node.stop()


async def test_pool_key_error_reported_without_crashing_dispatcher():
    events = []
    processed = []

    async def handler(msg):
        processed.append(msg)

    def bad_key(msg):
        if msg.get("bad"):
            raise ValueError("no key here")
        return msg["k"]

    node = Node("node@127.0.0.1:0")
    node.on_event(lambda e: events.append(e) if e["kind"] == "pool_worker_error" else None)
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(handler, 2, name="pool_sharded_err", key=bad_key)
        await pool.send({"bad": True})
        await wait_until(lambda: len(events) == 1, timeout=2.0)
        assert "no key here" in events[0]["detail"]
        # dispatcher keeps working afterward
        await pool.send({"k": "x"})
        await wait_until(lambda: len(processed) == 1, timeout=2.0)
    finally:
        await node.stop()
