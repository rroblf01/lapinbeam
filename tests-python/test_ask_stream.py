import asyncio

import pytest

from lapinbeam import Node, Supervisor, actor, current_message


async def test_ask_stream_yields_items_then_stops_after_final():
    @actor(name="streamer")
    class Streamer:
        async def receive(self, msg):
            for i in range(msg["n"]):
                await current_message().reply_stream({"i": i})
            await current_message().reply_final({"i": "done"})

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(Streamer)
        received = [item async for item in ref.ask_stream({"n": 3})]
        assert received == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": "done"}]
    finally:
        await node.stop()


async def test_ask_stream_with_only_a_final_reply():
    @actor(name="one_shot")
    class OneShot:
        async def receive(self, msg):
            await current_message().reply_final({"result": msg["x"] * 2})

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(OneShot)
        received = [item async for item in ref.ask_stream({"x": 21})]
        assert received == [{"result": 42}]
    finally:
        await node.stop()


async def test_ask_stream_hidden_mailbox_is_cleaned_up():
    @actor(name="cleanup_check")
    class CleanupCheck:
        async def receive(self, msg):
            await current_message().reply_final("ok")

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(CleanupCheck)
        before = set(node._mailboxes)
        async for _item in ref.ask_stream({}):
            pass
        after = set(node._mailboxes)
        assert after == before  # no leftover __lapinbeam_ask_N__ mailbox
    finally:
        await node.stop()


async def test_ask_stream_times_out_between_items():
    @actor(name="stalls")
    class Stalls:
        async def receive(self, msg):
            await current_message().reply_stream({"i": 0})
            await asyncio.sleep(10)  # never gets here before the test ends
            await current_message().reply_final({"i": 1})

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(Stalls)
        received = []
        with pytest.raises(TimeoutError):
            async for item in ref.ask_stream({}, timeout=0.2):
                received.append(item)
        assert received == [{"i": 0}]
    finally:
        await node.stop()


async def test_ask_stream_rejects_a_plain_reply():
    @actor(name="wrong_reply")
    class WrongReply:
        async def receive(self, msg):
            await current_message().reply({"oops": True})  # should have used reply_stream/reply_final

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        ref = sup.spawn(WrongReply)
        with pytest.raises(RuntimeError, match="reply_stream"):
            async for _item in ref.ask_stream({}):
                pass
    finally:
        await node.stop()


async def test_cross_node_ask_stream():
    @actor(name="remote_streamer")
    class RemoteStreamer:
        async def receive(self, msg):
            for i in range(msg["n"]):
                await current_message().reply_stream({"i": i})
            await current_message().reply_final({"i": "done"})

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_b = Supervisor(node=node_b)
        sup_b.spawn(RemoteStreamer)
        await node_a.connect_peer(node_b.local_id)
        remote = node_a.get_remote_actor(node_b.local_id, "remote_streamer")
        received = [item async for item in remote.ask_stream({"n": 2})]
        assert received == [{"i": 0}, {"i": 1}, {"i": "done"}]
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_pool_ask_stream_combines_both_features():
    """The exact pattern examples/order_stream/ uses: a pool worker
    reports progress via reply_stream()/reply_final(), the caller follows
    it with ask_stream() — spawn_pool() and ask_stream() composing
    cleanly."""

    async def handler(msg):
        for i in range(msg["steps"]):
            await asyncio.sleep(0.01)
            await current_message().reply_stream({"step": i})
        await current_message().reply_final({"step": "done"})

    node = Node("node@127.0.0.1:0")
    await node.start()
    try:
        sup = Supervisor(node=node)
        pool = await sup.spawn_pool(handler, 3, name="stream_pool")
        received = [item async for item in pool.ask_stream({"steps": 3})]
        assert received == [{"step": 0}, {"step": 1}, {"step": 2}, {"step": "done"}]
    finally:
        await node.stop()
