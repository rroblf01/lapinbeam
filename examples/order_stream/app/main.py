"""FastAPI HTTP layer only — the actual order work happens on a separate
`worker` node (a `Supervisor.spawn_pool()`, see worker/order.py), reached
over the network via `ask_stream()`.

`_relay()` calls `ask_stream()` **once per order**, not once per browser
tab: several tabs can open `/orders/{id}/stream` for the same order at
once, and all of them see the same live updates, because they all
subscribe to the local `pubsub` the one relay task is feeding — `ask_stream()`
itself only ever delivers to whoever called it, so fanning out to N
viewers is this process's job, not the pool's.
"""

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from lapinbeam import Node

import db
import pubsub

NODE_NAME = os.environ.get("NODE_NAME", "app@127.0.0.1:9000")
WORKER_NODE = os.environ.get("WORKER_NODE", "worker@worker:9001")
POOL_NAME = "order_pool"

node = None


@asynccontextmanager
async def lifespan(app):
    global node
    await db.connect()
    node = Node(NODE_NAME, connect_timeout=30.0)
    await node.start()

    for _ in range(30):
        try:
            await node.connect_peer(WORKER_NODE)
            break
        except ConnectionError:
            await asyncio.sleep(1.0)
    else:
        raise RuntimeError(f"no se pudo conectar con worker en {WORKER_NODE!r}")

    yield
    await node.stop()
    await db.disconnect()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


async def _relay(order_id):
    pool = node.get_remote_actor(WORKER_NODE, POOL_NAME)
    try:
        async for update in pool.ask_stream({"order_id": order_id}, timeout=None):
            pubsub.publish(order_id, update)
    except Exception:
        # Postgres already has whatever the worker managed to persist
        # before this failed — a client that reconnects still sees the
        # real state via the catch-up read below, just not live from here
        # on. See the README for what this doesn't cover (the worker
        # process dying mid-stream, which would leave this hanging on
        # `timeout=None` instead of raising at all).
        logging.exception("relay for order %s failed", order_id)


@app.post("/orders")
async def create_order():
    order_id = str(uuid.uuid4())
    await db.create_order(order_id)
    asyncio.create_task(_relay(order_id))
    return {"id": order_id}


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    found = await db.get_order(order_id)
    if found is None:
        raise HTTPException(status_code=404, detail="order not found")
    return found


@app.get("/orders/{order_id}/stream")
async def stream_order(order_id: str):
    found = await db.get_order(order_id)
    if found is None:
        raise HTTPException(status_code=404, detail="order not found")

    async def events():
        # Subscribe *before* the catch-up read — a step that completes in
        # the gap between "read current state" and "register as a
        # subscriber" would otherwise notify no one.
        queue = pubsub.subscribe(order_id)
        try:
            current = await db.get_order(order_id)
            yield _sse(current)
            if current["status"] != "en_progreso":
                return
            while True:
                event = await queue.get()
                yield _sse(event)
                if event["status"] in ("completado", "error"):
                    return
        finally:
            pubsub.unsubscribe(order_id, queue)

    return StreamingResponse(events(), media_type="text/event-stream")


def _sse(payload):
    return f"data: {json.dumps(payload)}\n\n"
