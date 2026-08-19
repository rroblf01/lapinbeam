"""FastAPI HTTP layer only — the actual investigation work happens on a
separate `worker` node (see worker/investigation.py), reached over the
network like any other lapinbeam peer. This container never runs an
investigation's steps itself; it only creates the Postgres row, dispatches
a message to `worker`'s `dispatcher` actor, and relays whatever `worker`
pushes back (via the local `ticks` actor, see ticks.py) into the SSE
stream the browser is watching.
"""

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from lapinbeam import Node, Supervisor

import db
import pubsub
from ticks import Ticks

NODE_NAME = os.environ.get("NODE_NAME", "app@127.0.0.1:9000")
WORKER_NODE = os.environ.get("WORKER_NODE", "worker@worker:9001")

node = None


@asynccontextmanager
async def lifespan(app):
    global node
    await db.connect()
    node = Node(NODE_NAME, connect_timeout=30.0)
    await node.start()
    Supervisor(node=node).spawn(Ticks)

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


@app.post("/investigations")
async def create_investigation():
    investigation_id = str(uuid.uuid4())
    await db.create_investigation(investigation_id)
    dispatcher = node.get_remote_actor(WORKER_NODE, "dispatcher")
    await dispatcher.send({"investigation_id": investigation_id, "reply_node": node.local_id})
    return {"id": investigation_id}


@app.get("/investigations/{investigation_id}")
async def get_investigation(investigation_id: str):
    found = await db.get_investigation(investigation_id)
    if found is None:
        raise HTTPException(status_code=404, detail="investigation not found")
    return found


@app.get("/investigations/{investigation_id}/stream")
async def stream_investigation(investigation_id: str):
    found = await db.get_investigation(investigation_id)
    if found is None:
        raise HTTPException(status_code=404, detail="investigation not found")

    async def events():
        # Subscribe *before* the catch-up read — a step that completes in
        # the gap between "read current state" and "register as a
        # subscriber" would otherwise notify no one.
        queue = pubsub.subscribe(investigation_id)
        try:
            current = await db.get_investigation(investigation_id)
            yield _sse(current)
            if current["status"] != "en_progreso":
                return
            while True:
                # `worker` pushes the full current state directly in
                # every tick (see worker/investigation.py's `_notify`) —
                # no re-read of Postgres needed here at all, unlike a
                # design that only got a "something changed" signal.
                event = await queue.get()
                yield _sse(event)
                if event["status"] in ("completado", "error"):
                    return
        finally:
            pubsub.unsubscribe(investigation_id, queue)

    return StreamingResponse(events(), media_type="text/event-stream")


def _sse(payload):
    return f"data: {json.dumps(payload)}\n\n"
