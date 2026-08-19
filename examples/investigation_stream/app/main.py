"""FastAPI + lapinbeam + Postgres: one lapinbeam actor per investigation
(see investigation.py), each one calling a simulated AI provider through a
process-wide semaphore (`MAX_PARALLEL`, default 200) and persisting its
progress to Postgres as it goes. `/investigations/{id}/stream` (SSE)
replays whatever's already in Postgres first, then live-streams new steps
— reload the page mid-investigation and it picks up exactly where it is.
"""

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from lapinbeam import Node, Supervisor

import db
import pubsub
from investigation import run_investigation

NODE_NAME = os.environ.get("NODE_NAME", "investigation_stream@127.0.0.1:9000")

node = None
sup = None


@asynccontextmanager
async def lifespan(app):
    global node, sup
    await db.connect()
    node = Node(NODE_NAME)
    await node.start()
    # max_restarts=0: an investigation that raises reports "error" to
    # Postgres and retires for good (see investigation.py) — no point
    # retrying a fresh instance against a "go" message that's already
    # gone, and one_for_one means it never affects any other investigation
    # sharing this Supervisor either way.
    sup = Supervisor(node=node, max_restarts=0)
    yield
    await node.stop()
    await db.disconnect()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


async def _run_and_log(investigation_id):
    try:
        await run_investigation(sup, investigation_id)
    except Exception:
        # investigation.py already persists "error" to Postgres before
        # this could ever fire — this is only a backstop against
        # something failing outside that (e.g. the DB write itself).
        logging.exception("investigation %s crashed outside its own handling", investigation_id)


@app.post("/investigations")
async def create_investigation():
    investigation_id = str(uuid.uuid4())
    await db.create_investigation(investigation_id)
    # Fire-and-forget from the HTTP response's point of view — the actual
    # work (and its own concurrency limit) happens independently of this
    # request/response cycle, exactly like the two-node docs example
    # doesn't wait for a reply before returning.
    asyncio.create_task(_run_and_log(investigation_id))
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
        # Subscribe *before* the catch-up read: otherwise a step that
        # completes in the gap between "read current state" and
        # "register as a subscriber" would notify no one, and — since a
        # very short investigation could have nothing else left to
        # publish — that update might never reach this client at all.
        queue = pubsub.subscribe(investigation_id)
        try:
            current = await db.get_investigation(investigation_id)
            yield _sse(current)
            if current["status"] != "en_progreso":
                return
            while True:
                await queue.get()
                current = await db.get_investigation(investigation_id)
                yield _sse(current)
                if current["status"] in ("completado", "error"):
                    return
        finally:
            pubsub.unsubscribe(investigation_id, queue)

    return StreamingResponse(events(), media_type="text/event-stream")


def _sse(payload):
    return f"data: {json.dumps(payload)}\n\n"
