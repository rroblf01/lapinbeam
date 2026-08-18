"""FastAPI front door for the investigation pipeline.

Submitting the form is fire-and-forget from the HTTP caller's point of
view: the request just hands the case off to the `investigator` node and
returns immediately with a case id (202 Accepted) — it does not block for
however long the full pipeline takes. Progress is tracked by a local
`case_tracker` actor that the *other* nodes call back into directly as
each stage finishes, and exposed for polling via GET /investigations/{id}.
"""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from lapinbeam import Node, Supervisor, actor

NODE_NAME = os.environ.get("NODE_NAME", "api@127.0.0.1:9000")
INVESTIGATOR_NODE = os.environ.get("INVESTIGATOR_NODE", "investigator@investigator:9001")

#: case_id -> case state. In-memory only — this is a demo, not a real
#: case-management system (see the example's README for what a production
#: version would need instead: a real database, auth, etc).
CASES: dict[str, dict] = {}

node: Optional[Node] = None


@actor(name="case_tracker")
class CaseTrackerActor:
    """Receives progress updates from the investigator/archive nodes as a
    case moves through the pipeline and folds them into `CASES`."""

    async def receive(self, msg):
        case = CASES.get(msg["case_id"])
        if case is None:
            return  # stale update for a case this process no longer has (restart)
        case["status"] = msg["status"]
        case["history"].append({"status": msg["status"], "detail": msg.get("detail", {})})
        case.update(msg.get("detail", {}))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global node
    node = Node(NODE_NAME, connect_timeout=30.0)
    await node.start()
    Supervisor(node=node).spawn(CaseTrackerActor)

    for attempt in range(30):
        try:
            await node.connect_peer(INVESTIGATOR_NODE)
            break
        except ConnectionError:
            print(f"[api] investigator not ready yet (attempt {attempt + 1}), retrying...")
            await asyncio.sleep(1.0)
    else:
        raise RuntimeError(f"could not connect to investigator at {INVESTIGATOR_NODE!r}")

    print(f"[api] listo en {node.local_id}, conectado a {INVESTIGATOR_NODE}")
    yield
    await node.stop()


app = FastAPI(title="Investigación policial (demo lapinbeam)", lifespan=lifespan)


class Denuncia(BaseModel):
    denunciante: str
    descripcion: str
    ubicacion: str


@app.post("/investigations", status_code=202)
async def crear_investigacion(form: Denuncia):
    case_id = str(uuid.uuid4())
    CASES[case_id] = {
        "case_id": case_id,
        "status": "recibido",
        "form": form.model_dump(),
        "history": [],
    }
    intake = node.get_remote_actor(INVESTIGATOR_NODE, "intake")
    await intake.send({"case_id": case_id, "origin_node": node.local_id, "form": form.model_dump()})
    return {"case_id": case_id, "status_url": f"/investigations/{case_id}"}


@app.get("/investigations/{case_id}")
async def obtener_investigacion(case_id: str):
    case = CASES.get(case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="caso no encontrado")
    return case


@app.get("/investigations")
async def listar_investigaciones():
    return list(CASES.values())


@app.get("/health")
async def health():
    return {"status": "ok", "connected_peers": node.peer_count() if node else 0}
