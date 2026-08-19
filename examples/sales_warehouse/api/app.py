"""FastAPI front door for the order fulfillment pipeline.

Submitting an order is fire-and-forget from the HTTP caller's point of
view: the request just hands the order off to the `fulfillment` node and
returns immediately with an order id (202 Accepted) — it does not block
for however long the full pipeline takes. Progress is tracked by a local
`order_tracker` actor that the *other* nodes call back into directly as
each stage finishes, and exposed for polling via GET /orders/{id}.
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
FULFILLMENT_NODE = os.environ.get("FULFILLMENT_NODE", "fulfillment@fulfillment:9001")

#: order_id -> order state. In-memory only — this is a demo, not a real
#: order-management system (see the example's README for what a
#: production version would need instead: a real database, auth, etc).
ORDERS: dict[str, dict] = {}

node: Optional[Node] = None


@actor(name="order_tracker")
class OrderTrackerActor:
    """Receives progress updates from the fulfillment/archive nodes as an
    order moves through the pipeline and folds them into `ORDERS`."""

    async def receive(self, msg):
        order = ORDERS.get(msg["order_id"])
        if order is None:
            return  # stale update for an order this process no longer has (restart)
        order["status"] = msg["status"]
        order["history"].append({"status": msg["status"], "detail": msg.get("detail", {})})
        order.update(msg.get("detail", {}))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global node
    node = Node(NODE_NAME, connect_timeout=30.0)
    await node.start()
    Supervisor(node=node).spawn(OrderTrackerActor)

    for attempt in range(30):
        try:
            await node.connect_peer(FULFILLMENT_NODE)
            break
        except ConnectionError:
            print(f"[api] fulfillment not ready yet (attempt {attempt + 1}), retrying...")
            await asyncio.sleep(1.0)
    else:
        raise RuntimeError(f"could not connect to fulfillment at {FULFILLMENT_NODE!r}")

    print(f"[api] listo en {node.local_id}, conectado a {FULFILLMENT_NODE}")
    yield
    await node.stop()


app = FastAPI(title="Almacén de ventas (demo lapinbeam)", lifespan=lifespan)


class Pedido(BaseModel):
    cliente: str
    producto: str
    direccion_envio: str


@app.post("/orders", status_code=202)
async def crear_pedido(form: Pedido):
    order_id = str(uuid.uuid4())
    ORDERS[order_id] = {
        "order_id": order_id,
        "status": "recibido",
        "form": form.model_dump(),
        "history": [],
    }
    intake = node.get_remote_actor(FULFILLMENT_NODE, "intake")
    await intake.send({"order_id": order_id, "origin_node": node.local_id, "form": form.model_dump()})
    return {"order_id": order_id, "status_url": f"/orders/{order_id}"}


@app.get("/orders/{order_id}")
async def obtener_pedido(order_id: str):
    order = ORDERS.get(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="pedido no encontrado")
    return order


@app.get("/orders")
async def listar_pedidos():
    return list(ORDERS.values())


@app.get("/health")
async def health():
    return {"status": "ok", "connected_peers": node.peer_count() if node else 0}
