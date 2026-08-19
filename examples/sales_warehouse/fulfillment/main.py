"""Fulfillment node: runs the actual order pipeline as four local actors
chained together (intake -> stock_check -> valuation -> dispatch), then
hands the finished order off to the archive node.

Each stage also sends a progress update straight back to the order's
*origin* node (the API service that received the original request) — not
by routing back through the chain, but by dialing that node directly using
the `origin_node` id carried in the message payload itself. This is the
same pattern `tests-python/test_runtime.py::test_remote_send_between_two_nodes`
uses (an actor holding `node` + a peer id, building a fresh `RemoteRef` per
send) rather than `reply_to`/`correlation_id`, which is better suited to a
single request/response hop than a multi-stage pipeline.
"""

import asyncio
import os
import random

from lapinbeam import ActorRef, Node, Supervisor, actor

ARCHIVE_NODE = os.environ.get("ARCHIVE_NODE", "archive@archive:9002")
WORK_DELAY = float(os.environ.get("WORK_DELAY", "0.15"))

PRODUCT_POOL = [
    {"nombre": "monitor 27 pulgadas", "valor": 25},
    {"nombre": "teclado mecánico", "valor": 20},
    {"nombre": "silla de oficina", "valor": 30},
    {"nombre": "portátil 15 pulgadas", "valor": 35},
    {"nombre": "auriculares inalámbricos", "valor": 15},
    {"nombre": "webcam HD", "valor": 10},
]


async def notify(node, msg, status, detail=None):
    """Reports progress back to the `order_tracker` actor on the node that
    originally received the order — regardless of how many local hops this
    order has gone through since."""
    tracker = node.get_remote_actor(msg["origin_node"], "order_tracker")
    await tracker.send({"order_id": msg["order_id"], "status": status, "detail": detail or {}})


@actor(name="intake")
class IntakeActor:
    """Stage 1: admite el pedido para su preparación."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        print(f"[fulfillment] pedido {msg['order_id']}: admitido para preparación")
        await notify(self.node, msg, "en_preparacion", {"detalle": "pedido admitido para preparación"})
        await ActorRef(self.node, "stock_check").send(msg)


@actor(name="stock_check")
class StockCheckActor:
    """Stage 2: comprueba el stock disponible en el almacén."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        n = random.randint(2, len(PRODUCT_POOL))
        productos = random.sample(PRODUCT_POOL, n)
        print(f"[fulfillment] pedido {msg['order_id']}: {len(productos)} productos reservados en stock")
        await notify(
            self.node,
            msg,
            "stock_reservado",
            {"productos": [p["nombre"] for p in productos]},
        )
        await ActorRef(self.node, "valuation").send({**msg, "productos": productos})


@actor(name="valuation")
class ValuationActor:
    """Stage 3: calcula el importe total del pedido."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        importe = sum(p["valor"] for p in msg["productos"])
        print(f"[fulfillment] pedido {msg['order_id']}: importe total = {importe}")
        await notify(self.node, msg, "importe_calculado", {"importe_total": importe})
        await ActorRef(self.node, "dispatch").send({**msg, "importe_total": importe})


@actor(name="dispatch")
class DispatchActor:
    """Stage 4: decide la prioridad de envío y remite el pedido a archivo."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        importe = msg["importe_total"]
        if importe >= 70:
            decision = "envío urgente en 24h"
        elif importe >= 40:
            decision = "envío estándar en 3-5 días"
        else:
            decision = "pedido en espera de consolidación con otros envíos"
        print(f"[fulfillment] pedido {msg['order_id']}: decisión de envío = {decision!r}")
        await notify(self.node, msg, "listo_para_envio", {"decision_envio": decision})

        archivist = self.node.get_remote_actor(ARCHIVE_NODE, "archivist")
        await archivist.send(
            {
                **msg,
                "productos": [p["nombre"] for p in msg["productos"]],
                "decision_envio": decision,
            }
        )


async def main():
    node_name = os.environ.get("NODE_NAME", "fulfillment@127.0.0.1:9001")
    node = Node(node_name, connect_timeout=30.0)
    await node.start()

    sup = Supervisor(node=node)
    sup.spawn(IntakeActor, node)
    sup.spawn(StockCheckActor, node)
    sup.spawn(ValuationActor, node)
    sup.spawn(DispatchActor, node)

    # `connect_peer` already retries a failed first dial on its own (the
    # archive container may not be listening yet) — the loop here is just
    # a generous outer bound for the very first startup race.
    for attempt in range(30):
        try:
            await node.connect_peer(ARCHIVE_NODE)
            break
        except ConnectionError:
            print(f"[fulfillment] archive not ready yet (attempt {attempt + 1}), retrying...")
            await asyncio.sleep(1.0)
    else:
        raise RuntimeError(f"could not connect to archive at {ARCHIVE_NODE!r}")

    print(f"[fulfillment] listo en {node.local_id}, conectado a {ARCHIVE_NODE}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
