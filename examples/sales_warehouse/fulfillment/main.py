"""Investigator node: runs the actual investigation pipeline as four
local actors chained together (intake -> collector -> analyzer ->
conclusion), then hands the closed case off to the archive node.

Each stage also sends a progress update straight back to the case's
*origin* node (the API service that received the original form) — not by
routing back through the chain, but by dialing that node directly using
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

EVIDENCE_POOL = [
    {"nombre": "huella dactilar", "peso": 25},
    {"nombre": "testigo ocular", "peso": 20},
    {"nombre": "grabación de una cámara de seguridad", "peso": 30},
    {"nombre": "rastro de ADN", "peso": 35},
    {"nombre": "coartada contradictoria", "peso": 15},
    {"nombre": "objeto encontrado en la escena", "peso": 10},
]


async def notify(node, msg, status, detail=None):
    """Reports progress back to the `case_tracker` actor on the node that
    originally received the form — regardless of how many local hops this
    case has gone through since."""
    tracker = node.get_remote_actor(msg["origin_node"], "case_tracker")
    await tracker.send({"case_id": msg["case_id"], "status": status, "detail": detail or {}})


@actor(name="intake")
class IntakeActor:
    """Stage 1: admite la denuncia a trámite."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        print(f"[investigator] caso {msg['case_id']}: denuncia admitida a trámite")
        await notify(self.node, msg, "en_investigacion", {"detalle": "denuncia admitida a trámite"})
        await ActorRef(self.node, "collector").send(msg)


@actor(name="collector")
class EvidenceCollectorActor:
    """Stage 2: recoge pruebas."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        n = random.randint(2, len(EVIDENCE_POOL))
        evidencias = random.sample(EVIDENCE_POOL, n)
        print(f"[investigator] caso {msg['case_id']}: {len(evidencias)} pruebas recogidas")
        await notify(
            self.node,
            msg,
            "pruebas_recogidas",
            {"evidencias": [e["nombre"] for e in evidencias]},
        )
        await ActorRef(self.node, "analyzer").send({**msg, "evidencias": evidencias})


@actor(name="analyzer")
class EvidenceAnalyzerActor:
    """Stage 3: analiza las pruebas recogidas."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        indice = min(100, sum(e["peso"] for e in msg["evidencias"]))
        print(f"[investigator] caso {msg['case_id']}: índice de sospecha = {indice}")
        await notify(self.node, msg, "pruebas_analizadas", {"indice_sospecha": indice})
        await ActorRef(self.node, "conclusion").send({**msg, "indice_sospecha": indice})


@actor(name="conclusion")
class ConclusionActor:
    """Stage 4: redacta la conclusión y remite el caso a archivo."""

    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        indice = msg["indice_sospecha"]
        if indice >= 70:
            conclusion = "pruebas suficientes para imputación"
        elif indice >= 40:
            conclusion = "se requiere ampliar la investigación"
        else:
            conclusion = "pruebas insuficientes, caso archivado provisionalmente"
        print(f"[investigator] caso {msg['case_id']}: conclusión = {conclusion!r}")
        await notify(self.node, msg, "concluido", {"conclusion": conclusion})

        archivist = self.node.get_remote_actor(ARCHIVE_NODE, "archivist")
        await archivist.send(
            {
                **msg,
                "evidencias": [e["nombre"] for e in msg["evidencias"]],
                "conclusion": conclusion,
            }
        )


async def main():
    node_name = os.environ.get("NODE_NAME", "investigator@127.0.0.1:9001")
    node = Node(node_name, connect_timeout=30.0)
    await node.start()

    sup = Supervisor(node=node)
    sup.spawn(IntakeActor, node)
    sup.spawn(EvidenceCollectorActor, node)
    sup.spawn(EvidenceAnalyzerActor, node)
    sup.spawn(ConclusionActor, node)

    # `connect_peer` already retries a failed first dial on its own (the
    # archive container may not be listening yet) — the loop here is just
    # a generous outer bound for the very first startup race.
    for attempt in range(30):
        try:
            await node.connect_peer(ARCHIVE_NODE)
            break
        except ConnectionError:
            print(f"[investigator] archive not ready yet (attempt {attempt + 1}), retrying...")
            await asyncio.sleep(1.0)
    else:
        raise RuntimeError(f"could not connect to archive at {ARCHIVE_NODE!r}")

    print(f"[investigator] listo en {node.local_id}, conectado a {ARCHIVE_NODE}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
