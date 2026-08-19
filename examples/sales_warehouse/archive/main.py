"""Archive node: receives closed cases from the investigator and writes
them to disk (a mounted volume), then confirms back to the case's origin
node — a second, independent hop, on top of api->investigator->archive,
demonstrating that a reply doesn't have to retrace the same path it came
from as long as the two nodes have a connection to each other.
"""

import asyncio
import json
import os
from pathlib import Path

from lapinbeam import Node, Supervisor, actor

API_NODE = os.environ.get("API_NODE", "api@api:9000")
WORK_DELAY = float(os.environ.get("WORK_DELAY", "0.15"))
DATA_DIR = Path(os.environ.get("ARCHIVE_DIR", "/data/cases"))


@actor(name="archivist")
class ArchivistActor:
    def __init__(self, node):
        self.node = node

    async def receive(self, msg):
        await asyncio.sleep(WORK_DELAY)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        path = DATA_DIR / f"{msg['case_id']}.json"
        record = {
            "case_id": msg["case_id"],
            "formulario": msg["form"],
            "evidencias": msg["evidencias"],
            "indice_sospecha": msg["indice_sospecha"],
            "conclusion": msg["conclusion"],
        }
        path.write_text(json.dumps(record, indent=2, ensure_ascii=False))
        print(f"[archive] caso {msg['case_id']} archivado en {path}")

        tracker = self.node.get_remote_actor(msg["origin_node"], "case_tracker")
        await tracker.send(
            {
                "case_id": msg["case_id"],
                "status": "archivado",
                "detail": {"archivo": str(path)},
            }
        )


async def main():
    node_name = os.environ.get("NODE_NAME", "archive@127.0.0.1:9002")
    node = Node(node_name, connect_timeout=30.0)
    await node.start()
    Supervisor(node=node).spawn(ArchivistActor, node)

    for attempt in range(30):
        try:
            await node.connect_peer(API_NODE)
            break
        except ConnectionError:
            print(f"[archive] api not ready yet (attempt {attempt + 1}), retrying...")
            await asyncio.sleep(1.0)
    else:
        raise RuntimeError(f"could not connect to api at {API_NODE!r}")

    print(f"[archive] listo en {node.local_id}, conectado a {API_NODE}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
