"""E2E fixture: exercises auto-reconnect across a real peer container restart.

Same shape as app_node_a.py, but slower and longer-running — long enough for
CI to comfortably restart node_b's container mid-stream and still observe
node_a reconnect (on its own, via Node's default reconnect_interval) and keep
delivering. See docker-compose.restart.yml and the "docker e2e restart" CI job.
"""

import asyncio
import os

from lapinbeam import Node, Supervisor, actor

TOTAL = 40
INTERVAL = 0.5


@actor(name="ingestor")
class IngestorActor:
    def __init__(self):
        self.processed_count = 0

    async def receive(self, msg: dict) -> dict:
        if msg.get("type") == "ACK":
            self.processed_count += 1
            print(f"[Node A] Confirmation received from Node B. Total: {self.processed_count}")
            return {"status": "ok"}
        return {"status": "ignored"}


async def main():
    node_name = os.environ.get("NODE_NAME", "node_a@10.0.0.1:9001")
    peer = os.environ.get("PEER", "node_b@10.0.0.2:9002")

    node = Node(node_name, reconnect_interval=1.0)
    await node.start()

    sup = Supervisor(strategy="one_for_one")
    sup.spawn(IngestorActor)

    for _ in range(20):
        try:
            await node.connect_peer(peer)
            break
        except ConnectionError:
            await asyncio.sleep(0.5)
    else:
        raise RuntimeError(f"could not connect to peer {peer!r}")

    remote_processor = node.get_remote_actor(peer, "processor")

    for i in range(TOTAL):
        # Fire-and-forget: while node_b's container is down mid-restart, a
        # send simply has nowhere to go and is dropped — the point of this
        # fixture is proving node_a reconnects and delivery resumes once
        # node_b is back, not that every single send survives the restart.
        try:
            await remote_processor.send({"type": "TASK", "payload_id": i, "reply_to": "ingestor"})
        except ValueError:
            pass
        await asyncio.sleep(INTERVAL)

    await asyncio.sleep(2.0)
    await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
