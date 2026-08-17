"""E2E fixture: peer for e2e_restart_node_a.py — see that file's docstring."""

import asyncio
import os

from lapinbeam import Node, Supervisor, actor


@actor(name="processor")
class ProcessorActor:
    def __init__(self, node_ref: Node, peer_id: str):
        self.node = node_ref
        self.peer_id = peer_id

    async def receive(self, msg: dict) -> dict:
        if msg.get("type") == "TASK":
            payload_id = msg.get("payload_id")
            reply_to = msg.get("reply_to")
            result = payload_id * 2
            remote_ingestor = self.node.get_remote_actor(self.peer_id, reply_to)
            await remote_ingestor.send({"type": "ACK", "result": result})
            return {"status": "processed"}
        return {"status": "error"}


async def main():
    node_name = os.environ.get("NODE_NAME", "node_b@10.0.0.2:9002")
    peer = os.environ.get("PEER", "node_a@10.0.0.1:9001")

    node = Node(node_name)
    await node.start()

    sup = Supervisor(strategy="one_for_one")
    sup.spawn(ProcessorActor, node, peer)

    await node.wait_until_stopped()


if __name__ == "__main__":
    asyncio.run(main())
