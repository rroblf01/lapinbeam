import asyncio
import os

from lapinbeam import Node, Supervisor, actor


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

    # Initialize the local node
    node = Node(node_name)
    await node.start()

    # Local supervisor
    sup = Supervisor(strategy="one_for_one")
    ingestor_ref = sup.spawn(IngestorActor)

    # Connect to Node B (retrying while the peer comes up).
    for _ in range(20):
        try:
            await node.connect_peer(peer)
            break
        except ConnectionError:
            await asyncio.sleep(0.5)
    else:
        raise RuntimeError(f"could not connect to peer {peer!r}")

    # Reference to the remote processor on Node B
    remote_processor = node.get_remote_actor(peer, "processor")

    # Continuous bidirectional send
    for i in range(100):
        # Send work to Node B passing our own reference for the callback
        await remote_processor.send(
            {
                "type": "TASK",
                "payload_id": i,
                "reply_to": "ingestor",
            }
        )
        await asyncio.sleep(0.01)

    # Give Node B time to flush the last ACKs, then shut down.
    await asyncio.sleep(1.0)
    await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
