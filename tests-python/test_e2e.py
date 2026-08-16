"""End-to-end acceptance test mirroring the two-node example from the docs.

Node A runs an `ingestor` actor; Node B runs a `processor` actor. A sends
TASK messages, B replies with ACK messages, all over a single multiplexed
TCP connection maintained by the Rust core.
"""

from helpers import wait_until
from lapinbeam import Node, Supervisor, actor


async def test_doc_example_bidirectional_flow():
    acks = []

    @actor(name="ingestor")
    class IngestorActor:
        def __init__(self):
            self.processed_count = 0

        async def receive(self, msg):
            if msg.get("type") == "ACK":
                self.processed_count += 1
                acks.append(msg)
                return {"status": "ok"}
            return {"status": "ignored"}

    @actor(name="processor")
    class ProcessorActor:
        def __init__(self, node_ref, peer_id):
            self.node = node_ref
            self.peer_id = peer_id

        async def receive(self, msg):
            if msg.get("type") == "TASK":
                result = msg["payload_id"] * 2
                remote_ingestor = self.node.get_remote_actor(self.peer_id, msg["reply_to"])
                await remote_ingestor.send({"type": "ACK", "result": result})
                return {"status": "processed"}
            return {"status": "error"}

    node_a = Node("node_a@127.0.0.1:0")
    node_b = Node("node_b@127.0.0.1:0")
    await node_a.start()
    await node_b.start()
    try:
        sup_a = Supervisor(strategy="one_for_one", node=node_a)
        sup_b = Supervisor(strategy="one_for_one", node=node_b)
        ingestor_ref = sup_a.spawn(IngestorActor)
        sup_b.spawn(ProcessorActor, node_b, node_a.local_id)

        await node_a.connect_peer(node_b.local_id)
        remote_processor = node_a.get_remote_actor(node_b.local_id, "processor")

        n = 20
        for i in range(n):
            await remote_processor.send(
                {"type": "TASK", "payload_id": i, "reply_to": "ingestor"}
            )

        await wait_until(lambda: len(acks) == n)
        assert sorted(msg["result"] for msg in acks) == [i * 2 for i in range(n)]
        assert ingestor_ref._node is node_a
    finally:
        await node_a.stop()
        await node_b.stop()
