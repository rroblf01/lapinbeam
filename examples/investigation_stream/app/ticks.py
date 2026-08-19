"""The one actor `worker` addresses on this node: a pure relay from
cross-node lapinbeam messages into the local, in-process pubsub that the
SSE endpoint (main.py) is actually subscribed to.
"""

from lapinbeam import actor

import pubsub


@actor(name="ticks")
class Ticks:
    async def receive(self, msg):
        pubsub.publish(msg["id"], msg)
