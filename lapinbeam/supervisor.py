"""Supervisor: spawns actors and restarts them on unhandled exceptions."""

import asyncio
import time

from .actor import actor_name
from .node import get_current_node
from .refs import ActorRef


class Supervisor:
    """Owns actor lifecycle. On crash, restarts the actor with backoff.

    Supported strategies:
      - ``one_for_one``: restart only the crashed actor (MVP).
    """

    def __init__(self, strategy="one_for_one", node=None, max_restarts=3,
                 restart_window=5.0):
        if strategy != "one_for_one":
            raise ValueError(f"unsupported strategy: {strategy!r}")
        self._node = node
        self.max_restarts = max_restarts
        self.restart_window = restart_window
        self._restart_times = []
        self._watchers = []

    @property
    def node(self):
        if self._node is None:
            self._node = get_current_node()
        if self._node is None:
            raise RuntimeError("no node started; start a Node or pass node= to Supervisor")
        return self._node

    def spawn(self, actor_cls, *args, **kwargs):
        """Spawns an actor and returns an `ActorRef`. Non-blocking."""
        node = self.node
        name = actor_name(actor_cls)
        # Register the mailbox synchronously so sends can land immediately,
        # even though the watcher task only runs on the next loop iteration.
        mailbox = asyncio.Queue()
        node.register_actor(name, mailbox)
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._watch(node, name, actor_cls, args, kwargs, mailbox))
        self._watchers.append(task)
        return ActorRef(node, name, task=task)

    async def _watch(self, node, name, actor_cls, args, kwargs, first_mailbox):
        mailbox = first_mailbox
        while True:
            instance = actor_cls(*args, **kwargs)
            mailbox = mailbox or asyncio.Queue()
            node.register_actor(name, mailbox)
            driver = asyncio.create_task(self._drive(instance, mailbox))
            try:
                await driver
            except asyncio.CancelledError:
                node.unregister_actor(name)
                raise
            except Exception as exc:
                node.unregister_actor(name)
                mailbox = None
                if not self._allow_restart():
                    raise
                await asyncio.sleep(self._backoff())
                continue
            else:
                node.unregister_actor(name)
                return

    @staticmethod
    async def _drive(instance, mailbox):
        meta = type(instance).__lapinbeam_actor__
        handlers = meta["handlers"]
        default_handler = meta["default_handler"]
        while True:
            msg = await mailbox.get()
            if not handlers:
                await instance.receive(msg)
                continue
            handler_name = handlers.get(type(msg), default_handler)
            if handler_name is None:
                raise TypeError(
                    f"{type(instance).__name__} has no @on handler for "
                    f"{type(msg).__name__} messages and no @on(default=True) fallback"
                )
            await getattr(instance, handler_name)(msg)

    def _allow_restart(self):
        now = time.monotonic()
        self._restart_times = [t for t in self._restart_times
                               if now - t < self.restart_window]
        if len(self._restart_times) >= self.max_restarts:
            return False
        self._restart_times.append(now)
        return True

    def _backoff(self):
        return min(2 ** len(self._restart_times) * 0.05, 1.0)
