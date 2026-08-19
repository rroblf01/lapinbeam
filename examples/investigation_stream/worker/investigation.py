"""A *fixed* pool of `MAX_PARALLEL` lapinbeam actors, created once at
startup — see the top-level README's "De un actor por investigación a un
pool fijo" section for why. `dispatcher` is the one reserved, well-known
actor `app`'s node addresses directly (the same trick lapinbeam's own
`lapinbeam.discovery`/`links`/`groups`/`registry` modules use for their
control-plane actors) — it just drops the incoming investigation id onto
the pool's shared queue and gets out of the way; the pool workers never
need to be individually addressable from outside this node at all.

Progress is pushed straight back to whichever `app` instance dispatched
the job (`reply_node`, carried in the dispatch message itself) — not
polled — by sending to a `ticks` actor on *that* node, reusing the same
TCP connection `app` already opened to reach `dispatcher` in the first
place. Postgres remains the durable source of truth regardless: a push
that fails to land (`app` restarting at the wrong instant, say) is a
missed *live* update, never lost data — anyone who reconnects afterward
gets the real current state from Postgres.
"""

import asyncio
import random

from lapinbeam import actor

import db

STEPS = [
    ("admision", "La IA admite la denuncia a trámite"),
    ("recogida_pruebas", "La IA sugiere qué pruebas recopilar"),
    ("analisis", "La IA analiza las pruebas y calcula un índice de sospecha"),
    ("conclusion", "La IA redacta la conclusión final"),
]

_queue = asyncio.Queue()


async def _notify(node, reply_node, investigation_id, status, steps):
    try:
        remote = node.get_remote_actor(reply_node, "ticks")
        await remote.send({"id": investigation_id, "status": status, "steps": list(steps)})
    except Exception:
        pass  # best-effort — Postgres already has the durable truth regardless


async def _run_steps(node, investigation_id, reply_node):
    # One broad try/except around the *entire* body, error path included:
    # nothing here may propagate back to the caller (build_worker's
    # `while True` loop) — an uncaught exception would crash this
    # persistent worker, permanently losing 1/MAX_PARALLEL of the pool's
    # capacity, since nothing will ever send it another message to
    # restart its loop.
    steps = []
    try:
        for step_name, detail in STEPS:
            await asyncio.sleep(random.uniform(1.0, 3.0))  # llamada a la IA simulada
            steps.append({"step": step_name, "detail": detail})
            await db.append_step(investigation_id, steps[-1])
            await _notify(node, reply_node, investigation_id, "en_progreso", steps)
        await db.mark_status(investigation_id, "completado")
        await _notify(node, reply_node, investigation_id, "completado", steps)
    except Exception:
        try:
            await db.mark_status(investigation_id, "error")
        except Exception:
            pass
        await _notify(node, reply_node, investigation_id, "error", steps)


def build_worker(index):
    @actor(name=f"investigation_worker_{index}")
    class Worker:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            while True:
                investigation_id, reply_node = await _queue.get()
                await _run_steps(self.node, investigation_id, reply_node)

    return Worker


@actor(name="dispatcher")
class Dispatcher:
    """The only actor `app` needs to know the name of. Just hands the
    request off to the shared queue — whichever pool worker is free next
    picks it up."""

    async def receive(self, msg):
        _queue.put_nowait((msg["investigation_id"], msg["reply_node"]))


async def start_pool(node, sup, n_workers):
    """Spawns the dispatcher and the fixed pool, and kicks off each
    worker's loop — an actor doesn't run anything until it receives its
    first message."""
    sup.spawn(Dispatcher)
    for i in range(n_workers):
        ref = sup.spawn(build_worker(i), node)
        await ref.send({"start": True})
