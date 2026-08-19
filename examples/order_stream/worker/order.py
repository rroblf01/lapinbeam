"""A *fixed* pool of `MAX_PARALLEL` lapinbeam actors, created once at
startup — not one actor per order. Each worker loops forever, pulling the
next order id off a shared queue and running its steps, then going back
for the next one.

This replaced an earlier one-actor-per-order design: spawning (and later
retiring) a brand new actor for every order meant real, synchronous CPU
cost — a fresh Python class, registering a mailbox with the Rust core,
etc. — paid once per order even though only `MAX_PARALLEL` of them could
ever be doing real work at once. A fixed pool pays that cost exactly
`MAX_PARALLEL` times, period, no matter how many orders are ever
submitted — see the README's benchmark section for the measured
before/after.

A crash inside one order's steps is caught *inside* the loop, not allowed
to escape `receive()`: since nothing ever sends this actor another
message after its one startup "go", if `receive()` itself crashed the
worker would never restart — the crashed message would be gone. Catching
here means the worker just picks up the next order in the queue, same as
a `one_for_one` restart would, without needing one.
"""

import asyncio
import random

from lapinbeam import actor

import db

STEPS = [
    ("validacion_pedido", "La IA valida los datos del pedido"),
    ("comprobacion_stock", "La IA comprueba el stock disponible en el almacén"),
    ("calculo_precio", "La IA calcula el importe final del pedido"),
    ("prioridad_envio", "La IA determina la prioridad de envío"),
]

_queue = asyncio.Queue()


async def _notify(node, reply_node, order_id, status, steps):
    try:
        remote = node.get_remote_actor(reply_node, "ticks")
        await remote.send({"id": order_id, "status": status, "steps": list(steps)})
    except Exception:
        pass  # best-effort — Postgres already has the durable truth regardless


async def _run_steps(node, order_id, reply_node):
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
            await db.append_step(order_id, steps[-1])
            await _notify(node, reply_node, order_id, "en_progreso", steps)
        await db.mark_status(order_id, "completado")
        await _notify(node, reply_node, order_id, "completado", steps)
    except Exception:
        try:
            await db.mark_status(order_id, "error")
        except Exception:
            pass
        await _notify(node, reply_node, order_id, "error", steps)


def build_worker(index):
    @actor(name=f"order_worker_{index}")
    class Worker:
        def __init__(self, node_ref):
            self.node = node_ref

        async def receive(self, msg):
            while True:
                order_id, reply_node = await _queue.get()
                await _run_steps(self.node, order_id, reply_node)

    return Worker


@actor(name="dispatcher")
class Dispatcher:
    """The only actor `app` needs to know the name of. Just hands the
    request off to the shared queue — whichever pool worker is free next
    picks it up."""

    async def receive(self, msg):
        _queue.put_nowait((msg["order_id"], msg["reply_node"]))


async def start_pool(node, sup, n_workers):
    """Spawns the dispatcher and the fixed pool, and kicks off each
    worker's loop — an actor doesn't run anything until it receives its
    first message."""
    sup.spawn(Dispatcher)
    for i in range(n_workers):
        ref = sup.spawn(build_worker(i), node)
        await ref.send({"start": True})
