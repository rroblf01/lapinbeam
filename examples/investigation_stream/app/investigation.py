"""One lapinbeam actor per investigation — not one shared actor per
pipeline stage (see docs/getting-started.md's "Concurrency" section for
why that distinction matters: a shared actor would serialize every
investigation behind the same mailbox). Each investigation's four steps
run inside a single actor, one after another for *that* investigation,
but every investigation's actor runs concurrently with every other's.

`AI_SEMAPHORE` gates the actual "AI call" (simulated here with
`asyncio.sleep`) — this is the knob a real AI provider's rate limit would
require regardless of how many investigations exist; it's deliberately
*not* a limit on how many investigation actors can exist, only on how
many are allowed to be mid-"AI call" at once.
"""

import asyncio
import contextlib
import os
import random

from lapinbeam import actor, current_message

import db
import pubsub

STEPS = [
    ("admision", "La IA admite la denuncia a trámite"),
    ("recogida_pruebas", "La IA sugiere qué pruebas recopilar"),
    ("analisis", "La IA analiza las pruebas y calcula un índice de sospecha"),
    ("conclusion", "La IA redacta la conclusión final"),
]

_max_parallel = int(os.environ.get("MAX_PARALLEL", "200"))
AI_SEMAPHORE = asyncio.Semaphore(_max_parallel) if _max_parallel > 0 else contextlib.nullcontext()


async def _simulate_ai_call():
    async with AI_SEMAPHORE:
        await asyncio.sleep(random.uniform(1.0, 3.0))


def build_investigation(investigation_id):
    @actor(name=f"investigation_{investigation_id}")
    class Investigation:
        async def receive(self, msg):
            try:
                for step_name, detail in STEPS:
                    await _simulate_ai_call()
                    step = {"step": step_name, "detail": detail}
                    await db.append_step(investigation_id, step)
                    pubsub.publish(investigation_id, {"type": "step", **step})
                await db.mark_status(investigation_id, "completado")
                pubsub.publish(investigation_id, {"type": "status", "status": "completado"})
            except Exception as exc:
                await db.mark_status(investigation_id, "error")
                pubsub.publish(investigation_id, {"type": "status", "status": "error"})
                raise
            finally:
                await current_message().reply(None)

    return Investigation


async def run_investigation(sup, investigation_id):
    """Spawns the actor, runs it to completion, and retires it — `ask()`
    blocks until `receive()` above replies (success or failure alike),
    then `ref.task.cancel()` frees its mailbox/registration instead of
    leaving a finished actor idle forever waiting on an empty mailbox."""
    ref = sup.spawn(build_investigation(investigation_id))
    try:
        await ref.ask(None, timeout=None)
    finally:
        ref.task.cancel()
