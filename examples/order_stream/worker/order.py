"""Order processing, as a `Supervisor.spawn_pool()` — a fixed pool of
`MAX_PARALLEL` actors (see main.py), each running `process_order()` for
whichever order is next in the pool's internal queue.

This used to be built by hand: a reserved "dispatcher" actor, a
hand-rolled `asyncio.Queue`, a `build_worker(index)` factory spawning one
class per worker, and each worker's own `while True: await queue.get()`
loop. `spawn_pool()` (see `lapinbeam/supervisor.py`) packages exactly that
pattern — see `docs/getting-started.md`'s "Concurrency" section for why a
pool, not one actor, is what makes N orders actually run in parallel.

Progress is reported with `current_message().reply_stream()`/
`reply_final()` instead of a hand-built "ticks" relay actor — whoever
called `ask_stream()` on this pool (see app/main.py) gets every update
directly, in order, ending with the one sent via `reply_final()`.
"""

import asyncio
import random

from lapinbeam import current_message

import db

STEPS = [
    ("validacion_pedido", "La IA valida los datos del pedido"),
    ("comprobacion_stock", "La IA comprueba el stock disponible en el almacén"),
    ("calculo_precio", "La IA calcula el importe final del pedido"),
    ("prioridad_envio", "La IA determina la prioridad de envío"),
]


async def process_order(msg):
    order_id = msg["order_id"]
    steps = []
    try:
        for step_name, detail in STEPS:
            await asyncio.sleep(random.uniform(1.0, 3.0))  # llamada a la IA simulada
            steps.append({"step": step_name, "detail": detail})
            await db.append_step(order_id, steps[-1])
            await current_message().reply_stream({"status": "en_progreso", "steps": list(steps)})
        await db.mark_status(order_id, "completado")
        await current_message().reply_final({"status": "completado", "steps": list(steps)})
    except Exception:
        try:
            await db.mark_status(order_id, "error")
        except Exception:
            pass
        # Still reply_final(), not a plain exception: whoever is waiting
        # on ask_stream() needs to see "error" and stop, not hang until
        # its timeout just because this order specifically failed.
        await current_message().reply_final({"status": "error", "steps": list(steps)})
