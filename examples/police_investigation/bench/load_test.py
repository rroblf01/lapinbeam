"""Load generator for the police-investigation example's API.

Zero third-party dependencies on purpose, so it runs with nothing more
than `uv run python bench/load_test.py` — uv doesn't even need to resolve
anything — or a plain `python3 bench/load_test.py`.

Fires POST /investigations concurrently and reports HTTP-level throughput
and latency. This only measures how fast the API *accepts* a case (202
Accepted, fire-and-forget) — not how long the full investigation pipeline
takes to reach "archivado". Poll GET /investigations after a run to see
completed cases.
"""

import argparse
import json
import random
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

NOMBRES = ["Ana", "Luis", "Marta", "Carlos", "Sofía", "Javier", "Elena", "Pablo"]
DESCRIPCIONES = [
    "robo con fuerza en domicilio",
    "hurto en vía pública",
    "daños a vehículo estacionado",
    "amenazas reiteradas",
    "estafa telefónica",
]
UBICACIONES = ["Calle Mayor 12", "Avenida del Puerto 5", "Plaza España 3", "Calle Sol 21"]


def send_one(base_url):
    body = json.dumps(
        {
            "denunciante": random.choice(NOMBRES),
            "descripcion": random.choice(DESCRIPCIONES),
            "ubicacion": random.choice(UBICACIONES),
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/investigations",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            resp.read()
    except urllib.error.HTTPError as e:
        status = e.code
    return status, time.perf_counter() - start


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("-n", "--requests", type=int, default=300)
    parser.add_argument("-c", "--concurrency", type=int, default=25)
    args = parser.parse_args()

    print(f"Enviando {args.requests} denuncias a {args.base_url} (concurrencia={args.concurrency})...")
    start = time.perf_counter()
    statuses = {}
    latencies = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(send_one, args.base_url) for _ in range(args.requests)]
        for fut in as_completed(futures):
            status, elapsed = fut.result()
            statuses[status] = statuses.get(status, 0) + 1
            latencies.append(elapsed)
    total = time.perf_counter() - start
    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p99 = latencies[int(len(latencies) * 0.99) - 1]

    print(f"Total: {total:.2f}s  ({args.requests / total:.1f} req/s)")
    print(f"Códigos de estado: {statuses}")
    print(
        "Latencia HTTP de aceptación (no de la investigación completa): "
        f"p50={p50 * 1000:.1f}ms p99={p99 * 1000:.1f}ms"
    )


if __name__ == "__main__":
    main()
