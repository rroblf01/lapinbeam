# Benchmarks

Estas cifras salen directamente de los scripts en `bench/`, ejecutados en la
máquina del mantenedor (Python 3.14, red loopback). **No** son una suite de
benchmarks formal y reproducible con intervalos de confianza — trátalas como
una comprobación de orden de magnitud, y vuelve a ejecutarlas en tu propio
hardware antes de tomar una decisión basada en ellas:

```bash
uv run python bench/bench_remote.py   # throughput
uv run python bench/bench_latency.py  # percentiles de RTT
```

## Throughput (`bench_remote.py`)

| Métrica | Resultado |
| --- | --- |
| `asyncio.Queue` put/get (línea base, sin lapinbeam) | ~1.6M msg/s |
| Envío local de actor en lapinbeam | ~1.2M msg/s |
| Envío remoto en lapinbeam (TCP loopback) | ~16K msg/s |

Metodología: `bench_asyncio_queue` mide un `asyncio.Queue` puro con una
corrutina productora y una consumidora como techo teórico para el paso de
mensajes en un solo proceso Python. `bench_local_send` envía al `ActorRef`
de un actor ya creado (2000 envíos de calentamiento, luego se cronometra).
`bench_remote_send` envía a un `RemoteRef` sobre una conexión TCP loopback
ya establecida (100 envíos de calentamiento + 200ms de asentamiento, luego
se cronometra) — esto es throughput de disparar-y-olvidar, no de
petición/respuesta.

La caída de aproximadamente 75x entre envío local y remoto es esperable: los
envíos locales son referencias a objetos Python sin copia dentro de un
`asyncio.Queue`; los envíos remotos pagan la codificación JSON, una
escritura con framing bincode a un socket TCP real (aunque loopback), y la
cola de salida por peer y la tarea de escritura del lado Rust.

## Latencia (`bench_latency.py`)

| Métrica | Resultado |
| --- | --- |
| RTT de despacho local (envío → recepción) | p50 0.007 ms |
| RTT TCP loopback remoto (envío + ack) | p50 0.44 ms / p99 0.93 ms |

Metodología: 2000 round trips cronometrados (100 de calentamiento) por
medición. "Despacho local" cronometra un envío desde fuera de cualquier
actor a la mailbox de un actor `Client` y de vuelta a un `asyncio.Event`, es
decir, puro overhead de scheduling de Python/asyncio sin red involucrada.
"Loopback remoto" cronometra un round trip completo a través del transporte
Rust: el nodo A envía a un actor `Echo` en el nodo B, que responde al nodo A
sobre la misma conexión TCP multiplexada — esta es la cifra a comparar
contra un round trip mediado por un broker (ver
[lapinbeam frente a Celery + RabbitMQ](vs-celery-rabbitmq.md)).

## Cómo leer estas cifras correctamente

- **Solo loopback.** Nada de esto mide latencia de red real entre hosts
  separados — espera que las cifras remotas estén dominadas por el RTT real
  de red una vez los nodos estén en máquinas distintas, no por el overhead
  propio de lapinbeam. Loopback es justo lo que aísla el coste del
  framework del coste de la red. Ver [Ejemplos](examples.md) para correr
  nodos entre hosts reales.
- **Un solo peer, un solo actor.** Estos benchmarks no ejercitan múltiples
  peers concurrentes, backpressure de mailbox, ni muchos actores
  multiplexados sobre una conexión — aíslan el coste en el mejor caso de un
  único envío.
- **No hay una comparación ejecutada contra Celery+RabbitMQ en este
  repositorio.** La comparación cualitativa de latencia en la página
  [frente a Celery + RabbitMQ](vs-celery-rabbitmq.md) (salto por broker
  típicamente entre unos pocos y varias decenas de milisegundos) es una
  característica general de la mensajería mediada por broker, no un
  benchmark ejecutado contra lapinbeam lado a lado bajo condiciones
  idénticas. Tómala como orientativa, no como una cifra que citar.
