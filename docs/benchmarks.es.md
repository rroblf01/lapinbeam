# Benchmarks

Estas cifras salen directamente de los scripts en `bench/`, ejecutados en la
máquina del mantenedor (Python 3.14, red loopback). **No** son una suite de
benchmarks formal y reproducible con intervalos de confianza — trátalas como
una comprobación de orden de magnitud, y vuelve a ejecutarlas en tu propio
hardware antes de tomar una decisión basada en ellas:

```bash
uv run python bench/bench_remote.py   # throughput
uv run python bench/bench_latency.py  # percentiles de RTT
uv run python bench/bench_codec.py    # ruta de codec + conversión JSON, capa a capa
uv run python bench/bench_memory.py   # RSS bajo carga sostenida, churn de conexiones y backpressure de mailbox
```

## Throughput (`bench_remote.py`)

| Métrica | Resultado |
| --- | --- |
| `asyncio.Queue` put/get (línea base, sin lapinbeam) | ~1.6M msg/s |
| Envío local de actor en lapinbeam | ~440K msg/s |
| Envío remoto en lapinbeam (TCP loopback) | ~20K msg/s |

Metodología: `bench_asyncio_queue` mide un `asyncio.Queue` puro con una
corrutina productora y una consumidora como techo teórico para el paso de
mensajes en un solo proceso Python. `bench_local_send` envía al `ActorRef`
de un actor ya creado (2000 envíos de calentamiento, luego se cronometra).
`bench_remote_send` envía a un `RemoteRef` sobre una conexión TCP loopback
ya establecida (100 envíos de calentamiento + 200ms de asentamiento, luego
se cronometra) — esto es throughput de disparar-y-olvidar, no de
petición/respuesta.

La caída de aproximadamente 20x entre envío local y remoto es esperable: los
envíos locales son referencias a objetos Python sin copia dentro de un
`asyncio.Queue`; los envíos remotos pagan la codificación JSON, una
escritura con framing bincode a un socket TCP real (aunque loopback), y la
cola de salida por peer y la tarea de escritura del lado Rust. Ver "Codec"
más abajo para saber dónde se va el coste propio de la ruta remota.

## Codec (`bench_codec.py`)

Desglosa la ruta de envío remoto capa a capa — donde `full encode` y
`end-to-end` incluyen todo lo que está por encima en la tabla:

| Paso | Resultado |
| --- | --- |
| `codec.encode_payload` (Python: tagging de dataclass/Pydantic) | ~42K ops/s |
| `codec.decode_payload` (Python) | ~52K ops/s |
| `_core.encode_payload` (Rust: PyAny → JSON) | ~5K ops/s |
| `_core.decode_payload` (Rust: JSON → PyAny) | ~11K ops/s |
| Encode completo (`codec` + `_core`, lo que hace `_send_remote`) | ~4.5K ops/s |
| `remote.send()` de extremo a extremo (disparar-y-olvidar, conexión loopback real) | ~2.3K ops/s |

Metodología: 20.000 operaciones cronometradas por fila (200 de
calentamiento), sobre un payload moderadamente anidado — unos escalares,
un dict anidado, y una lista de 20 dicts anidados (ver el script para la
forma exacta). Es más pesado que el mensaje trivial `{"n": 1}` de
`bench_remote.py`, por eso estas cifras por operación son menores que la
cifra de ~20K msg/s de throughput de arriba — los dos scripts no miden el
mismo payload, solo la misma ruta de código.

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

## Memoria (`bench_memory.py`)

A diferencia de los benchmarks de throughput/latencia/codec de arriba, este
no da una cifra única que comparar — muestrea la RSS de este proceso
(`VmRSS` de `/proc/self/status`, **solo Linux**) a lo largo de tres
escenarios más largos e imprime si se estabiliza (sano) o sigue creciendo
(una fuga):

1. **Tráfico sostenido local + remoto** — la RSS debería estabilizarse en
   los primeros segundos y quedarse plana.
2. **Churn rápido de `connect_peer()`/`forget_peer()`**, ejecutado en
   varias rondas seguidas — una fuga real por ciclo sigue añadiendo
   aproximadamente lo mismo cada ronda; un comportamiento sano se aplana
   después de la primera.
3. **Un actor en bucle de caída permanente**, una vez con el buzón sin
   límite por defecto y otra con `mailbox_capacity` configurado —
   demuestra que la limitación de "buzones sin límite" (ver
   [Limitaciones](index.es.md#limitaciones)) es trivial de disparar de
   verdad (basta un consumidor lento o que se cae en bucle) y que
   `mailbox_capacity` de verdad la acota, disparando
   `on_event(kind="mailbox_full")` en vez de crecer para siempre.

Los valores absolutos de RSS dependen mucho de la máquina y no tiene
sentido compararlos entre ejecuciones o hardware — lo que importa es la
*forma* de la curva de cada escenario (plana frente a creciente), por eso
este script imprime una serie de muestras en vez de un único par
antes/después.

## Cómo leer estas cifras correctamente

- **Solo loopback.** Nada de esto mide latencia de red real entre hosts
  separados — espera que las cifras remotas estén dominadas por el RTT real
  de red una vez los nodos estén en máquinas distintas, no por el overhead
  propio de lapinbeam. Loopback es justo lo que aísla el coste del
  framework del coste de la red. Ver [Ejemplos](examples.md) para correr
  nodos entre hosts reales.
- **Un solo peer, un solo actor.** Los benchmarks de throughput/latencia/
  codec no ejercitan múltiples peers concurrentes ni muchos actores
  multiplexados sobre una conexión — aíslan el coste en el mejor caso de un
  único envío. `bench_memory.py` es la excepción: ejercita específicamente
  el churn de conexiones y el backpressure de mailbox.
- **No hay una comparación ejecutada contra Celery+RabbitMQ en este
  repositorio.** La comparación cualitativa de latencia en la página
  [frente a Celery + RabbitMQ](vs-celery-rabbitmq.md) (salto por broker
  típicamente entre unos pocos y varias decenas de milisegundos) es una
  característica general de la mensajería mediada por broker, no un
  benchmark ejecutado contra lapinbeam lado a lado bajo condiciones
  idénticas. Tómala como orientativa, no como una cifra que citar.
