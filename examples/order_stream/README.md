# Ejemplo: pedidos en paralelo con FastAPI + SSE + Postgres

Tres contenedores — `postgres`, `app` y `worker` — donde cada
`POST /orders` en `app` arranca, en `worker`, un pedido con 4 pasos
secuenciales que simulan llamadas a un proveedor de IA
(`await asyncio.sleep(random.uniform(1, 3))`). El progreso se guarda en
Postgres y se retransmite en tiempo real por Server-Sent Events — cierra
la pestaña, vuelve a abrir `/orders/{id}/stream` y retoma exactamente por
donde iba, porque el estado vive en Postgres, no en memoria de ningún
proceso.

## Cómo está montado

`app` (la capa HTTP) y `worker` (el pool de actores lapinbeam que hace el
trabajo real) son **procesos y contenedores separados**, hablando por red
como dos nodos lapinbeam cualquiera — no una capa HTTP y un "hilo de
fondo" compartiendo memoria en el mismo proceso. Esto importa de verdad:
`worker` se puede reiniciar sin tirar abajo la API, escalar por separado
si algún día hiciera falta, y un pico de tráfico HTTP en `app` no le quita
CPU al trabajo que `worker` tiene entre manos.

```
┌─────────────── app (FastAPI) ───────────────┐   ┌────────── worker ───────────┐
│                                              │   │                             │
│  POST /orders                                │   │  sup.spawn_pool(            │
│    crea fila en Postgres (en_progreso)       │   │    process_order,           │
│    asyncio.create_task(_relay(id)) ──┐       │   │    MAX_PARALLEL,            │
│                                       │       │   │    name="order_pool")      │
│  _relay(id):                         │       │   │                             │
│    async for update in               │       │   │  process_order(msg):        │
│      pool.ask_stream({id}) ──────────┼──────▶│   │    for step in STEPS:       │
│        pubsub.publish(id, update)    │       │   │      sleep(1-3s)  # IA      │
│                                       │       │   │      guarda en Postgres     │
│  GET /orders/{id}/stream             │       │   │      reply_stream(estado) ──┼──▶ (de vuelta
│    lee Postgres (catch-up)           │       │   │    reply_final(estado)      │     al mismo
│    se suscribe a pubsub.py (memoria)◀┘       │   │                             │     ask_stream)
│    reenvía cada evento tal cual llega        │   │                             │
└──────────────────────────────────────────────┘   └─────────────────────────────┘
                    │                                            │
                    └──────────────── Postgres ──────────────────┘
                          (única fuente de verdad durable)
```

- **`sup.spawn_pool()`** (`lapinbeam` ≥ 1.3.0) reemplaza lo que antes
  había que montar a mano: un actor "dispatcher" reservado, una cola
  compartida, y `N` actores-worker creados con una factoría por índice.
  `process_order()` es ahora una función normal — nada de eso hace falta
  ya, ver la sección de abajo para la comparación completa.
- **`ask_stream()`/`reply_stream()`/`reply_final()`** reemplazan el trío
  `dispatcher`/`ticks`/`pubsub.py` que reenviaba el progreso a mano: el
  worker llama a `current_message().reply_stream()` en cada paso y a
  `reply_final()` al terminar, y `_relay()` en `app` los recibe
  directamente con `pool.ask_stream(...)` — sin actor relé, sin envoltorio
  manual.
- **`_relay()` corre una sola vez por pedido, no una vez por pestaña.**
  `ask_stream()` solo le entrega a quien preguntó — así que si abrieras
  una pestaña nueva por cada `ask_stream()`, la segunda pestaña que
  mirase el mismo pedido no vería nada. En vez de eso, `_relay()` reenvía
  cada actualización al `pubsub.py` local, y **cualquier** número de
  conexiones SSE que se suscriban a ese `order_id` ven lo mismo en
  tiempo real — verificado abriendo dos streams a la vez sobre el mismo
  pedido: ambas reciben idénticos eventos, al mismo tiempo.
- **Postgres sigue siendo la única fuente de verdad durable.** Si un
  push se pierde (`app` reiniciando justo en ese instante, por ejemplo),
  es una actualización en vivo perdida, nunca un dato perdido — quien
  reconecte después ve el estado real leyendo Postgres directamente.

> **Nota:** `Supervisor.spawn_pool()` y `ask_stream()`/`reply_stream()`/
> `reply_final()` son nuevos en el paquete y todavía no están publicados
> en PyPI en el momento de escribir esto — hace falta `lapinbeam>=1.3.0`
> (ver `CHANGELOG.md`). Hasta entonces, `docker compose up --build`
> construye la imagen sin problema pero el proceso fallará porque la
> versión instalada desde PyPI todavía no trae esas funciones.

## Ejecutarlo

```bash
cd examples/order_stream
docker compose up --build              # MAX_PARALLEL=200 por defecto (tamaño del pool en `worker`)
# o, por ejemplo:
MAX_PARALLEL=1000 docker compose up --build
```

Abre <http://localhost:8000>, pulsa "Nuevo pedido", y observa los pasos
llegar en vivo. Cierra la pestaña y vuelve a abrir la misma URL (lleva el
id en `?id=...`) — verás el estado actual al instante, tanto si sigue en
marcha como si ya terminó.

## Generador de carga

```bash
cd bench
uv run python load_test.py -n 1000   # dispara N pedidos, sigue el SSE
                                      # de cada uno hasta el final
```

## De un actor por pedido a un pool fijo: qué cambió y qué se midió

La primera versión de este ejemplo creaba **un actor lapinbeam nuevo por
cada pedido** (una clase Python nueva, registrada en el core de Rust, con
su propio mailbox), limitando solo la llamada a la IA con un semáforo — y
retirándolo (`ask()` + `ref.task.cancel()`) al terminar. Con 1000 pedidos
eso son 1000 clases creadas y 1000 retiradas, aunque solo `MAX_PARALLEL`
pudieran hacer trabajo útil a la vez. Ese coste de creación/destrucción —
no el trabajo en sí — era lo que aparecía como ráfagas de CPU. La versión
con pool fijo (workers creados una sola vez al arrancar) mejoró justo
eso — la comparación completa, con números reales, está más abajo.

Repetí las mismas corridas con los tres diseños (actor-por-pedido en un
proceso único; pool fijo en un proceso único; pool fijo repartido en
`app`+`worker` separados), contra los mismos contenedores, Postgres
limpio antes de cada una:

| Escenario | v1: actor por pedido (1 proceso) | v2: pool fijo (1 proceso) | v3: pool fijo, `app`+`worker` separados |
| --- | --- | --- | --- |
| N=1 | 9.6s · CPU 0.1% | 7.6s · CPU 0.1% | — |
| N=10 | 9.2s · CPU 0.8% | 8.7s · CPU 0.6% | — |
| N=200 | 11.5s · CPU pico 13.7% · 199/200 | 11.3s · CPU pico 15.0% · 199/200 | 11.9s · CPU pico `app` 18.8% / `worker` 7.6% · 198/200 |
| **N=1000, `MAX_PARALLEL=200`** | **43.1s · CPU pico 65.3% · RAM pico 128 MiB · 994/1000** | **48.5s · CPU pico 42.1% · RAM pico 97 MiB · 1000/1000** | **46.8s · CPU pico `app` 51.2% / `worker` 15.3% · RAM `app` 95 MiB / `worker` ~41 MiB · 997/1000** |
| N=1000, pool = N (sin cola) | 12.9s · CPU pico 97.6% · RAM pico 117 MiB · 1000/1000 | 13.5s · CPU pico 89-98%\* · RAM pico 108 MiB · 1000/1000 | — |

\* Con el pool ya del mismo tamaño que N, no hay ninguna cola que
suavizar — la ráfaga de CPU que queda ahí ya no es "crear 1000 actores"
(eso pasó una vez, al arrancar, antes de la prueba), sino simplemente
atender 1000 conexiones HTTP nuevas y hacer ~2000 idas y vueltas a
Postgres (INSERT + SELECT de catch-up) casi al mismo tiempo — un coste
que existiría igual con cualquier framework, no algo que el diseño de
actores pueda evitar.

### Lecturas

- **La mejora del pool fijo frente al actor-por-pedido se mantiene al
  separar `app` y `worker`**: con 1000 pedidos y `MAX_PARALLEL=200`, v3
  completa 997/1000 (v1: 994/1000) en un tiempo comparable a v1 y v2, sin
  que ningún contenedor se acerque a saturar su CPU (picos de 51% y 15%,
  en una máquina de 12 cores).
- **La CPU se reparte según el rol de cada contenedor**: `app` (HTTP +
  reenvío de SSE) siempre pica más alto que `worker` (el trabajo real) —
  en la corrida de 1000, 51.2% contra 15.3%. Tiene sentido: gestionar
  1000 conexiones HTTP entrantes y sus streams SSE es más caro que
  ejecutar `asyncio.sleep()` 4000 veces. Es exactamente la separación de
  responsabilidades que se buscaba al partir el proceso en dos.
- **`worker` apenas mueve su RAM bajo carga** (~39→41 MiB durante toda la
  corrida de 1000) — el pool de actores es un coste fijo pagado al
  arrancar; procesar más o menos pedidos no lo cambia. Toda la variación
  de RAM ocurre en `app` (69→95 MiB), donde sí crece con el número de
  conexiones HTTP/SSE abiertas a la vez.
- **Separar procesos tiene un coste fijo pequeño**: dos runtimes Python
  separados (cada uno con su propio intérprete, su propio asyncio, su
  propia conexión a Postgres) pesan un poco más en conjunto que uno
  solo — normal y esperable, es el precio de poder reiniciar/escalar cada
  pieza por separado.
- Los picos de CPU que quedan siguen siendo el mismo artefacto que ya se
  documentó antes: ráfagas cortas (~1-1.5s) por lo sincronizado del propio
  test (1000 pedidos naciendo y terminando casi a la vez), no una meseta
  sostenida — con tráfico real, repartido en el tiempo, esto se aplanaría.

### v4: los mismos `app`/`worker` separados, con `spawn_pool()`/`ask_stream()` en vez de código a mano

La v3 de la tabla de arriba (`app`+`worker` separados) usaba un
`dispatcher` reservado, una cola y una factoría de clases-worker escritos
a mano en `worker/`, y un actor `ticks` + `pubsub.py` en `app/` para
reenviar el progreso. La versión actual del código sustituye todo eso por
`sup.spawn_pool(process_order, MAX_PARALLEL, name="order_pool")` en el
worker y `pool.ask_stream(...)` + un único `_relay()` en la API — mismo
comportamiento, con `worker/order.py` reducido a una sola función y sin
`ticks.py` en absoluto.

El rendimiento no debería cambiar (es exactamente el mismo patrón, con
menos código alrededor) — lo confirmé con una corrida de 100 pedidos tras
el cambio: 100/100 completados en 10.75s, en línea con los números de v3
de la tabla de arriba, no una regresión. No repetí la batería completa de
1000 con y sin límite, ya que ambas primitivas nuevas están cubiertas
además por tests dedicados (`tests-python/test_pool.py`,
`test_ask_stream.py`).

Lo que sí verifiqué específicamente, porque es justo el problema que
`ask_stream()` por sí solo no resuelve (ver
[Streaming replies con ask_stream()](../../docs/getting-started.es.md#respuestas-en-streaming-con-ask_stream)):
abrir **dos** conexiones `GET /orders/{id}/stream` a la vez sobre el
mismo pedido — ambas reciben exactamente los mismos eventos, al mismo
tiempo, porque `_relay()` llama a `ask_stream()` una sola vez por pedido
(no una vez por conexión) y reenvía al `pubsub.py` local, que sí sabe
repartir a cuantos haya suscritos.

## Lo que este ejemplo no resuelve (y por qué es aceptable aquí)

- **Un único `worker`.** Si se cae, ningún pedido nuevo se procesa hasta
  que vuelva (los ya encolados en Postgres como `en_progreso` se quedan
  huérfanos hasta entonces) — `docker compose` lo reinicia solo, pero no
  hay redundancia real. Para eso haría falta más de un `worker` detrás de
  un mecanismo de reparto — `app` podría conectarse a varios y elegir uno
  por `round_robin`/salud, o los propios `worker` podrían coordinarse con
  `lapinbeam.registry` (ver
  [Patrones inspirados en OTP](../../docs/otp-patterns.md)) para que solo
  uno se anuncie como activo a la vez.
- **Reparto por cola compartida, no por prioridad.** Todos los pedidos
  son iguales para el pool — no hay forma de decir "este es más urgente,
  procésalo antes que los que ya están en cola". Para eso haría falta una
  cola con prioridad en vez de un `asyncio.Queue` simple.
- **`_relay()` usa `timeout=None`** (espera indefinidamente entre
  actualizaciones) — si `worker` se cae a mitad de un pedido sin llegar a
  responder `reply_final()`, ese `_relay()` se queda esperando para
  siempre en vez de fallar con un `TimeoutError` observable. Postgres ya
  tiene lo que se guardó hasta ese punto (nunca se pierde el dato), pero
  la tarea de relay en sí queda colgada hasta que `app` se reinicie. Un
  `timeout` finito razonable (o combinar esto con `lapinbeam.monitors`
  sobre el pool, ver [Patrones inspirados en OTP](../../docs/otp-patterns.md))
  lo resolvería, a costa de tener que decidir qué "razonable" significa
  para pedidos que de verdad pueden tardar minutos.
