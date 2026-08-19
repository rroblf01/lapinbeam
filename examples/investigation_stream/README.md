# Ejemplo: investigaciones paralelas con FastAPI + SSE + Postgres

Un servidor FastAPI en el que cada `POST /investigations` arranca una
investigación con 4 pasos secuenciales que simulan llamadas a un
proveedor de IA (`await asyncio.sleep(random.uniform(1, 3))`), va
guardando el progreso en Postgres, y lo retransmite en tiempo real por
Server-Sent Events — cierra la pestaña, vuelve a abrir
`/investigations/{id}/stream` y retoma exactamente por donde iba, porque
el estado vive en Postgres, no en memoria.

La pieza de lapinbeam es deliberadamente pequeña: **un actor por
investigación**, no un actor compartido por etapa. Es la misma lección de
[Concurrencia en Primeros pasos](../../docs/getting-started.md#concurrency-one-actor-handles-one-message-at-a-time) —
si las 4 etapas fueran actores fijos compartidos por todas las
investigaciones, se serializarían entre sí sin importar cuántas quisieras
lanzar en paralelo.

## Cómo está montado

```
POST /investigations                    GET /investigations/{id}/stream
        │                                          │
        ▼                                          ▼
  crea fila en Postgres              lee Postgres (catch-up) + se
  (status=en_progreso)               suscribe a las actualizaciones
        │                            en vivo (pubsub.py, en memoria)
        ▼                                          ▲
  sup.spawn(Investigation_id)                       │
        │                                           │
        ▼                                           │
  ┌─────────────────────────────┐                   │
  │ un actor lapinbeam por       │                   │
  │ investigación:               │                   │
  │  for step in STEPS:          │                   │
  │    async with AI_SEMAPHORE:  │───────────────────┘
  │      await sleep(1-3s)  # IA │  tras cada paso: UPDATE Postgres
  │    guarda paso en Postgres   │  + pubsub.publish()
  │  ask() responde -> se retira │
  │  (ref.task.cancel())         │
  └─────────────────────────────┘
```

- **`AI_SEMAPHORE`** (tamaño = `MAX_PARALLEL`, por defecto 200) limita
  cuántas llamadas a la IA están en vuelo *a la vez* — no cuántas
  investigaciones pueden existir. Es exactamente el límite que impondría
  el rate-limit real de un proveedor de IA.
- **Postgres** es la única fuente de verdad del estado. El pubsub en
  memoria (`pubsub.py`) solo existe para no tener que hacer polling desde
  cada conexión SSE abierta — un reinicio del proceso no pierde nada,
  porque cualquier cliente que reconecte vuelve a leer de Postgres.
- Cada investigación termina con `ref.task.cancel()` tras su propio
  `ask()` — sin esto, un actor cuyo trabajo ya terminó se queda vivo para
  siempre esperando en un mailbox vacío. Con investigaciones "infinitas"
  en producción, eso sí importa.

> **Nota:** este ejemplo solo usa `Node`/`Supervisor`/`actor`/
> `current_message`, ya disponibles desde `lapinbeam>=1.0.2` en PyPI — a
> diferencia de `examples/cluster_supervision/`, no necesita ninguna
> versión sin publicar.

## Ejecutarlo

```bash
cd examples/investigation_stream
docker compose up --build          # MAX_PARALLEL=200 por defecto
# o:
MAX_PARALLEL=0 docker compose up --build   # 0 = sin límite
```

Abre <http://localhost:8000>, pulsa "Nueva investigación", y observa los
pasos llegar en vivo. Cierra la pestaña y vuelve a abrir la misma URL
(lleva el id en `?id=...`) — verás el estado actual al instante, tanto si
sigue en marcha como si ya terminó.

## Generador de carga

```bash
cd bench
uv run python load_test.py -n 1000   # dispara N investigaciones, sigue
                                      # el SSE de cada una hasta el final
```

## Resultados medidos

Cinco corridas reales contra los contenedores (`docker stats` muestreado
cada 3s durante cada corrida; Postgres arrancado limpio antes de cada
una). Cada investigación individual, sin contención, tarda entre ~6 y
~12s (4 pasos × `uniform(1,3)`s cada uno).

| N | `MAX_PARALLEL` | tiempo total | completadas | CPU app (pico) | RAM app (antes → durante) |
| --- | --- | --- | --- | --- | --- |
| 1 | 200 | 9.6s | 1/1 | 0.1% | 58.7 → 63.5 MiB |
| 10 | 200 | 9.2s | 10/10 | 0.8% | 63.5 → 63.8 MiB |
| 200 | 200 | 11.5s | 199/200 | 13.7% | 63.5 → 71.4 MiB |
| 1000 | 200 (con límite) | **43.1s** | 994/1000 | 65.3%* | 70.9 → 127.9 MiB |
| 1000 | 0 (sin límite) | **12.9s** | 1000/1000 | 97.6%* | 58.8 → 116.7 MiB |

\* Cifra de `docker stats` muestreado cada 3s — demasiado grueso para ver
la forma real del pico. Repetido con muestreo de `/proc/<pid>/stat` cada
~0.2s (ver "¿Son normales los picos de CPU?" más abajo): en realidad son
**dos** ráfagas cortas de ~1-1.5s cada una (una al crear las 1000
investigaciones, otra cuando la mayoría termina casi a la vez), no un
único pico ni una meseta sostenida.

### Lecturas

- **1, 10 y 200 tardan prácticamente lo mismo** (~9-12s) que una sola
  investigación suelta — es la prueba de que sí hay paralelismo real, no
  solo concurrencia aparente: cada investigación tiene su propio actor,
  su propio mailbox y su propia tarea de asyncio, así que sus `sleep(1-3s)`
  se solapan de verdad.
- **1000 con el límite de 200 tarda ~4.5x más** (43.1s vs ~12s) — exactamente
  lo esperable: con 1000 investigaciones × 4 pasos = 4000 "llamadas a la
  IA" repartidas en tandas de 200 en vuelo a la vez, el tiempo total crece
  con el número de tandas, no con el número de investigaciones. Esto es
  el semáforo haciendo su trabajo, no lapinbeam quedándose corto.
- **1000 sin límite vuelve a tardar ~13s** — casi igual que 1 sola. Con
  la única "carga" real siendo `asyncio.sleep()` (I/O simulado, cero CPU
  real), asyncio intercala miles de esperas en el mismo hilo sin coste
  añadido relevante. Aquí es donde se ve que **el cuello de botella nunca
  fue el proceso Python ni lapinbeam** — fue el límite que le pusimos a
  propósito.
- **RAM crece de forma acotada y modesta**: de ~60-70 MiB en reposo a
  ~115-130 MiB con 1000 investigaciones concurrentes vivas a la vez — del
  orden de 50-70 KiB por investigación (actor + mailbox + fila en la
  pubsub en memoria), nada que preocupe hasta órdenes de magnitud mucho
  mayores.
- **Sin picos de CPU sostenidos ni con 1000 en paralelo** — ver el
  desglose fino en la siguiente sección.
- **Un puñado de fallos (0.5-0.6%) en las corridas de 200 y 1000-limitado**:
  "servidor desconectado sin respuesta" en 1-6 conexiones de las
  cientos/miles abiertas de golpe. Coincide en el tiempo con la primera
  ráfaga de CPU (ver abajo): mientras el único hilo del event loop está
  ocupado con el coste síncrono de crear 1000 actores de golpe, no puede
  atender el accept() de nuevas conexiones TCP tan rápido, y si la cola
  de espera del sistema operativo se llena en esa ventana de ~1s, alguna
  conexión entrante se pierde — no es un fallo de lapinbeam ni de la
  lógica de la investigación (Postgres confirma 0 filas en estado `error`
  tras cada corrida). En producción con tráfico real (llegadas repartidas
  en el tiempo, no una ráfaga sincronizada de un script de carga) esto no
  debería aparecer; si aparece, `uvicorn --backlog` o varios workers
  detrás de un balanceador lo resuelven.

### ¿Son normales los picos de CPU?

Repetí la corrida de N=1000 sin límite midiendo `/proc/<pid>/stat` del
proceso de uvicorn cada ~0.2s (mucho más fino que los `docker stats` de
la tabla de arriba, que solo muestrean cada 3s y por eso no distinguían
esto). La forma real es esta:

```
t=0.0-0.6s   ~0%       (conexiones llegando, aún no hay trabajo real)
t=0.6-2.0s   86-102%   ráfaga #1: crear 1000 actores (clase Python nueva
                       por investigación, registro en el core Rust vía
                       PyO3, 1000 INSERT a Postgres repartidos en un pool
                       de 20 conexiones)
t=2.0-7.3s    9-14%    en reposo: 1000 actores solo esperando en
                       asyncio.sleep(), que no cuesta CPU real
t=7.5-8.9s   70-104%   ráfaga #2: la mayoría de las 1000 terminan casi a
                       la vez (arrancaron juntas y tienen duraciones
                       parecidas) — UPDATE final a Postgres + cierre del
                       actor + cierre de 1000 streams SSE, todo junto
t=9-13s      36→0%     cola de las que tardaron más en terminar, cada vez
                       menos solapadas
```

Dos ráfagas cortas (~1-1.5s cada una), no una meseta — y **importa cómo
mide `docker stats` el CPU**: `100%` significa "un core entero ocupado",
no "toda la máquina" (esta prueba corrió en una máquina de 12 cores, así
que ni el pico más alto llega al 10% de la capacidad total).

Es normal, y es normal **precisamente por lo sintético del test**: las
1000 investigaciones se crean todas en el mismo instante y tienen
duraciones parecidas (mismo rango `uniform(1,3)` × 4 pasos), así que
tienden a *terminar* todas casi a la vez también — de ahí las dos
ráfagas, una en la creación y otra en el cierre. Tráfico real, con
investigaciones llegando repartidas a lo largo del tiempo en vez de en
una ráfaga sincronizada, no produciría este patrón de "doble joroba" —
se aplanaría en un uso de CPU mucho más constante y bajo. Lo que sí vale
la pena quedarse: en ningún momento hay una meseta sostenida de CPU alta,
lo cual habría sido la señal real de un problema (indicaría que el
*trabajo en sí*, no solo su arranque/cierre, fuera costoso).

## Lo que este ejemplo no resuelve (y por qué es aceptable aquí)

- **`Supervisor._children` acumula un registro pequeño por cada
  investigación terminada** (el `ret.task.cancel()` libera el mailbox y
  la tarea, pero no saca el registro de la lista interna del
  `Supervisor`) — unos pocos cientos de bytes por investigación, no un
  problema a la escala probada aquí, pero si el servidor va a vivir
  semanas procesando investigaciones sin parar, valdría la pena reciclar
  el `Supervisor` periódicamente.
- **Un único proceso, un único `Node`.** Como la carga es I/O-bound (solo
  espera de red simulada), esto basta para las escalas probadas. Si el
  trabajo real de "consultar a la IA" tuviera cómputo pesado de verdad
  (no solo esperar una respuesta HTTP), haría falta repartir entre varios
  procesos/`Node` — ver
  [Patrones inspirados en OTP](../../docs/otp-patterns.md) para cómo
  lapinbeam soporta eso sin cambiar el modelo de actor por investigación.
