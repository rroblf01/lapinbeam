# lapinbeam

**Framework de sistemas distribuidos en tiempo real para Python, con núcleo en Rust.**

lapinbeam trae a Python un modelo de actores inspirado en Erlang/Elixir
(BEAM): clases decoradas con `@actor`, un `Supervisor` que reinicia los
actores que fallan, y un `Node` que da referencias transparentes a actores
que corren en otras máquinas. La capa de red — un transporte TCP
multiplexado con heartbeats, framing y reconexión automática — está escrita
en Rust (Tokio) y expuesta mediante PyO3, así que toda la E/S ocurre fuera
del GIL mientras tus actores siguen siendo simples corrutinas `async def`.

!!! warning "Estado: alpha"
    El MVP es paso de mensajes bidireccional entre dos nodos sobre una
    conexión TCP multiplexada. Revisa [Limitaciones](#limitaciones) antes de
    apostar tráfico de producción a esto.

## Por qué existe lapinbeam

La mayoría de sistemas Python recurren a una cola de tareas (Celery, RQ,
Dramatiq) en cuanto necesitan ejecutar trabajo fuera del ciclo
petición/respuesta, y recurren a un broker de mensajes (RabbitMQ, Redis,
Kafka) en cuanto dos procesos necesitan hablar entre sí. Esa combinación es
excelente para **trabajos de fondo duraderos** — pero añade un servicio más
que operar, y un salto por el broker, incluso para dos procesos que solo
quieren intercambiar un mensaje y recibir un ack en menos de un milisegundo.

lapinbeam apunta a ese otro caso: procesos que quieren mensajería directa,
de baja latencia y tipada entre actores, sin broker que desplegar, y sin un
blob JSON sin tipo en medio. Consulta
[lapinbeam frente a Celery + RabbitMQ](vs-celery-rabbitmq.md) para una
comparación honesta — resuelven problemas distintos y es muy posible que
quieras los dos en el mismo sistema.

## Características

- Clases Python decoradas con `@actor` y `async def receive(msg)`, o
  despacho tipado vía `@on(Type)` / `@on(default=True)` — ver
  [Mensajes tipados](typed-messages.md).
- `Supervisor` con estrategias de reinicio (`one_for_one`).
- `Node` con referencias transparentes a actores remotos (`RemoteRef`) que
  se usan exactamente igual que las locales (`ActorRef`).
- Transporte TCP multiplexado (un socket por peer) con framing en bincode.
- Heartbeat y watchdog de conexión en el núcleo Rust; reconexión automática
  de los peers deseados con backoff.
- Eventos de sistema (`Node.on_event`) para conexión/desconexión de peers y
  errores de entrega — sin mensajes perdidos en silencio.
- Payloads que preservan el tipo: los modelos `@dataclass` y Pydantic v2
  viajan entre nodos exactamente como se enviaron, vía `lapinbeam.codec`.

## Instalación

```bash
pip install lapinbeam
```

El wheel se compila para `abi3 >= 3.11`, así que un único artefacto cubre
Python 3.11 a 3.14. No hace falta instalar ni ejecutar nada más — sin
broker, sin servicio externo.

Continúa con [Primeros pasos](getting-started.md).

## Limitaciones

- Los payloads deben ser compatibles con JSON (dict/list/str/int/float/bool/
  None) — o un modelo `@dataclass`/Pydantic, codificado vía
  `lapinbeam.codec`. Los enteros están limitados a `i64`/`u64`.
- La preservación de tipo solo ocurre en envíos **remotos**; los envíos
  locales pasan el objeto por referencia (sin copia).
- Los nombres de actor deben ser únicos por nodo.
- No hay persistencia de mensajes ni entrega "at-least-once": un mensaje en
  vuelo durante una partición de red se pierde, no se reintenta. Consulta
  [lapinbeam frente a Celery + RabbitMQ](vs-celery-rabbitmq.md) para lo que
  esto implica en la práctica.
- Los payloads mayores de 16 MiB se rechazan en el emisor.

## Licencia

MIT
