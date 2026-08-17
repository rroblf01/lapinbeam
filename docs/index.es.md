# lapinbeam

**Framework de sistemas distribuidos en tiempo real para Python, con núcleo en Rust.**

lapinbeam trae a Python un modelo de actores inspirado en Erlang/Elixir
(BEAM): clases decoradas con `@actor`, un `Supervisor` que reinicia los
actores que fallan, y un `Node` que da referencias transparentes a actores
que corren en otras máquinas. La capa de red — un transporte TCP
multiplexado con heartbeats, framing y reconexión automática — está escrita
en Rust (Tokio) y expuesta mediante PyO3, así que toda la E/S ocurre fuera
del GIL mientras tus actores siguen siendo simples corrutinas `async def`.

!!! info "Estado: 1.0"
    La API pública (`Node`, `Supervisor`, `actor`/`on`, `ActorRef`/
    `RemoteRef`, `codec`) es estable — un cambio incompatible ahora requiere
    subir la versión mayor. Todavía no se ha probado a escala de producción,
    así que revisa [Limitaciones](#limitaciones) antes de apostar tráfico de
    producción a esto.

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

## Seguridad

El transporte de lapinbeam **no cifra** nada: todo el tráfico viaja como TCP
plano, legible por cualquiera que pueda observar la red. Sí tiene una
**comprobación de handshake con secreto compartido**, ligera y opcional:

```python
node = Node("app@0.0.0.0:9001", cluster_secret="un-secreto-que-solo-conoce-tu-cluster")
```

Cada nodo del clúster debe arrancar con el *mismo* `cluster_secret`. Al
conectar, quien marca demuestra que conoce el secreto (un nonce aleatorio
más su `HMAC-SHA256`); si el secreto de quien acepta no produce la misma
prueba, el handshake se descarta antes de que la conexión llegue a
registrarse como peer — un proceso cualquiera que alcance el puerto ya no
puede simplemente afirmar ser un peer y que se le crea. Sin
`cluster_secret` (el valor por defecto), el comportamiento es el de
siempre: se acepta cualquier handshake.

Es deliberadamente el mismo modelo de confianza que ha usado la
distribución de Erlang durante décadas (una cookie de clúster compartida)
— y tiene los mismos límites, dichos con claridad:

- **Unidireccional.** Quien marca se demuestra a quien acepta; quien acepta
  no se demuestra de vuelta. Un proceso impostado en la dirección de un
  peer antes de que el nodo real arranque no queda cubierto por esto.
- **Sin cifrado, sin protección contra repetición.** Un atacante con
  posición en la red que ya pueda observar el tráfico puede capturar un
  handshake válido y reproducirlo más tarde. Esto cierra "cualquier
  proceso puede unirse", no "un atacante pasivo en el cable nunca puede
  entrar".

Está bien para un clúster de procesos dentro de un límite de red en el que
ya confías — una VPC privada, una única red de Docker Compose/Kubernetes,
una LAN —, y `cluster_secret` sube el listón dentro de ese límite. **No**
está bien exponer el puerto de escucha de un `Node` directamente a internet
abierto, con secreto o sin él. Si necesitas eso, pon lapinbeam detrás de
algo que realmente autentique y cifre el enlace de extremo a extremo — una
VPN, un túnel WireGuard, un proxy que termine mTLS — en vez de confiar en
el propio transporte.

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
  `__lb_type__` es una clave de payload reservada, usada por el codec que
  preserva el tipo.
- La preservación de tipo solo ocurre en envíos **remotos**; los envíos
  locales pasan el objeto por referencia (sin copia). Un campo Pydantic
  tipado de forma laxa (p.ej. `Any`) no reconstruye un valor `@dataclass`
  anidado al decodificar — vuelve como un dict plano; un campo bien tipado
  (p.ej. `inner: Inner`) sí hace el roundtrip correctamente gracias a la
  propia validación de Pydantic.
- Los nombres de actor deben ser únicos por nodo. El dial simultáneo (ambos
  nodos conectándose entre sí a la vez) se resuelve de forma determinista —
  sobrevive exactamente una conexión, no dos.
- No hay persistencia de mensajes ni entrega "at-least-once": un mensaje en
  vuelo durante una partición de red se pierde, no se reintenta. Consulta
  [lapinbeam frente a Celery + RabbitMQ](vs-celery-rabbitmq.md) para lo que
  esto implica en la práctica.
- Los payloads mayores de 16 MiB se rechazan en el emisor.
- Los mailboxes de los actores no tienen límite por defecto: un actor que no
  da abasto con su ritmo de entrada hace que su mailbox crezca sin límite
  en vez de aplicar backpressure. Pasa `Node(..., mailbox_capacity=N)` para
  acotarlo — un mailbox lleno descarta los mensajes nuevos en su lugar,
  disparando `on_event(kind="mailbox_full")` (y, si el envío descartado era
  remoto, un evento `"error"` en el emisor).

## Licencia

MIT
