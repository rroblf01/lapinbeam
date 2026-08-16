# Primeros pasos

## Instalación

```bash
pip install lapinbeam
```

Para desarrollar contra el propio código fuente en su lugar:

```bash
uv sync                       # crea .venv, compila la extensión Rust, instala deps
uv run maturin develop        # recompila la extensión tras tocar código Rust
uv run pytest                 # suite de tests en Python
cargo test                    # suite de tests en Rust
```

No se instala nada a nivel de sistema: todo vive en `.venv`.

## Conceptos clave

| Concepto | Qué es |
| --- | --- |
| `Node` | El extremo local del clúster: `nombre@host:puerto`. Posee el runtime de Tokio en segundo plano, el listener y las conexiones a peers. |
| `@actor` | Marca una clase como actor. `Supervisor.spawn` lee estos metadatos; no es un envoltorio de runtime. |
| `Supervisor` | Crea actores y los reinicia ante excepciones no controladas (estrategia `one_for_one`, con reinicios limitados y backoff). |
| `ActorRef` / `RemoteRef` | Un handle para enviar mensajes a un actor local o remoto. Ambos exponen el mismo `await ref.send(msg)`. |

## Un único actor

```python
import asyncio
from lapinbeam import Node, Supervisor, actor


@actor(name="echo")
class Echo:
    async def receive(self, msg):
        print("recibido:", msg)


async def main():
    node = Node("app@127.0.0.1:0")  # el puerto 0 elige uno efímero
    await node.start()

    sup = Supervisor(strategy="one_for_one", node=node)
    echo = sup.spawn(Echo)

    await echo.send({"hello": "world"})
    await asyncio.sleep(0.1)  # deja que la mailbox se vacíe antes de parar
    await node.stop()


asyncio.run(main())
```

`Node` también funciona como gestor de contexto asíncrono, la forma más
idiomática para cualquier cosa de vida más larga:

```python
async with Node("app@127.0.0.1:0") as node:
    sup = Supervisor(node=node)
    ref = sup.spawn(Echo)
    await ref.send({"hello": "world"})
    await asyncio.sleep(0.1)
# node.stop() se ejecuta automáticamente al salir, incluso si el bloque lanza una excepción.
```

## Dos nodos hablando entre sí

Esta es la forma de la demo de dos nodos en `examples/`: el nodo A envía
mensajes `TASK` a un actor `processor` en el nodo B; B responde con `ACK` al
actor que A haya indicado como `reply_to`.

```python
import asyncio
import os
from lapinbeam import Node, Supervisor, actor


@actor(name="ingestor")
class Ingestor:
    async def receive(self, msg):
        if msg.get("type") == "ACK":
            print("ack recibido para", msg["payload_id"])


@actor(name="processor")
class Processor:
    def __init__(self, node_ref, peer_id):
        self.node = node_ref
        self.peer_id = peer_id

    async def receive(self, msg):
        if msg.get("type") == "TASK":
            remote = self.node.get_remote_actor(self.peer_id, msg["reply_to"])
            await remote.send({"type": "ACK", "payload_id": msg["payload_id"]})


async def main():
    node_name = os.environ["NODE_NAME"]   # p.ej. node_a@127.0.0.1:9001
    peer = os.environ["PEER"]             # p.ej. node_b@127.0.0.1:9002

    node = Node(node_name)
    await node.start()
    sup = Supervisor(node=node)

    if "node_a" in node_name:
        sup.spawn(Ingestor)
        await node.connect_peer(peer)
        remote = node.get_remote_actor(peer, "processor")
        for i in range(100):
            await remote.send({"type": "TASK", "payload_id": i, "reply_to": "ingestor"})
            await asyncio.sleep(0.01)
    else:
        sup.spawn(Processor, node, peer)
        await node.wait_until_stopped()


asyncio.run(main())
```

Ejecútalo como dos procesos separados (ver [Ejemplos](examples.md) para
correr esto entre contenedores u hosts reales en vez de `127.0.0.1`):

```bash
# terminal 1
NODE_NAME=node_a@127.0.0.1:9001 PEER=node_b@127.0.0.1:9002 python app_node_a.py
# terminal 2
NODE_NAME=node_b@127.0.0.1:9002 PEER=node_a@127.0.0.1:9001 python app_node_b.py
```

## Observar el clúster

`connect_peer` ya espera a que el handshake se complete antes de devolver el
control, pero rara vez querrás volar a ciegas sobre el estado de la conexión
o los errores de entrega en un proceso de larga duración — suscríbete a los
eventos de sistema:

```python
def on_event(event):
    if event["kind"] == "peer_disconnected":
        print("peer perdido:", event["peer"])
    elif event["kind"] == "error":
        print("error de entrega desde", event["peer"], ":", event["detail"])

node.on_event(on_event)
```

`event["kind"]` es uno de `"peer_connected"`, `"peer_disconnected"` o
`"error"` — este último se dispara cuando un peer reporta un fallo de
entrega, p.ej. un mensaje enviado a un nombre de actor que no existe en el
nodo remoto.

Siguiente: [Mensajes tipados](typed-messages.md) para enviar tipos reales de
Python en vez de dicts, o [Benchmarks](benchmarks.md) para saber cuánto
cuesta esto en latencia y throughput.
