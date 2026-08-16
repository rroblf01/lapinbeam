# Ejemplos

Las versiones ejecutables de la demo de dos nodos de abajo viven en
`examples/` (`app_node_a.py` / `app_node_b.py`). Esta página muestra la
misma forma corriendo en distintos destinos de despliegue, más dos patrones
extra (recuperación ante fallos, tipos personalizados) que no encajan en la
página de [Primeros pasos](getting-started.md).

## Dos procesos locales

El caso más simple: ambos nodos en `127.0.0.1`, puertos distintos.

```bash
# terminal 1
NODE_NAME=node_a@127.0.0.1:9001 PEER=node_b@127.0.0.1:9002 python examples/app_node_a.py
# terminal 2
NODE_NAME=node_b@127.0.0.1:9002 PEER=node_a@127.0.0.1:9001 python examples/app_node_b.py
```

## Docker Compose (dos contenedores)

`docker-compose.yml` en la raíz del repositorio ejecuta los mismos dos
actores como dos contenedores en una red bridge, dirigiéndose entre sí por
nombre de contenedor en vez de por IP:

```yaml
services:
  node_a:
    build: .
    command: python app_node_a.py
    environment:
      - NODE_NAME=node_a@node_a:9001
      - PEER=node_b@node_b:9002
    ports: ["9001:9001"]
    networks: [lapinbeam-net]

  node_b:
    build: .
    command: python app_node_b.py
    environment:
      - NODE_NAME=node_b@node_b:9002
      - PEER=node_a@node_a:9001
    ports: ["9002:9002"]
    networks: [lapinbeam-net]

networks:
  lapinbeam-net:
    driver: bridge
```

```bash
docker compose up --build
```

Lo único que cambia respecto a dos procesos locales es la parte de host en
`NODE_NAME`/`PEER`: el DNS embebido de Docker resuelve `node_a`/`node_b` a
la IP correcta del contenedor en la red bridge. El pipeline de CI
(`.github/workflows/ci.yml`) ejecuta exactamente este compose y comprueba
que los logs de node_a muestren `Total: 100` ACKs antes de tirar todo abajo.

## Hosts reales y separados

Nada en lapinbeam es específico de loopback — `NodeId` es simplemente
`nombre@host:puerto`, y `host` puede ser cualquier dirección alcanzable
desde el otro lado. Ejecutar los dos actores en dos máquinas distintas solo
cambia las variables de entorno:

```bash
# máquina en 10.0.0.1
NODE_NAME=node_a@10.0.0.1:9001 PEER=node_b@10.0.0.2:9002 python examples/app_node_a.py
# máquina en 10.0.0.2
NODE_NAME=node_b@10.0.0.2:9002 PEER=node_a@10.0.0.1:9001 python examples/app_node_b.py
```

Dos cosas a tener en cuenta al salir de loopback:

- **Abre en el firewall el puerto de escucha** (`9001`/`9002` arriba) entre
  los hosts — `Node.start()` se vincula y acepta desde cualquier origen por
  defecto.
- **Espera que el RTT de red real domine.** Los
  [benchmarks en loopback](benchmarks.md) aíslan el overhead propio de
  lapinbeam (submilisegundo); entre hosts reales tu suelo de latencia es lo
  que dé la red entre ellos, más ese overhead encima.

## Recuperarse de un fallo

`Supervisor` reinicia un actor cuyo `receive` (o handler `@on`) lance una
excepción, usando la estrategia `one_for_one`: solo se reinicia el actor que
falló, con backoff exponencial, hasta `max_restarts` dentro de
`restart_window` segundos antes de rendirse y relanzar la excepción:

```python
import asyncio
from lapinbeam import Node, Supervisor, actor

# Deliberadamente fuera del actor — ver la nota de abajo.
attempts = {"n": 0}


@actor(name="flaky")
class Flaky:
    async def receive(self, msg):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError(f"fallo transitorio #{attempts['n']}")
        print("éxito en el intento", attempts["n"])


async def main():
    async with Node("app@127.0.0.1:0") as node:
        sup = Supervisor(strategy="one_for_one", node=node,
                          max_restarts=5, restart_window=10.0)
        ref = sup.spawn(Flaky)
        for _ in range(3):
            await ref.send({})
            # Da tiempo a que el reinicio (con backoff) termine antes del
            # siguiente envío — enviar mientras el actor está reiniciándose
            # deja momentáneamente sin mailbox donde aterrizar y lanza
            # ValueError, igual que enviar a cualquier otro nombre que
            # todavía no esté registrado.
            await asyncio.sleep(0.4)
```

!!! warning "Los reinicios crean una instancia nueva — el estado no sobrevive"
    Cada reinicio vuelve a ejecutar `actor_cls(*args, **kwargs)`, así que
    cualquier cosa guardada en `self` (como `self.attempts`) vuelve a su
    valor inicial en cada fallo — solo el registro (la mailbox y su nombre)
    sobrevive, así que quien envía nunca necesita saber que hubo un
    reinicio. Por eso `attempts` vive fuera del actor arriba: si fuera
    `self.attempts`, nunca llegaría a `3` sin importar cuántas veces envíes,
    porque cada fallo entrega el siguiente mensaje a una instancia nueva que
    empieza de cero. Si un actor necesita que su estado sobreviva a sus
    propios fallos, persístelo fuera (una base de datos, Redis, o — como
    arriba — un objeto plano que el actor captura por clausura) en vez de en
    `self`.

## Tipos personalizados (ni dataclass ni Pydantic)

[Mensajes tipados](typed-messages.md) cubre dataclasses y modelos Pydantic,
que viajan automáticamente. Cualquier otra cosa necesita un códec explícito
registrado en **ambos** extremos del clúster:

```python
from lapinbeam import register_codec

class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __repr__(self):
        return f"Point({self.x}, {self.y})"


register_codec(
    Point,
    encode=lambda p: {"x": p.x, "y": p.y},
    decode=lambda d: Point(d["x"], d["y"]),
)

# A partir de aquí, enviar un Point funciona igual que un dataclass:
# await remote.send(Point(1, 2))
```

Registra el códec una vez, al importar, en cada nodo que vaya a enviar o
recibir instancias de `Point` — la búsqueda por etiqueta al decodificar
necesita que el códec (o la propia clase, si es un dataclass/modelo
Pydantic) ya esté registrado/sea importable.
