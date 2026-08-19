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

Otros dos ficheros compose en la raíz del repositorio ejercitan la misma
demo bajo condiciones distintas, cada uno con su propio job de CI:

- **`docker-compose.secure.yml`** — los mismos dos contenedores, pero con
  una variable de entorno `CLUSTER_SECRET` igual en ambos lados, conectada
  hasta `Node(..., cluster_secret=...)` (ver
  [Seguridad](index.es.md#seguridad)). Demuestra que el handshake funciona
  entre dos procesos de verdad separados, no solo dentro de uno.
- **`docker-compose.restart.yml`** — una variante de más duración
  (`examples/e2e_restart_node_a.py` / `e2e_restart_node_b.py`) que envía
  más despacio, dando a la CI margen para reiniciar el contenedor de
  node_b a mitad de flujo y confirmar que la reconexión automática de
  node_a de verdad retoma la entrega después, en vez de solo disparar un
  evento `"peer_disconnected"` y quedarse callada.

```bash
docker compose -f docker-compose.secure.yml up --build
docker compose -f docker-compose.restart.yml up --build
```

## Hosts reales y separados

Ver [Primeros pasos](getting-started.md#dos-nodos-hablando-entre-si) para un
diagrama de qué significan exactamente "servidor A" y "servidor B" aquí —
cada uno es su propio proceso del sistema operativo, y esta sección solo
cambia sus direcciones de loopback a máquinas reales. Nada en lapinbeam es
específico de loopback — `NodeId` es simplemente
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

## Una tubería multinodo detrás de una API HTTP

`examples/police_investigation/` es un ejemplo más grande y realista que la
demo de dos nodos de arriba: el envío de un formulario con FastAPI recorre
**tres** contenedores/nodos separados (`api` → `investigator` → `archive`),
cada uno con un salto de red real al siguiente, con el progreso reportado de
vuelta al nodo de origen a medida que el caso pasa por cuatro actores
encadenados localmente. Su README documenta una medición completa de CPU y
RAM (en reposo y bajo carga) usando solo `docker`, `docker compose` y `uv`.

```bash
cd examples/police_investigation
docker compose up --build
```

## Descubrimiento de nodos vía nodo semilla

Todos los ejemplos de arriba configuran cada nodo con la dirección exacta de
cada peer con el que necesita hablar — vale para dos o tres nodos, pero son
hasta N·(N-1)/2 direcciones a configurar a mano para una malla de N.
`lapinbeam.discovery` (ver la lista de [Características](index.es.md)) lo
convierte en "cada nodo necesita una única dirección semilla compartida":
conéctate a una semilla, pregúntale a quién conoce, conéctate también a
esos, recursivamente, hasta que no aparezca nadie nuevo.

```python
from lapinbeam import Node, Supervisor, register_discovery, join_via_seeds

node = Node("app@app:9001")
await node.start()
register_discovery(node, Supervisor(node=node))
encontrados: set[str] = await join_via_seeds(node, seeds=["seed@seed:9000"])
```

`examples/seed_discovery/` ejecuta esto con cuatro contenedores — una
semilla y tres nodos que solo conocen la dirección de la semilla — y
muestra en los logs cómo los cuatro convergen en una malla completa:

```bash
cd examples/seed_discovery
docker compose up --build
```

## Árboles de supervisión, links, monitors, grupos y registro de nombres entre nodos reales

`Supervisor.spawn_supervisor()` (árboles de supervisión anidados),
`lapinbeam.links` (links bidireccionales), `lapinbeam.monitors` (monitors
unidireccionales y no letales), `lapinbeam.groups` (grupos de proceso a
nivel de clúster) y `lapinbeam.registry` (registro de nombres únicos a
nivel de clúster) funcionan igual en local o entre nodos, sin ningún
cambio en el protocolo de red en ninguno de los cinco casos — el tráfico
entre nodos de los cinco viaja como frames `Data` corrientes dirigidos a
un actor local reservado, el mismo truco que ya usa `lapinbeam.discovery`:

```python
from lapinbeam import (
    ActorRef, Down, Exit, Node, RemoteRef, Supervisor, actor, on,
    link, trap_exit, register_links,
    monitor, register_monitors,
    join_group, members, register_groups,
    register_name, whereis_name, register_registry,
)

node = Node("app@app:9100")
await node.start()
sup = Supervisor(node=node)
register_links(node, sup)
register_monitors(node, sup)
register_groups(node, sup)
register_registry(node, sup)


@actor(name="watcher")
class Watcher:
    @on(Exit)
    async def on_exit(self, msg: Exit):
        print("el actor enlazado salió:", msg.actor, msg.reason)

    @on(Down)
    async def on_down(self, msg: Down):
        print("el actor monitorizado salió:", msg.actor, msg.reason)


ref: ActorRef = sup.spawn(Watcher)
await node.connect_peer("worker@worker:9101")
other: RemoteRef = node.get_remote_actor("worker@worker:9101", "task_worker")
# trap_exit() debe llamarse desde dentro del actor — p.ej. en su primer
# handler.
await link(other)                          # link entre nodos (mata/atrapa al salir), sin cambios en el núcleo
ref_monitor: str = await monitor(other)    # monitor entre nodos (nunca mata, nunca lo matan)
await join_group(node, "watchers", ref=ref)   # pertenencia a grupo, todo el clúster
encontrados: list[ActorRef | RemoteRef] = members(node, "watchers")
await register_name(node, "leader", ref=ref)  # nombre único, todo el clúster
propietario: ActorRef | RemoteRef | None = whereis_name(node, "leader")
```

`examples/cluster_supervision/` ejecuta las cinco cosas a la vez con tres
contenedores reales — un `hub` con un árbol de supervisión anidado que se
enlaza **y** monitoriza a dos workers (así la misma caída entrega tanto un
`Exit` como un `Down`) y vigila un grupo `"workers"` a nivel de clúster y
el nombre registrado `"task_worker_primary"` mientras cada worker falla
para siempre:

```bash
cd examples/cluster_supervision
docker compose up --build
```

## Recuperarse de un fallo

`Supervisor` reinicia un actor cuyo `receive` (o handler `@on`) lance una
excepción, usando la estrategia `one_for_one`: solo se reinicia el actor que
falló, con backoff exponencial, hasta `max_restarts` dentro de
`restart_window` segundos antes de rendirse y relanzar la excepción:

```python
import asyncio
from lapinbeam import ActorRef, Node, Supervisor, actor

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
        ref: ActorRef = sup.spawn(Flaky)
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
