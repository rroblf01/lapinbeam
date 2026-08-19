# Patrones inspirados en OTP

Más allá del trío base `Node`/`@actor`/`Supervisor`, lapinbeam toma
prestados cinco patrones más de Erlang/OTP: árboles de supervisión
anidados, links bidireccionales, monitors unidireccionales, grupos de
proceso a nivel de clúster, y registro de nombres a nivel de clúster. Los
cinco son adiciones puramente en Python sobre la misma API pública —
**cero cambios en Rust o en el protocolo de red** para ninguno de ellos.
El tráfico entre nodos de links, monitors, grupos y el registro viaja
como frames `Data` corrientes dirigidos a un actor local reservado y bien
conocido (el mismo truco que ya usa `lapinbeam.discovery` para su propio
actor de descubrimiento) — un peer que no se ha apuntado a una
característica dada simplemente responde con el `actor_not_found` de
siempre, en vez de romper la conexión.

## Árboles de supervisión anidados y estrategias de reinicio

`Supervisor(strategy=...)` acepta tres estrategias, aplicadas cada vez que
falla cualquier hijo (un actor *o* un `Supervisor` anidado):

- **`one_for_one`** (la de por defecto): reinicia solo el hijo que falló.
  Si agota su presupuesto de reintentos, solo *ese* hijo se rinde — los
  hermanos no relacionados nunca se ven afectados. Esto es lo que hace
  seguro alojar muchos actores independientes y sin relación bajo un
  mismo `Supervisor` a lo largo de su vida (un patrón de pool de workers
  en el sentido amplio de "muchos actores, un Supervisor" — para un pool
  fijo de workers *idénticos* compartiendo una única cola de trabajo, ver
  `Supervisor.spawn_pool()` en
  [Primeros pasos](getting-started.es.md#concurrencia-un-actor-procesa-un-mensaje-cada-vez)
  en su lugar).
- **`one_for_all`**: un fallo reinicia *todos* los hijos que gestiona ese
  `Supervisor`, no solo el que falló.
- **`rest_for_one`**: un fallo reinicia el hijo que falló y todos los
  hijos creados *después* de él (importa el orden de creación).

Para `one_for_all`/`rest_for_one`, agotar el presupuesto de reintentos
tira abajo todo el subárbol y este `Supervisor` se considera a sí mismo
rendido.

`Supervisor.spawn_supervisor(name, build, *, strategy=, max_restarts=,
restart_window=)` crea un `Supervisor` **anidado** como hijo de otro — un
árbol de supervisión de verdad, no solo un pool plano de actores. `build`
recibe el `Supervisor` anidado recién creado y lo puebla (crea actores, o
anida aún más); se vuelve a ejecutar cada vez que este subárbol se
reinicia, ya que un `Supervisor` ya usado no se puede "reiniciar en
sitio" de la forma en que se reutiliza el mailbox de un actor.

```python
from lapinbeam import ActorRef, Node, Supervisor, SupervisorRef, actor


@actor(name="worker_a")
class WorkerA:
    async def receive(self, msg):
        if msg.get("crash"):
            raise RuntimeError("boom")


@actor(name="worker_b")
class WorkerB:
    async def receive(self, msg):
        pass


def build_pool(pool_sup):
    pool_sup.spawn(WorkerA)
    pool_sup.spawn(WorkerB)


async def main():
    async with Node("app@127.0.0.1:0") as node:
        sup = Supervisor(node=node)
        pool: SupervisorRef = sup.spawn_supervisor("pool", build_pool, strategy="one_for_all")
        await ActorRef(node, "worker_a").send({"crash": True})
        # one_for_all: worker_b también recibe una instancia nueva, aunque
        # solo worker_a haya lanzado la excepción.
```

`spawn_supervisor()` devuelve un `SupervisorRef` — `await ref.task`
bloquea hasta que todo ese subárbol se rinda, relanzando la excepción
*original* (no un envoltorio genérico), de forma recursiva a través de
niveles anidados:

```python
try:
    await pool.task
except RuntimeError as exc:
    print("todo el pool se rindió:", exc)
```

## Links bidireccionales (`lapinbeam.links`)

`link(other)` enlaza al actor que se está ejecutando ahora mismo con
`other` (un `ActorRef` o un `RemoteRef`): si cualquiera de los dos lados
sale *para siempre* — su `Supervisor` se rinde, retorna limpiamente, o se
apaga explícitamente, **no** en un reinicio en sitio ordinario dentro de
presupuesto — el otro también muere, a través del camino normal de
crash/reinicio de su propio `Supervisor`. Un actor que llama a
`trap_exit()` recibe en su lugar la señal como un mensaje `Exit`
corriente:

```python
from lapinbeam import Exit, Node, Supervisor, actor, on, link, trap_exit, register_links


@actor(name="watcher")
class Watcher:
    def __init__(self, node_ref):
        self.node = node_ref

    @on(Exit)
    async def on_exit(self, msg: Exit):
        print("el actor enlazado salió:", msg.actor, msg.reason)

    @on(default=True)
    async def on_setup(self, msg):
        trap_exit(True)
        other = self.node.get_remote_actor("worker@worker:9101", "task_worker")
        await link(other)


async def main():
    node = Node("app@app:9100")
    await node.start()
    register_links(node, Supervisor(node=node))  # solo hace falta para links entre nodos
```

`unlink(other)` quita un link. Los links son "pid-scoped": no sobreviven
a su propio reinicio en sitio, así que un actor reiniciado que siga
queriendo estar enlazado debe llamar a `link()` de nuevo — normalmente
desde su primer handler de mensaje, ya que `__init__` no puede hacer
`await`.

## Monitors unidireccionales y no letales (`lapinbeam.monitors`)

`monitor(other)` es la contraparte no letal de `link()`: quien vigila
recibe un mensaje `Down` cuando el actor/peer monitorizado sale para
siempre, pero **no pasa nada al otro lado** — ni se mata en ninguna
dirección, ni hace falta `trap_exit()`. Es la herramienta para "avísame
cuando X desaparezca" sin el riesgo (ni la obligación) que conlleva
`link()`:

```python
from lapinbeam import Down, actor, on, monitor, register_monitors


@actor(name="watcher")
class Watcher:
    def __init__(self, node_ref):
        self.node = node_ref

    @on(Down)
    async def on_down(self, msg: Down):
        print("el actor monitorizado salió:", msg.actor, msg.reason)

    @on(default=True)
    async def on_setup(self, msg):
        other = self.node.get_remote_actor("worker@worker:9101", "task_worker")
        ref: str = await monitor(other)  # guarda el ref si vas a hacer demonitor() luego
```

`demonitor(ref)` detiene un monitor. Mismo pid-scoping que los links, y la
misma llamada de configuración `register_monitors(node, sup)` para el
caso entre nodos.

## Grupos de proceso a nivel de clúster (`lapinbeam.groups`)

`join_group(node, group)` añade al actor en ejecución a un grupo con
nombre, visible desde cualquier nodo conectado — no solo el local — vía
`members(node, group)`, que devuelve una mezcla de `ActorRef` (miembros
locales) y `RemoteRef` (remotos):

```python
from lapinbeam import ActorRef, RemoteRef, actor, join_group, members, register_groups


@actor(name="worker")
class Worker:
    def __init__(self, node_ref):
        self.node = node_ref

    async def receive(self, msg):
        await join_group(self.node, "workers")


# Desde cualquier sitio con una referencia a Node:
encontrados: list[ActorRef | RemoteRef] = members(node, "workers")
```

`leave_group(node, group)` quita un miembro. La pertenencia es
"pid-scoped": un actor reiniciado se cae de todos los grupos en los que
estaba y debe volver a unirse explícitamente — mismo razonamiento que los
links, y la misma llamada de configuración `register_groups(node, sup)`
para visibilidad entre nodos. La convergencia para un peer recién
conectado es un intercambio de snapshot puntual, no gossip continuo: dos
peers que se unen/salen en una carrera muy ajustada, justo cuando se
conecta un tercer nodo, podrían converger un instante tarde en vez de al
momento.

## Registro de nombres a nivel de clúster (`lapinbeam.registry`)

`register_name(node, name)` reclama `name` como el **único** propietario
en todo el clúster conectado — el `:global` de Erlang. A diferencia de un
grupo (muchos miembros), un nombre tiene exactamente un propietario:
`register_name()` lanza `ValueError` si ya lo tiene reclamado un actor
distinto, el mismo contrato de "sin colisión silenciosa" que
`Supervisor.spawn()` ya exige para los nombres de actor locales:

```python
from lapinbeam import ActorRef, RemoteRef, actor, register_name, whereis_name, register_registry


@actor(name="worker")
class Worker:
    def __init__(self, node_ref):
        self.node = node_ref

    async def receive(self, msg):
        await register_name(self.node, "leader")


propietario: ActorRef | RemoteRef | None = whereis_name(node, "leader")
```

`unregister_name(node, name)` libera un nombre; "pid-scoped" igual que la
pertenencia a un grupo. La convergencia es delta-más-snapshot, el mismo
compromiso que los grupos — con una arruga añadida: dos nodos que cada
uno compite por reclamar el mismo nombre *antes* de conocerse tendrán
éxito localmente cada uno por su lado, y el desacuerdo aparece más tarde
como `on_event(kind="registry_conflict")` en vez de resolverse en
silencio. Esto deliberadamente **no** es consenso distribuido de verdad —
ver el docstring del módulo en `lapinbeam/registry.py` para las garantías
exactas.

## Verlo todo funcionando en contenedores reales

`examples/cluster_supervision/` ejecuta un árbol de supervisión anidado,
links **y** monitors entre nodos (el mismo fallo entrega tanto un `Exit`
como un `Down`), un grupo a nivel de clúster, y un nombre registrado, en
**tres contenedores reales** — la prueba de que todo esto funciona de
verdad sobre TCP genuino entre procesos separados, no solo en pytest
sobre localhost:

```bash
cd examples/cluster_supervision
docker compose up --build
```
