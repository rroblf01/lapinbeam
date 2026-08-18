# Ejemplo: árboles de supervisión, links y grupos entre nodos reales

Este ejemplo levanta **tres contenedores reales** (`hub`, `worker1`,
`worker2`) para demostrar, todo a la vez y sobre conexiones TCP genuinas
entre procesos separados, las tres primitivas nuevas inspiradas en
OTP/Erlang que no tenían todavía una verificación multi-contenedor:

- **Árboles de supervisión anidados** (`Supervisor.spawn_supervisor`): el
  `hub` supervisa un `Supervisor` anidado ("watch_tree"), que a su vez
  supervisa el actor `Watcher` — un `Supervisor` supervisando a otro
  `Supervisor`, no solo una lista plana de actores.
- **Links bidireccionales entre nodos** (`lapinbeam.links`): `Watcher` se
  enlaza (`link()`) al actor `task_worker` remoto de cada worker. Cuando un
  worker falla para siempre, `Watcher` (que llama a `trap_exit()`) recibe
  un mensaje `Exit` con el motivo real — sin necesidad de que el hub esté
  sondeando nada.
- **Grupos de proceso a nivel de clúster** (`lapinbeam.groups`): cada
  `task_worker` se une al grupo `"workers"`; el hub llama a `members()`
  cada pocos segundos y ve la lista converger y reducirse en tiempo real a
  medida que los workers fallan.

Ninguna de las tres primitivas añade un `MessageKind` nuevo al protocolo de
red — las tres viajan como frames `Data` corrientes dirigidos a un actor
local reservado y bien conocido (el mismo truco que ya usa
`lapinbeam.discovery`). Este ejemplo es la prueba de que eso funciona de
verdad entre procesos separados, no solo en pytest sobre localhost.

> **Nota:** `lapinbeam.links`, `lapinbeam.groups` y
> `Supervisor.spawn_supervisor` son nuevos en el paquete y todavía no están
> publicados en PyPI en el momento de escribir esto — hace falta
> `lapinbeam>=1.1.0` (ver `CHANGELOG.md`). Hasta entonces, `docker compose
> up --build` construye la imagen sin problema pero el proceso fallará con
> `ImportError` porque la versión instalada desde PyPI todavía no trae esos
> módulos.

## Cómo está montado

```
worker1, worker2                          hub
─────────────────                         ───
Node + Supervisor(max_restarts=0)         Node + Supervisor
  └─ task_worker (@actor)                   └─ spawn_supervisor("watch_tree")
       - join_group(node, "workers")             └─ Watcher (@actor)
       - procesa tareas 1, 2, 3...                    - trap_exit(True)
       - a la N-ésima, lanza una excepción            - link() a cada
         (fallo permanente: sin budget de               task_worker remoto
          reintentos, no hay reinicio en sitio)        - @on(Exit): imprime
                                                          motivo real
                                                       - members(node,
                                                          "workers") cada 3s
```

`worker1` falla a la 4ª tarea, `worker2` a la 7ª — a propósito, para que se
vea a cada uno fallar por separado, no a la vez. `max_restarts=0` es
deliberado: el primer fallo ya es el definitivo, así que no hay un
reinicio en sitio intermedio que borre el link que el hub registró antes
de que ocurra (los links son "pid-scoped": un reinicio en sitio limpia el
lado del propio actor reiniciado — ver el docstring de
[`lapinbeam/links.py`](../../lapinbeam/links.py) — así que este ejemplo se
queda deliberadamente en el caso simple y sin ambigüedad;
`tests-python/test_links.py` cubre directamente el caso "el reinicio en
sitio no se propaga").

## Ejecutarlo

```bash
cd examples/cluster_supervision
docker compose up --build
```

`hub` espera a que `worker1`/`worker2` estén sanos antes de arrancar y
conectarse a ambos; cada worker espera unos segundos tras arrancar para
darle tiempo al hub a conectarse y enlazarse antes de empezar a procesar
tareas. En los logs del hub verás algo como:

```
hub  | conectado a 2 workers: ['worker1@worker1:9101', 'worker2@worker2:9102']
hub  | enlazado (link) a task_worker en worker1@worker1:9101
hub  | enlazado (link) a task_worker en worker2@worker2:9102
hub  | miembros actuales de 'workers': []
hub  | miembros actuales de 'workers': ['worker1@worker1:9101/task_worker', 'worker2@worker2:9102/task_worker']
hub  | EXIT recibido: 'worker1@worker1:9101/task_worker' salió con motivo 'RuntimeError: fallo permanente tras 4 tareas'
hub  | miembros actuales de 'workers': ['worker2@worker2:9102/task_worker']
hub  | EXIT recibido: 'worker2@worker2:9102/task_worker' salió con motivo 'RuntimeError: fallo permanente tras 7 tareas'
hub  | miembros actuales de 'workers': []
```

Nótese lo que pasa **sin que el hub tenga que sondear nada**: el motivo
real de cada fallo (`RuntimeError: fallo permanente tras N tareas`) llega
como un mensaje `Exit` corriente gracias al link — no un evento genérico
de desconexión — y la lista de `members()` se actualiza sola, en ambas
direcciones, cuando cada worker entra y sale del grupo. Los tres
contenedores siguen vivos después: el fallo de `task_worker` es real y
definitivo (agotó su presupuesto de reintentos, que aquí es cero), pero no
se propaga al proceso entero — solo al actor, y desde ahí, a quien esté
enlazado.

## Qué mirar en el código

- [`worker/main.py`](worker/main.py) — un `Supervisor(max_restarts=0)`
  deliberadamente sin margen de reintentos, y una única unión al grupo
  hecha en el primer mensaje de cada generación del actor (no en
  `__init__`, que no puede hacer `await`, y no una sola vez desde fuera,
  porque la pertenencia al grupo es "pid-scoped": no sobrevive a un
  reinicio, y un `task_worker` que sí tuviera presupuesto de reintentos
  tendría que volver a unirse en cada generación).
- [`hub/main.py`](hub/main.py) — `spawn_supervisor` para el árbol anidado,
  `trap_exit(True)` + `@on(Exit)` en vez del comportamiento por defecto
  (que mataría a `Watcher` también), y `members()` sondeado con un simple
  `while True` + `sleep` porque no hay ninguna otra forma de "suscribirse"
  a cambios de un grupo — es una vista de solo lectura, no un stream de
  eventos.

## Lo que este ejemplo no prueba (a propósito)

- **No prueba el caso de reinicio-en-sitio-no-propaga.** Con
  `max_restarts=0`, cada worker solo tiene una generación. El caso donde
  un actor se reinicia varias veces dentro de su presupuesto sin que el
  link se dispare está cubierto por
  [`tests-python/test_links.py`](../../tests-python/test_links.py), no
  aquí — mezclar ambos casos en un solo demo de contenedores habría hecho
  el resultado mucho más difícil de leer.
- **No prueba `unlink()`/`leave_group()` explícitos**, ni la convergencia
  de un cuarto nodo que se une al clúster tarde (cubierta por
  `tests-python/test_groups.py`). El objetivo aquí es demostrar que las
  tres primitivas funcionan de verdad cruzando una red real entre
  contenedores — no repetir la cobertura exhaustiva que ya tienen los
  tests de pytest.
