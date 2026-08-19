# Ejemplo: árboles de supervisión, links, monitors, grupos y registro entre nodos reales

Este ejemplo levanta **tres contenedores reales** (`hub`, `worker1`,
`worker2`) para demostrar, todo a la vez y sobre conexiones TCP genuinas
entre procesos separados, las cinco primitivas nuevas inspiradas en
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
- **Monitors unidireccionales entre nodos** (`lapinbeam.monitors`): además
  del link, `Watcher` también `monitor()`ea al mismo `task_worker` — la
  misma caída dispara **ambas** señales (`Exit` vía link, `Down` vía
  monitor), para que se vea en los logs la diferencia: un monitor nunca
  mata ni es matado por lo que vigila.
- **Grupos de proceso a nivel de clúster** (`lapinbeam.groups`): cada
  `task_worker` se une al grupo `"workers"`; el hub llama a `members()`
  cada pocos segundos y ve la lista converger y reducirse en tiempo real a
  medida que los workers fallan.
- **Registro de nombres a nivel de clúster** (`lapinbeam.registry`):
  `worker1` (arrancado con `PRIMARY=1`) reclama el nombre único
  `"task_worker_primary"`; el hub llama a `whereis_name()` cada pocos
  segundos y ve cómo aparece y luego desaparece cuando `worker1` falla
  para siempre — el nombre es "pid-scoped", igual que la pertenencia a un
  grupo.

Ninguna de las cinco primitivas añade un `MessageKind` nuevo al protocolo
de red — las cinco viajan como frames `Data` corrientes dirigidos a un
actor local reservado y bien conocido (el mismo truco que ya usa
`lapinbeam.discovery`). Este ejemplo es la prueba de que eso funciona de
verdad entre procesos separados, no solo en pytest sobre localhost.

> **Nota:** `lapinbeam.links`, `lapinbeam.monitors`, `lapinbeam.groups`,
> `lapinbeam.registry` y `Supervisor.spawn_supervisor` son nuevos en el
> paquete y todavía no están publicados en PyPI en el momento de escribir
> esto — hace falta `lapinbeam>=1.2.0` (ver `CHANGELOG.md`). Hasta
> entonces, `docker compose up --build` construye la imagen sin problema
> pero el proceso fallará con `ImportError` porque la versión instalada
> desde PyPI todavía no trae esos módulos.

## Cómo está montado

```
worker1 (PRIMARY=1), worker2               hub
─────────────────────────────              ───
Node + Supervisor(max_restarts=0)         Node + Supervisor
  └─ task_worker (@actor)                   └─ spawn_supervisor("watch_tree")
       - join_group(node, "workers")             └─ Watcher (@actor)
       - register_name(node,                          - trap_exit(True)
         "task_worker_primary")                        - link() + monitor()
         (solo en worker1)                               a cada task_worker
       - procesa tareas 1, 2, 3...                       remoto
       - a la N-ésima, lanza una excepción            - @on(Exit): motivo
         (fallo permanente: sin budget de                real (link)
          reintentos, no hay reinicio en sitio)        - @on(Down): motivo
                                                          real (monitor)
                                                       - members(node,
                                                          "workers") y
                                                          whereis_name(node,
                                                          "task_worker_primary")
                                                          cada 3s
```

`worker1` falla a la 4ª tarea, `worker2` a la 7ª — a propósito, para que se
vea a cada uno fallar por separado, no a la vez. `max_restarts=0` es
deliberado: el primer fallo ya es el definitivo, así que no hay un
reinicio en sitio intermedio que borre el link/monitor que el hub
registró antes de que ocurra (links y monitors son "pid-scoped": un
reinicio en sitio limpia el lado del propio actor reiniciado — ver los
docstrings de [`lapinbeam/links.py`](../../lapinbeam/links.py) y
[`lapinbeam/monitors.py`](../../lapinbeam/monitors.py) — así que este
ejemplo se queda deliberadamente en el caso simple y sin ambigüedad;
`tests-python/test_links.py`/`test_monitors.py` cubren directamente el
caso "el reinicio en sitio no se propaga").

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
hub  | enlazado (link) y monitorizado (monitor) task_worker en worker1@worker1:9101
hub  | enlazado (link) y monitorizado (monitor) task_worker en worker2@worker2:9102
hub  | miembros actuales de 'workers': []
hub  | whereis_name('task_worker_primary'): None
hub  | miembros actuales de 'workers': ['worker1@worker1:9101/task_worker', 'worker2@worker2:9102/task_worker']
hub  | whereis_name('task_worker_primary'): worker1@worker1:9101/task_worker
hub  | DOWN recibido (monitor): 'worker1@worker1:9101/task_worker' salió con motivo 'RuntimeError: fallo permanente tras 4 tareas'
hub  | EXIT recibido (link): 'worker1@worker1:9101/task_worker' salió con motivo 'RuntimeError: fallo permanente tras 4 tareas'
hub  | miembros actuales de 'workers': ['worker2@worker2:9102/task_worker']
hub  | whereis_name('task_worker_primary'): None
hub  | DOWN recibido (monitor): 'worker2@worker2:9102/task_worker' salió con motivo 'RuntimeError: fallo permanente tras 7 tareas'
hub  | EXIT recibido (link): 'worker2@worker2:9102/task_worker' salió con motivo 'RuntimeError: fallo permanente tras 7 tareas'
hub  | miembros actuales de 'workers': []
hub  | whereis_name('task_worker_primary'): None
```

Nótese lo que pasa **sin que el hub tenga que sondear nada** para el motivo
del fallo: el motivo real de cada crash (`RuntimeError: fallo permanente
tras N tareas`) llega dos veces, como dos mensajes distintos — un `Exit`
vía el link y un `Down` vía el monitor — no un evento genérico de
desconexión. Lo que sí se sondea (porque no existe otra forma de
"suscribirse" a un cambio) es `members()`/`whereis_name()`, y ambos se
actualizan solos, en ambas direcciones, cuando cada worker entra y sale
del grupo o suelta el nombre. Los tres contenedores siguen vivos después:
el fallo de `task_worker` es real y definitivo (agotó su presupuesto de
reintentos, que aquí es cero), pero no se propaga al proceso entero —
solo al actor, y desde ahí, a quien esté enlazado o monitorizándolo.

## Qué mirar en el código

- [`worker/main.py`](worker/main.py) — un `Supervisor(max_restarts=0)`
  deliberadamente sin margen de reintentos, y una única unión al grupo
  (y, solo en el worker `PRIMARY=1`, un `register_name()`) hecha en el
  primer mensaje de cada generación del actor (no en `__init__`, que no
  puede hacer `await`, y no una sola vez desde fuera, porque tanto la
  pertenencia al grupo como el nombre registrado son "pid-scoped": no
  sobreviven a un reinicio, y un `task_worker` que sí tuviera presupuesto
  de reintentos tendría que repetir ambas llamadas en cada generación).
- [`hub/main.py`](hub/main.py) — `spawn_supervisor` para el árbol anidado,
  `trap_exit(True)` + `@on(Exit)` + `@on(Down)` en vez del comportamiento
  por defecto de un link sin trap (que mataría a `Watcher` también —
  `monitor()` nunca lo haría, con o sin trap), y `members()`/
  `whereis_name()` sondeados con un simple `while True` + `sleep` porque
  ninguno de los dos tiene otra forma de "suscribirse" a un cambio — son
  vistas de solo lectura, no streams de eventos.

## Lo que este ejemplo no prueba (a propósito)

- **No prueba el caso de reinicio-en-sitio-no-propaga.** Con
  `max_restarts=0`, cada worker solo tiene una generación. El caso donde
  un actor se reinicia varias veces dentro de su presupuesto sin que el
  link/monitor se dispare está cubierto por
  [`tests-python/test_links.py`](../../tests-python/test_links.py) y
  [`test_monitors.py`](../../tests-python/test_monitors.py), no aquí —
  mezclar ambos casos en un solo demo de contenedores habría hecho el
  resultado mucho más difícil de leer.
- **No prueba `unlink()`/`demonitor()`/`leave_group()`/`unregister_name()`
  explícitos**, ni la convergencia de un cuarto nodo que se une al
  clúster tarde, ni un conflicto real de `register_name()` entre dos
  nodos que reclaman el mismo nombre a la vez (todo esto cubierto por
  `tests-python/test_groups.py`/`test_registry.py`). El objetivo aquí es
  demostrar que las cinco primitivas funcionan de verdad cruzando una red
  real entre contenedores — no repetir la cobertura exhaustiva que ya
  tienen los tests de pytest.
