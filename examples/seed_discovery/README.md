# Ejemplo: descubrimiento de nodos vía nodo semilla

En `examples/app_node_*.py` y `examples/police_investigation/`, cada nodo
tiene que conocer explícitamente la dirección exacta de cada peer al que
necesita hablar — para una malla de N nodos, eso es hasta N·(N-1)/2
direcciones que alguien tiene que configurar a mano.

Este ejemplo muestra cómo evitarlo con el patrón más ligero posible: cada
nodo solo necesita conocer **una** dirección — la de un nodo "semilla" ya
en marcha. Todo lo demás se aprende preguntando "¿a quién conoces tú?" y
conectando también a esas respuestas, recursivamente, hasta que una pasada
completa no encuentra nada nuevo.

No hay ningún cambio en el núcleo de Rust — el mecanismo (`register_discovery`
y `join_via_seeds`) vive en [`lapinbeam/discovery.py`](../../lapinbeam/discovery.py),
construido entero sobre la API pública que ya existía (`Node`, `@actor`,
`ask()` + `current_message().reply()`, `on_event`). Este ejemplo es solo
[`node_app.py`](node_app.py) — el código de demostración que lo usa.

> **Nota:** `lapinbeam.discovery` es nuevo en el paquete y todavía no está
> publicado en PyPI en el momento de escribir esto — hace falta una versión
> de `lapinbeam` posterior a `1.0.3` (ver `CHANGELOG.md`). Hasta entonces,
> `docker compose up --build` fallará con `ImportError` porque la versión
> instalada desde PyPI no trae el módulo todavía.

## Cómo funciona

```
seed  ◀───────────────┐
  ▲                    │  1) node1 se conecta a seed (la única dirección
  │ 1                  │     que conoce) y le pregunta "¿a quién conoces?"
  │                    │
node1 ◀── 2 ── node2   │  2) seed responde (al principio, solo a sí misma).
                        │     node1 no descubre a node2/node3 todavía si
node1 ──── 3 ──▶ node3  │     estos aún no se han conectado a seed.
                        │
                        │  3) una segunda pasada (unos segundos después)
                        │     repite la pregunta contra todo lo ya conocido
                        │     — para entonces, seed ya sabe de node2/node3
                        │     (se conectaron mientras tanto), así que
                        │     node1 los descubre y se conecta directamente.
```

`node1`, `node2` y `node3` **nunca** se configuran con la dirección del uno
o del otro — solo con la de `seed`. Aun así, terminan completamente
conectados entre sí.

## Ejecutarlo

```bash
cd examples/seed_discovery
docker compose up --build
```

`node1`/`node2`/`node3` arrancan en cuanto `seed` está sana, básicamente a
la vez entre ellos — a propósito, para que se vea la condición de carrera
real que resuelve la segunda pasada. En los logs verás algo como:

```
node1  | uniéndome vía semillas: ['seed@seed:9000']
node1  | ronda 1: descubiertos 1 peers: ['seed@seed:9000']
node1  | ronda 2: total 3 peers: ['node2@node2:9002', 'node3@node3:9003', 'seed@seed:9000']
node1  | peers conectados ahora mismo: 3
```

`node1` solo encuentra a `seed` en la ronda 1 (node2/node3 todavía no se
habían conectado en ese instante) — la ronda 2, unos segundos después, ya
los recoge. Los cuatro contenedores terminan reportando `peer_count() == 3`
sin que ninguno conociera de antemano la dirección de los otros dos.

## El módulo (`lapinbeam.discovery`)

Dos piezas, importadas directamente desde `lapinbeam`
(`from lapinbeam import register_discovery, join_via_seeds`):

- **`register_discovery(node, sup)`**: arranca un actor interno que
  responde "a quién conozco" (`ask()` + `current_message().reply()`),
  y mantiene esa respuesta al día escuchando los eventos
  `peer_connected`/`peer_disconnected` de este mismo nodo. El conjunto de
  peers conocidos vive **fuera** del actor a propósito: `on_event` no tiene
  forma de desregistrar un listener, así que si viviera dentro del
  `__init__` del actor, un reinicio del actor tras un fallo registraría un
  segundo listener permanente atado a la instancia descartada.
- **`join_via_seeds(node, seeds)`**: se conecta a cada semilla, le pregunta
  a quién conoce, se conecta también a esas respuestas, y repite con cada
  nueva hasta que una pasada completa no descubre a nadie más. Es
  idempotente — `connect_peer()` ya es un no-op si el peer sigue conectado
  — así que se puede volver a llamar más tarde (como hace `node_app.py`,
  dos veces, con una pausa de 2s) para recoger rezagados sin ningún coste
  extra en los que ya se conocían.

## Lo que esto no resuelve (y por qué es aceptable aquí)

- **No hay redescubrimiento continuo.** Es una comprobación puntual al
  arrancar (repetida una vez más, no en bucle) — no un gossip periódico de
  verdad. Si dos nodos se unen exactamente al mismo tiempo y ninguno llega
  a decírselo al otro en las dos rondas, no se conectarán entre sí hasta
  que algo vuelva a llamar a `join_via_seeds` (o hasta que un tercer nodo
  se una más tarde y los conecte a ambos transitivamente).
- **Sin detección de fallos propia.** Se apoya enteramente en el
  `peer_timeout` que ya tiene lapinbeam — no hay nada adicional tipo SWIM o
  heartbeat de membresía.
- **Un único punto de fallo mientras nadie más se una.** Si `seed` se cae
  antes de que el resto de nodos hayan terminado su descubrimiento, no hay
  ningún mecanismo de reintento contra otra semilla en este ejemplo —
  bastaría con pasar más de una dirección en `SEEDS` y probarlas todas.

Para un caso real con membresía que cambia constantemente (nodos que se
van y vienen todo el rato), esto se quedaría corto — pero para un clúster
de tamaño mayormente estable (el caso común en Docker Compose/Kubernetes),
es toda la complejidad que hace falta para pasar de "cada nodo necesita la
dirección de todos" a "cada nodo necesita una única dirección compartida".
