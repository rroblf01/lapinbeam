# Ejemplo: flujo de un almacén de ventas

Un ejemplo más elaborado que los de `examples/app_node_*.py`: un formulario
HTTP (FastAPI) dispara un pedido que atraviesa una tubería de actores de
lapinbeam repartida en **tres contenedores/nodos distintos**, cada uno
haciendo un salto de red real al siguiente:

```
                    POST /orders
                        │
                        ▼
┌─────────────┐   intake → stock_check → valuation → dispatch  ┌─────────────┐
│     api     │ ───────────────────────────────────────────▶  │ fulfillment │
│ (FastAPI +  │                                                │ (4          │
│  lapinbeam) │ ◀── progress updates (en cada etapa) ────────  │  actores)   │
└─────────────┘                                                └──────┬──────┘
       ▲                                                               │
       │             confirmación final ("archivado")                 │ pedido listo
       └──────────────────────────────────────────────────┬───────────┘
                                                            ▼
                                                      ┌───────────┐
                                                      │  archive  │
                                                      │ (escribe  │
                                                      │  a disco) │
                                                      └───────────┘
```

- **`api`**: recibe el formulario, crea el pedido y lo reenvía al nodo
  `fulfillment` — la petición HTTP responde al instante (`202 Accepted`)
  sin esperar a que termine la preparación. Un actor `order_tracker` local
  recibe actualizaciones de progreso de los otros dos nodos y las expone
  para consulta (`GET /orders/{id}`).
- **`fulfillment`**: cuatro actores encadenados *dentro del mismo nodo*
  (`intake` → `stock_check` → `valuation` → `dispatch`), cada uno simulando
  una etapa real de la preparación del pedido, y cada uno reportando su
  progreso de vuelta a `api` con un salto de red independiente (no
  reenviando la respuesta a través de la cadena).
- **`archive`**: recibe el pedido ya listo para envío, lo persiste como
  JSON en un volumen de Docker, y confirma a `api` que quedó archivado —
  un tercer salto de red distinto de los dos anteriores.

Cada servicio solo depende de `docker`, `docker compose` y `uv` — no hace
falta instalar Python, Rust ni nada más en la máquina anfitriona. `lapinbeam`
se instala desde PyPI como cualquier otra dependencia.

## Cómo ejecutarlo

```bash
cd examples/sales_warehouse
docker compose up --build
```

Cuando los tres servicios estén sanos (`docker compose ps`):

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"cliente":"Ana Pérez","producto":"monitor 27 pulgadas","direccion_envio":"Calle Mayor 12"}'
# {"order_id": "...", "status_url": "/orders/..."}

curl http://localhost:8000/orders/<order_id>
```

`GET /orders/<order_id>` va devolviendo `history` con cada etapa a medida
que se completa, hasta llegar a `"status": "archivado"`.

## Generador de carga

`bench/load_test.py` no tiene dependencias — solo librería estándar —, así
que corre igual con `uv run python bench/load_test.py` o con
`python3 bench/load_test.py`:

```bash
python3 bench/load_test.py -n 300 -c 25   # 300 pedidos, 25 en paralelo
```

Solo mide la latencia de *aceptación* HTTP (fire-and-forget), no cuánto
tarda el pedido completo en prepararse — para eso, consulta `GET /orders`
después.

## Resultado del hallazgo importante: un bug real de lapinbeam

Al medir el consumo **en reposo** de este mismo ejemplo (sin ninguna
petición, solo con los tres nodos conectados entre sí) apareció esto:

| Contenedor | CPU en reposo (lapinbeam 1.0.2, tal cual está en PyPI) |
| --- | --- |
| `api` | 70-80% |
| `fulfillment` | 74-80% |
| `archive` | 72-140% |

Tres nodos que no están haciendo *nada* no deberían consumir eso. La causa,
confirmada y arreglada en el núcleo de Rust del propio lapinbeam (ver
`CHANGELOG.md`): cada heartbeat recibido se respondía con otro heartbeat
indistinguible de uno nuevo — así que esa respuesta generaba otra respuesta,
que generaba otra, sin parar, entre cada par de nodos conectados. No es un
problema de este ejemplo: se reproduce igual con el `docker-compose.yml` de
la raíz del repo, y con solo dos nodos en el mismo proceso Python sin Docker
de por medio.

**Los números de abajo están medidos con el fix ya aplicado** (compilado en
local desde el código fuente en el momento de escribir esto — llegará a
PyPI en la próxima versión publicada; hasta entonces, `lapinbeam>=1.0.2`
instalado desde PyPI reproducirá los números de la tabla de arriba, no los
de abajo).

## Medición de CPU y RAM (con el fix aplicado)

Medido con `docker stats` en la máquina del mantenedor — no es un benchmark
formal ni reproducible con precisión entre máquinas distintas; lo
importante es la forma (plano en reposo, picos acotados bajo carga), no el
valor exacto.

### En reposo (los tres nodos conectados, sin peticiones)

| Contenedor | CPU | RAM |
| --- | --- | --- |
| `api` | 0-10%\* | ~55 MiB |
| `fulfillment` | 2-5% | ~34 MiB |
| `archive` | 0-5% | ~35 MiB |

\* Los picos ocasionales en `api` vienen del propio `healthcheck` de Docker
(arranca un intérprete de Python cada 2s), no de lapinbeam.

### Bajo carga (300 pedidos, concurrencia 25, repetido dos veces = 600 pedidos)

| Contenedor | CPU media | CPU pico |
| --- | --- | --- |
| `api` | 5-6% | 20-33% |
| `fulfillment` | 3% | ~6% |
| `archive` | 2-3% | ~6% |

- Los 600 pedidos terminaron con `"status": "archivado"` — sin ningún
  error en los logs de los tres servicios.
- La RAM se mantuvo prácticamente plana durante y después de la carga
  (`api` ~55→60 MiB, `fulfillment` ~34→46 MiB, `archive` ~35→37 MiB) — sin
  indicios de fuga tras procesar 600 pedidos.
- Aceptación HTTP: ~1.100-1.180 req/s, p50 16-17ms, p99 41-85ms — de nuevo,
  esto es solo cuánto tarda en aceptar el pedido (`202`), no en resolverlo;
  el propio pipeline (4 etapas encadenadas + archivado) tarda más porque
  cada actor procesa un mensaje a la vez.

## Simplificaciones deliberadas (esto es un ejemplo, no un sistema real)

- `ORDERS` vive en un diccionario en memoria dentro de `api` — se pierde si
  el proceso se reinicia. Un sistema real necesitaría una base de datos
  (ver `examples/order_stream/` para una variante que sí usa
  Postgres como fuente de verdad durable).
- Sin autenticación ni autorización en la API.
- Sin `cluster_secret` entre los nodos (ver la sección "Security" de
  `docs/index.md` del proyecto principal para cuándo hace falta).
- El "stock disponible" y el "importe del pedido" son simulados con
  `random.sample`/sumas de valores fijos — no hay ningún inventario real
  detrás.
