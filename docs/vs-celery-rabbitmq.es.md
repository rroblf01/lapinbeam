# lapinbeam frente a Celery + RabbitMQ

Estas herramientas resuelven problemas genuinamente distintos, así que
"cuál es mejor" es la pregunta equivocada — la correcta es qué forma tiene
realmente tu carga de trabajo. Esta página es una comparación técnica, no
un argumento de venta: Celery + RabbitMQ es infraestructura madura y
probada en producción, y lapinbeam es una biblioteca en fase alpha que no
se ha probado a esa escala.

## La forma del problema que ataca cada uno

**Celery + RabbitMQ** modela **trabajos de fondo duraderos**: una tarea es
un nombre de función más argumentos, colocada en una cola duradera,
recogida por cualquiera de un conjunto de procesos worker, con reintentos
automáticos, límites de tasa, planificación (`celery beat`) y manejo de
dead-letter si sigue fallando. El broker es la fuente de verdad sobre qué
trabajo existe — si cada worker e incluso el propio broker se reinician,
las colas duraderas siguen reentregando los mensajes sin confirmar.

**lapinbeam** modela **comunicación directa, tipada, actor a actor**: dos
procesos de larga duración (o el mismo proceso, para actores locales)
intercambian mensajes sobre una conexión que poseen, sin ningún servicio
intermediario. No hay cola que inspeccionar independientemente del proceso
que tiene la mailbox, y no hay reentrega — si un nodo no es alcanzable en el
momento del envío, el mensaje desaparece.

## Dónde lapinbeam tiene una ventaja real

- **Sin broker que operar.** Celery necesita RabbitMQ (o Redis) como
  servicio aparte y monitorizado. El transporte de lapinbeam es un import de
  biblioteca — dos procesos Python con `pip install lapinbeam` y la red es
  toda la infraestructura.
- **Latencia.** Un salto por el broker implica: serializar → publicar →
  el broker persiste/enruta → el consumidor hace polling o recibe un push →
  deserializar → round-trip de ack. lapinbeam habla directamente sobre una
  conexión TCP multiplexada por peer, sin intermediario. Ver
  [Benchmarks](benchmarks.md): el despacho local son microsegundos, y un
  round trip remoto en loopback (envío + ack) está por debajo de medio
  milisegundo p50. Un round trip de tarea Celery a través de RabbitMQ suele
  estar entre unos pocos y varias decenas de milisegundos — el salto por el
  broker y el polling del pool de workers hacen más trabajo que la propia
  lógica de la tarea para jobs pequeños.
- **Payloads tipados, no blobs JSON por convención.** Los modelos
  `@dataclass`/Pydantic viajan como el tipo exacto en el extremo receptor
  (ver [Mensajes tipados](typed-messages.md)), y `@on(Type)` despacha
  directamente sobre ese tipo. Los argumentos de una tarea Celery son
  argumentos posicionales/con nombre serializados por el serializador
  configurado (JSON por defecto); nada te impide pasar el `.dict()` de un
  modelo Pydantic a mano, pero el framework en sí no preserva ni despacha
  sobre el tipo.
- **Estado del actor.** Una instancia de actor persiste a través de
  mensajes (es un objeto Python con `__init__`, mantenido vivo por
  `Supervisor` entre envíos). Una tarea Celery es una invocación de función
  sin estado — cualquier estado tiene que vivir fuera (una base de datos,
  Redis, encadenamiento de tareas).

## Dónde Celery + RabbitMQ gana claramente

- **Durabilidad.** RabbitMQ persiste mensajes a disco (con colas/exchanges
  duraderos) y reentrega los no confirmados tras un fallo. lapinbeam **no
  tiene ninguna persistencia**: un canal en memoria acotado por actor, sin
  volcado a disco, sin garantía de entrega más allá de "la conexión TCP
  estaba levantada y la mailbox tenía sitio". Un mensaje enviado durante una
  partición de red simplemente se pierde — ver
  [Limitaciones](index.md#limitaciones).
- **Escalado horizontal de consumidores.** Muchos workers Celery pueden
  consumir de la misma cola, repartiendo trabajo automáticamente; añades
  workers y sube el throughput. En lapinbeam, un nombre de actor es una
  mailbox en un nodo — no hay reparto integrado del mismo trabajo lógico
  entre varios procesos.
- **Reintentos, límites de tasa, planificación, workflows.** `celery beat`
  (planificación tipo cron), `retry(countdown=..., max_retries=...)`,
  límites de tasa por tarea, chains/chords/groups para componer workflows
  de varios pasos — nada de esto existe en lapinbeam. `Supervisor` reinicia
  un *actor que falló*, que es algo bastante distinto de reintentar una
  *unidad de trabajo que falló*.
- **Madurez operativa.** Flower para monitorización, más de una década de
  uso en producción, un ecosistema grande de extensiones y guías de
  integración. lapinbeam está en alpha: sin dashboard, sin herramientas de
  operación dedicadas todavía, y su protocolo de red no tiene garantías de
  compatibilidad entre versiones.

## Recomendación práctica

Recurre a **Celery + RabbitMQ** para: enviar emails, redimensionar
imágenes, procesar subidas, cualquier cosa que lamentarías perder ante un
fallo, o cualquier cosa que se beneficie de un pool de workers
intercambiables.

Recurre a **lapinbeam** para: una simulación en tiempo real o una máquina de
estados de servidor de juego repartida entre procesos, un clúster de
servicios con estado que necesitan llamarse entre sí con latencia
submilisegundo y payloads tipados, o cualquier caso donde "los dos procesos
están levantados y son directamente alcanzables, y perder un mensaje en
vuelo ante un fallo es aceptable" — el mismo compromiso que asume el `send`
de Erlang/Elixir.

Nada te impide usar ambos en el mismo sistema: Celery para trabajo de fondo
duradero, lapinbeam para la malla de actores de baja latencia delante de él.
