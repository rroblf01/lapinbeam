# Mensajes tipados

Los dicts planos funcionan en todo lapinbeam, pero rara vez quieres
`msg.get("type") == "ACK"` esparcido por el código de tus actores cuando
Python ya tiene tipos reales. Dos piezas independientes hacen esto cómodo:
**payloads que preservan el tipo** (dataclasses y modelos Pydantic
sobreviven el viaje entre nodos) y **despacho tipado** (`@on(Type)`, una
alternativa a un único método `receive`).

## Payloads que preservan el tipo

`lapinbeam.codec` envuelve instancias `@dataclass` y modelos Pydantic v2 en
un sobre etiquetado (`{"__lb_type__": "module.QualName", "data": {...}}`)
antes de que lleguen al transporte Rust, que solo acepta JSON, y reconstruye
el tipo exacto en el extremo receptor:

```python
from dataclasses import dataclass
from lapinbeam import Node, Supervisor, actor


@dataclass
class Task:
    payload_id: int
    name: str


@actor(name="worker")
class Worker:
    async def receive(self, msg: Task):
        print(msg.payload_id, msg.name)  # msg es un Task real, no un dict


# El envío es transparente — sin paso manual de serialización:
# await remote.send(Task(payload_id=1, name="build"))
```

Esto funciona igual para modelos Pydantic v2. Las clases propias (cualquier
cosa que no sea un dataclass ni un modelo Pydantic) necesitan un códec
explícito:

```python
from lapinbeam import register_codec

class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

register_codec(
    Point,
    encode=lambda p: {"x": p.x, "y": p.y},
    decode=lambda d: Point(d["x"], d["y"]),
)
```

Ambos extremos del clúster deben registrar el mismo códec — el tipo se
busca por su etiqueta `module.QualName`, así que la clase debe ser
importable (o estar registrada) allí donde corra `decode_payload`.

!!! note "Los envíos locales siempre son sin copia"
    La preservación de tipo solo importa en envíos **remotos**. Un
    `ActorRef.send(obj)` local pasa el objeto Python exacto por referencia —
    sin codificar, sin copiar, y sin restricción a tipos compatibles con
    JSON o cubiertos por un códec. Puedes enviar *cualquier cosa* entre dos
    actores del mismo nodo.

## Despacho tipado con `@on`

Una vez los mensajes llevan tipos reales, despachar a mano sigue
significando una cadena de comprobaciones `isinstance`/`match` dentro de un
único `receive`. `@on(Type)` mueve eso a la propia declaración del actor:

```python
from dataclasses import dataclass
from lapinbeam import actor, on


@dataclass
class Task:
    payload_id: int
    name: str


@dataclass
class Ack:
    result: int


@actor(name="worker")
class Worker:
    @on(Task)
    async def handle_task(self, msg: Task):
        ...

    @on(Ack)
    async def handle_ack(self, msg: Ack):
        ...

    @on(default=True)
    async def handle_other(self, msg):
        print("mensaje no reconocido:", msg)
```

Un actor con cualquier handler `@on` deja de usar `receive` por completo —
los mensajes se despachan por `type(msg)` al handler correspondiente.
`@on(default=True)` marca un único handler de "cajón de sastre" para
cualquier tipo sin handler dedicado (incluidos los dicts planos); es la
forma más sencilla de estar a salvo frente a formas de mensaje que no
planeaste. Sin un handler por defecto, un tipo sin coincidencia lanza
`TypeError`, lo que hace fallar al actor y deja que `Supervisor` lo reinicie
— la misma filosofía "let it crash" que ya sigue el resto del framework para
excepciones no controladas en `receive`.

Los actores que solo definen `receive` no se ven afectados en absoluto —
`@on` es estrictamente aditivo, no una migración forzosa.

### O prescinde de `@on` por completo: usa `match`

Como el mensaje ya llega con su tipo real, no necesitas `@on` para tener
despacho tipado — el pattern matching estructural de Python funciona
directamente sobre dataclasses dentro de un único `receive`:

```python
async def receive(self, msg):
    match msg:
        case Task(payload_id=pid, name=name):
            ...
        case Ack(result=r):
            ...
        case _:
            print("mensaje no reconocido:", msg)
```

Esto no cuesta nada (ninguna característica del framework está involucrada)
y da destructuring en el mismo aliento que la comprobación de tipo. Recurre
a `@on` en su lugar cuando prefieras tener un método pequeño por tipo de
mensaje — por ejemplo, para testear cada handler de forma aislada, o cuando
un único bloque `match` crece demasiado.
