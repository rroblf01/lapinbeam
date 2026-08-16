"""Actor decorator: marks a class as an actor managed by a `Supervisor`."""

_UNSET = object()


def actor(name=None):
    """Decorator that marks `cls` as an actor.

    Usage (single generic handler):

        @actor(name="worker")
        class Worker:
            async def receive(self, msg):
                ...

    Usage (typed dispatch, see `on`):

        @actor(name="worker")
        class Worker:
            @on(Task)
            async def handle_task(self, msg: Task):
                ...

            @on(default=True)
            async def handle_other(self, msg):
                ...

    The actor name defaults to the class name. Actors must be unique per node.
    """
    if callable(name) and not isinstance(name, str):
        cls = name
        cls.__lapinbeam_actor__ = _build_metadata(cls, cls.__name__)
        return cls

    def decorator(cls):
        cls.__lapinbeam_actor__ = _build_metadata(cls, name or cls.__name__)
        return cls

    return decorator


def on(msg_type=_UNSET, *, default=False):
    """Marks a method as the handler for messages of type `msg_type`.

    An actor that has any `@on`-decorated method dispatches each incoming
    message to the handler registered for `type(msg)`, instead of calling a
    single `receive(msg)`. Exactly one handler may be marked `@on(default=True)`
    to catch any message whose type has no dedicated handler — this is the
    simplest way to stay safe against messages you didn't explicitly plan
    for, e.g. plain dicts, or types added later elsewhere in the cluster:

        @on(Task)
        async def handle_task(self, msg: Task):
            ...

        @on(default=True)
        async def handle_other(self, msg):
            print("unrecognized message:", msg)

    Actors that only define `receive` (no `@on` at all) are unaffected — this
    is purely additive.
    """
    if not default and msg_type is _UNSET:
        raise TypeError("on() requires a message type, e.g. on(Task), or on(default=True)")
    if default and msg_type is not _UNSET:
        raise TypeError("on(msg_type) and on(default=True) are mutually exclusive")

    def decorator(func):
        func.__lapinbeam_on__ = {"type": msg_type, "default": default}
        return func

    return decorator


def _build_metadata(cls, name):
    handlers = {}
    default_handler = None
    for attr_name in dir(cls):
        attr = getattr(cls, attr_name, None)
        spec = getattr(attr, "__lapinbeam_on__", None)
        if spec is None:
            continue
        if spec["default"]:
            if default_handler is not None:
                raise TypeError(
                    f"{cls.__name__} defines more than one @on(default=True) handler "
                    f"({default_handler!r} and {attr_name!r})"
                )
            default_handler = attr_name
        else:
            msg_type = spec["type"]
            if msg_type in handlers:
                raise TypeError(
                    f"{cls.__name__} defines more than one @on handler for "
                    f"{msg_type!r} ({handlers[msg_type]!r} and {attr_name!r})"
                )
            handlers[msg_type] = attr_name
    return {"name": name, "handlers": handlers, "default_handler": default_handler}


def actor_name(cls) -> str:
    return cls.__lapinbeam_actor__["name"]
