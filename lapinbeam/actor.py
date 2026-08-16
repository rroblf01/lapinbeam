"""Actor decorator: marks a class as an actor managed by a `Supervisor`."""


def actor(name=None):
    """Decorator that marks `cls` as an actor.

    Usage:

        @actor(name="worker")
        class Worker:
            async def receive(self, msg):
                ...

    The actor name defaults to the class name. Actors must be unique per node.
    """
    if callable(name) and not isinstance(name, str):
        cls = name
        cls.__lapinbeam_actor__ = {"name": cls.__name__}
        return cls

    def decorator(cls):
        cls.__lapinbeam_actor__ = {"name": name or cls.__name__}
        return cls

    return decorator


def actor_name(cls) -> str:
    return cls.__lapinbeam_actor__["name"]
