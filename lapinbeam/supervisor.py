"""Supervisor: spawns actors (and nested Supervisors) and restarts them on
unhandled exceptions, according to a restart strategy.
"""

import asyncio
import concurrent.futures
import functools
import inspect
import time

from .actor import actor, actor_name
from .context import current as current_message_var
from .context import current_actor as current_actor_var
from .node import get_current_node
from .refs import ActorRef, PoolRef, SupervisorRef

_STRATEGIES = frozenset({"one_for_one", "one_for_all", "rest_for_one"})


class _ActorChild:
    """Bookkeeping for one `spawn()`ed actor. `task` stays the *same*
    object across this child's own restart-in-place cycles (a persistent
    loop, like the pre-tree-supervision `_watch`) — it's only ever
    replaced when a sibling's crash sweeps this child into a group restart
    under `one_for_all`/`rest_for_one`. `ActorRef.task` reads this live
    instead of freezing it at spawn time, precisely to observe that
    replacement correctly."""

    __slots__ = ("name", "actor_cls", "args", "kwargs", "mailbox", "driver", "pending_exit", "task")

    def __init__(self, name, actor_cls, args, kwargs, mailbox):
        self.name = name
        self.actor_cls = actor_cls
        self.args = args
        self.kwargs = kwargs
        self.mailbox = mailbox
        self.driver = None
        # Set by lapinbeam.links to redirect a genuine `CancelledError`
        # (link-triggered kill) into the normal crash/restart path instead
        # of a silent shutdown. `None` for an ordinary Supervisor.
        self.pending_exit = None
        self.task = None


class _SupervisorChild:
    """Bookkeeping for one `spawn_supervisor()`ed nested Supervisor.
    `build` is a recipe re-run on every restart (a fresh child `Supervisor`
    each time) rather than a pre-built instance, since a Supervisor's
    state (its children, its restart budget) is used up once it's run —
    it can't itself be "restarted" in place the way an actor's mailbox is
    reused. Same task-persistence rule as `_ActorChild`."""

    __slots__ = ("name", "build", "strategy", "max_restarts", "restart_window", "supervisor", "task")

    def __init__(self, name, build, strategy, max_restarts, restart_window):
        self.name = name
        self.build = build
        self.strategy = strategy
        self.max_restarts = max_restarts
        self.restart_window = restart_window
        self.supervisor = None
        self.task = None


class _PoolSpawn:
    """Returned by `Supervisor.spawn_pool()`. Awaitable — `pool = await
    sup.spawn_pool(...)` — and an async context manager — `async with
    sup.spawn_pool(...) as pool: ...`, which calls `pool.stop()` on the
    way out — since spawning a pool needs an `await` (each worker needs
    one real message to start its loop) but `spawn_pool()` itself can't
    be a plain `async def` and *also* support `async with` on its call
    expression. Building the pool only happens once, on first
    await/`__aenter__`, even if a caller somehow used both on the same
    object."""

    __slots__ = ("_supervisor", "_handler", "_n_workers", "_args", "_name",
                 "_queue_capacity", "_executor", "_key", "_kwargs", "_pool")

    def __init__(self, supervisor, handler, n_workers, args, name,
                 queue_capacity, executor, key, kwargs):
        self._supervisor = supervisor
        self._handler = handler
        self._n_workers = n_workers
        self._args = args
        self._name = name
        self._queue_capacity = queue_capacity
        self._executor = executor
        self._key = key
        self._kwargs = kwargs
        self._pool = None

    async def _build(self):
        if self._pool is None:
            self._pool = await self._supervisor._spawn_pool(
                self._handler, self._n_workers, self._args, self._name,
                self._queue_capacity, self._executor, self._key, self._kwargs,
            )
        return self._pool

    def __await__(self):
        return self._build().__await__()

    async def __aenter__(self):
        return await self._build()

    async def __aexit__(self, exc_type, exc, tb):
        if self._pool is not None:
            await self._pool.stop()
        return False


class Supervisor:
    """Owns actor (and nested Supervisor) lifecycle. On crash, restarts
    according to `strategy`:

      - ``one_for_one``: restart only the crashed child. If it exhausts
        its restart budget, only *that* child gives up — unrelated
        siblings are never affected, whether restarting or giving up
        (this is what makes it safe to host many independent, unrelated
        actors under one Supervisor over its life, e.g. a worker-pool
        pattern).
      - ``one_for_all``: a crash restarts every child this Supervisor
        manages, not just the one that failed.
      - ``rest_for_one``: a crash restarts the crashed child and every
        child spawned *after* it (spawn order matters).

    For ``one_for_all``/``rest_for_one``, exhausting the restart budget
    tears down the *whole* subtree (every child, not just the one that
    failed) and this Supervisor itself is considered to have given up —
    observable via a `SupervisorRef.task` if this Supervisor was itself
    spawned as a nested child of another one.
    """

    def __init__(self, strategy="one_for_one", node=None, max_restarts=3,
                 restart_window=5.0):
        if strategy not in _STRATEGIES:
            raise ValueError(f"unsupported strategy: {strategy!r}")
        self.strategy = strategy
        self._node = node
        self.max_restarts = max_restarts
        self.restart_window = restart_window
        self._restart_times = []
        self._children = []
        self._give_up_exc = None
        self._given_up = asyncio.Event()

    @property
    def node(self):
        if self._node is None:
            self._node = get_current_node()
        if self._node is None:
            raise RuntimeError("no node started; start a Node or pass node= to Supervisor")
        return self._node

    def spawn(self, actor_cls, *args, **kwargs):
        """Spawns an actor and returns an `ActorRef`. Non-blocking."""
        node = self.node
        name = actor_name(actor_cls)
        # Register the mailbox synchronously so sends can land immediately,
        # even though the child's own task only runs on the next loop
        # iteration.
        mailbox = asyncio.Queue(maxsize=node.mailbox_capacity or 0)
        node.register_actor(name, mailbox)
        child = _ActorChild(name, actor_cls, args, kwargs, mailbox)
        self._children.append(child)
        loop = asyncio.get_running_loop()
        child.task = loop.create_task(self._run_actor_child(child))
        node._register_task(child.task)
        return ActorRef(node, name, child=child)

    def spawn_pool(self, handler, n_workers, *args, name,
                    queue_capacity=None, executor=None, key=None, **kwargs):
        """Spawns a *fixed* pool of `n_workers` actors sharing one queue —
        the pattern `examples/order_stream/` used to build by hand before
        this existed: a reserved "dispatcher" actor (named `name`, the one
        a remote node addresses via `get_remote_actor(peer, name)`) that
        just drops incoming messages on a shared `asyncio.Queue`, and
        `n_workers` persistent actors pulling from it — whichever is free
        next picks up the next message, not round-robin (unless `key` is
        given — see below).

        Unlike `spawn()`, this can't hand back a ready `PoolRef`
        synchronously — each worker needs one real message to start its
        loop, and that has to be awaited. So `spawn_pool()` itself returns
        an object that's both awaitable and an async context manager,
        whichever reads better at the call site:

            pool = await sup.spawn_pool(handler, 5, name="processors")
            ...
            await pool.stop()                    # tear down just this pool

            async with sup.spawn_pool(handler, 5, name="processors") as pool:
                ...
            # pool.stop() already called on the way out

        `handler` is either a plain async function or an `@actor`-decorated
        class:

        - **Function**: `handler(msg, *args, **kwargs)` runs once per
          message `pool.send()` delivers. `args`/`kwargs` are the same for
          every worker and every message (e.g. a shared `node` reference)
          — workers are stateless between calls, nothing is kept on `self`
          because there is no `self`.
        - **`@actor` class**: each of the `n_workers` gets its own
          instance, built once via `handler(*args, **kwargs)` — here
          `args`/`kwargs` go to the constructor, exactly like `spawn()`.
          Each message is dispatched through that instance's own `@on`
          handlers (or `receive`, if it defines no `@on` at all), so a
          worker can keep real per-instance state (a cache, a counter, a
          connection) across the messages it happens to pick up. Which
          worker gets which message is still not deterministic — don't
          rely on message *order* landing on any particular instance
          unless you also pass `key` (below).

        Either way, inside the handler, `current_message()`/
        `current_message().reply()` and `ask()`/`ask_stream()` sent to the
        pool all work exactly as they would in an ordinary `spawn()`ed
        actor — the dispatcher captures each message's metadata
        (`reply_to`, `correlation_id`, ...) and the worker re-binds it for
        the duration of that one dispatch, even though many dispatches
        share the worker's single `receive()` invocation.

        A handler that raises is caught here, not left to crash the
        worker: nothing ever sends a pool worker a second message on its
        own mailbox, so a crashed worker would never restart on its own
        the way an ordinary `spawn()`ed actor does — the exception is
        reported via `on_event(kind="pool_worker_error")` instead, and the
        worker moves on to the next queued message. For a class `handler`
        this means the *same* instance keeps running afterward — if the
        exception left `self` in a half-updated state, that state stays
        half-updated for whichever message that worker picks up next
        (unlike a `spawn()`ed actor's crash, which gets a fresh instance
        on restart).

        `queue_capacity` bounds the shared queue (or, with `key`, *each*
        per-worker queue) — `None` (default) means unbounded, same
        convention as `Node(mailbox_capacity=...)`. Once the queue is
        full, a further `pool.send()` is dropped and reported via
        `on_event(kind="pool_queue_full")` instead of blocking the sender
        or growing memory without limit — same "bounded, drop, and tell
        someone" shape as `mailbox_full`, not a new backpressure mechanism.

        `executor="thread"` or `executor="process"` routes each message to
        a `concurrent.futures.ThreadPoolExecutor`/`ProcessPoolExecutor`
        (sized to `n_workers`) instead of running `handler` on the event
        loop — for genuinely CPU-bound work, where `asyncio.sleep`-style
        concurrency doesn't help (see the warning in
        [Concurrency](https://rroblf01.github.io/lapinbeam/getting-started/#concurrency-one-actor-handles-one-message-at-a-time)).
        Only a plain **synchronous** function `handler` is supported here
        (no `@actor` class — there's no way to keep Python state on `self`
        across a process boundary, and threads share the GIL so there's no
        parallelism gain in keeping state there either); `handler`'s
        return value is sent back automatically via `current_message().reply()`
        equivalent if the message was sent through `ask()`/`ask_stream()`.
        `executor="process"` additionally requires `handler` (and every
        message/`args`/`kwargs` it's called with) to be picklable — a
        module-level function, not a closure or lambda; a failure to
        pickle surfaces as an ordinary `on_event(kind="pool_worker_error")`
        for that message, same as any other handler exception.

        `key(msg)` turns this from a load-balanced pool into a *sharded*
        one: instead of one shared queue, each worker gets its own, and
        the dispatcher routes `msg` to worker `hash(key(msg)) % n_workers`
        — so every message with the same key is always handled by the
        same worker, in the order it arrived, while different keys still
        run in parallel across workers. Use this when per-key ordering
        matters (e.g. all updates for the same `order_id` must apply in
        order) — the tradeoff is that an unlucky key distribution can
        leave some workers idle while others queue up, unlike the default
        whichever-is-free routing.

        Use this instead of `n_workers` separate `spawn()` calls when work
        items arrive faster than one actor could get through them, but the
        actual per-item work is cheap enough (or I/O-bound enough) that a
        handful of workers can keep up — see
        [Concurrency](https://rroblf01.github.io/lapinbeam/getting-started/#concurrency-one-actor-handles-one-message-at-a-time)
        for why a single actor never parallelizes itself. `name` is
        required (not derived from `handler` automatically) so two pools
        never collide by accident — actor names must be unique per node.

        The returned `PoolRef` also has `map(items)` — `asyncio.gather()`
        over `ask()` for every item, in order — for "send N independent
        items, collect N results" without writing the `gather()` out by
        hand; and `stop()` to tear down just this pool's dispatcher and
        workers (and, with `executor=`, its thread/process pool) without
        touching anything else on this `Supervisor` — useful for a server
        that creates and destroys pools over its lifetime rather than
        keeping one fixed at startup.
        """
        return _PoolSpawn(self, handler, n_workers, args, name, queue_capacity, executor, key, kwargs)

    async def _spawn_pool(self, handler, n_workers, args, name, queue_capacity, executor, key, kwargs):
        node = self.node
        is_actor_cls = inspect.isclass(handler) and hasattr(handler, "__lapinbeam_actor__")
        maxsize = queue_capacity or 0

        exec_pool = None
        if executor is not None:
            if executor not in ("thread", "process"):
                raise ValueError(f"executor must be 'thread', 'process', or None, not {executor!r}")
            if is_actor_cls:
                raise TypeError(
                    "spawn_pool(executor=...) doesn't support an @actor class handler "
                    "— per-instance state can't cross a thread/process boundary; use a "
                    "plain function"
                )
            if inspect.iscoroutinefunction(handler):
                raise TypeError(
                    "spawn_pool(executor=...) requires a synchronous handler (def, not "
                    "async def) — it runs off the event loop, in a real thread/process"
                )
            executor_cls = (
                concurrent.futures.ThreadPoolExecutor
                if executor == "thread"
                else concurrent.futures.ProcessPoolExecutor
            )
            exec_pool = executor_cls(max_workers=n_workers)

        sharded = key is not None
        if sharded:
            queues = [asyncio.Queue(maxsize=maxsize) for _ in range(n_workers)]
        else:
            queue = asyncio.Queue(maxsize=maxsize)

        @actor(name=name)
        class _Dispatcher:
            async def receive(self, msg):
                # Captured *inside* the dispatcher's own receive() — this
                # is the one point where `current_message_var` correctly
                # describes `msg`, since `_drive` binds it fresh per
                # dispatch. Carried alongside `msg` so a worker can restore
                # it later, potentially long after the dispatcher has
                # moved on to other messages.
                meta = current_message_var.get()
                if sharded:
                    try:
                        target = queues[hash(key(msg)) % n_workers]
                    except Exception as exc:
                        node._on_core_event({
                            "kind": "pool_worker_error",
                            "pool": name,
                            "detail": f"key() raised {type(exc).__name__}: {exc}",
                        })
                        return
                else:
                    target = queue
                try:
                    target.put_nowait((msg, meta))
                except asyncio.QueueFull:
                    node._on_core_event({
                        "kind": "pool_queue_full",
                        "pool": name,
                        "detail": f"pool {name!r}'s queue is full (capacity={maxsize})",
                    })

        async def _dispatch_to_instance(instance, item):
            actor_meta = type(instance).__lapinbeam_actor__
            handlers = actor_meta["handlers"]
            default_handler = actor_meta["default_handler"]
            if not handlers:
                await instance.receive(item)
                return
            handler_name = handlers.get(type(item), default_handler)
            if handler_name is None:
                raise TypeError(
                    f"{type(instance).__name__} has no @on handler for "
                    f"{type(item).__name__} messages and no @on(default=True) fallback"
                )
            await getattr(instance, handler_name)(item)

        def _build_worker(index):
            worker_queue = queues[index] if sharded else queue

            @actor(name=f"__{name}_worker_{index}__")
            class _Worker:
                def __init__(self):
                    self._instance = handler(*args, **kwargs) if is_actor_cls else None

                async def receive(self, _msg):
                    try:
                        while True:
                            item, meta = await worker_queue.get()
                            token = current_message_var.set(meta)
                            try:
                                if exec_pool is not None:
                                    loop = asyncio.get_running_loop()
                                    call = functools.partial(handler, item, *args, **kwargs)
                                    result = await loop.run_in_executor(exec_pool, call)
                                    if meta is not None and meta.reply_to is not None:
                                        await meta.reply(result)
                                elif is_actor_cls:
                                    await _dispatch_to_instance(self._instance, item)
                                else:
                                    await handler(item, *args, **kwargs)
                            except Exception as exc:
                                node._on_core_event({
                                    "kind": "pool_worker_error",
                                    "pool": name,
                                    "detail": f"{type(exc).__name__}: {exc}",
                                })
                            finally:
                                current_message_var.reset(token)
                    finally:
                        if exec_pool is not None:
                            exec_pool.shutdown(wait=False, cancel_futures=True)

            return _Worker

        dispatcher_ref = self.spawn(_Dispatcher)
        worker_refs = []
        for i in range(n_workers):
            ref = self.spawn(_build_worker(i))
            await ref.send({"start": True})
            worker_refs.append(ref)
        return PoolRef(dispatcher_ref, worker_refs, supervisor=self)

    def spawn_supervisor(self, name, build, *, strategy="one_for_one",
                          max_restarts=3, restart_window=5.0):
        """Spawns a nested `Supervisor` as a child of this one, forming a
        supervision tree. Non-blocking.

        `build(child_supervisor)` populates the fresh child — call
        `.spawn()`/`.spawn_supervisor()` on it — and may be a sync or
        async callable. It runs again every time this subtree restarts
        (see `_SupervisorChild`'s docstring for why a fresh instance is
        built each time rather than reusing one).

        `name` identifies this subtree for `on_event(kind=
        "supervisor_gave_up")`'s `actor` field if it ever gives up — it
        isn't an actor name and isn't registered with the node.
        """
        node = self.node
        child = _SupervisorChild(name, build, strategy, max_restarts, restart_window)
        self._children.append(child)
        loop = asyncio.get_running_loop()
        child.task = loop.create_task(self._run_supervisor_child(child))
        node._register_task(child.task)
        return SupervisorRef(child)

    async def shutdown(self):
        """Cancels every child this `Supervisor` spawned (actors and
        nested Supervisors alike) and waits for them to stop. Safe to call
        more than once. Children spawned by a different `Supervisor` on
        the same `Node` are unaffected — to stop everything on a node
        regardless of which `Supervisor` spawned it, use `Node.stop()`.
        """
        await self._shutdown_except(None)

    async def _shutdown_except(self, exclude):
        # `exclude` lets a give-up in progress (already unwinding via its
        # own `raise`, from inside its own task) tear down every *other*
        # child without also cancelling-and-awaiting itself, which would
        # deadlock: a task can't finish while it's awaiting on itself.
        children = [c for c in self._children if c is not exclude]
        for c in children:
            if c.task is not None:
                c.task.cancel()
        tasks = [c.task for c in children if c.task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _discard_child(self, child):
        try:
            self._children.remove(child)
        except ValueError:
            pass

    def _affected_children(self, child):
        if self.strategy == "one_for_one":
            return [child]
        if self.strategy == "one_for_all":
            return list(self._children)
        # rest_for_one: the crashed child and everything spawned after it.
        i = self._children.index(child)
        return self._children[i:]

    def _respawn_child(self, child):
        """Replaces `child.task` with a brand-new one. Only used for a
        *sibling* swept into a group restart it didn't cause itself — the
        crashed child's own restart-in-place is handled by its own loop
        `continue`ing, keeping the same task throughout."""
        loop = asyncio.get_running_loop()
        if isinstance(child, _ActorChild):
            child.driver = None
            child.task = loop.create_task(self._run_actor_child(child))
        else:
            child.task = loop.create_task(self._run_supervisor_child(child))
        self.node._register_task(child.task)

    async def _teardown_sibling(self, child):
        if child.task is not None and not child.task.done():
            child.task.cancel()
            try:
                await child.task
            except BaseException:
                pass

    async def _handle_child_crash(self, child, exc):
        """Called from `child`'s own persistent loop when it crashes.
        Returns `True` if `child` should restart in place (its caller
        should `continue` its own loop), `False` if it should propagate
        `exc` and stop for good. As a side effect, tears down and
        restarts *other* affected children per `strategy`."""
        if not self._allow_restart():
            self.node._on_core_event({
                "kind": "supervisor_gave_up",
                "actor": child.name,
                "detail": f"{type(exc).__name__}: {exc}",
            })
            if self.strategy == "one_for_one":
                # Only the exhausted child itself gives up. Preserves the
                # pre-existing worker-pool pattern (many independent
                # actors spawned over this Supervisor's life; one dying
                # for good must not take unrelated siblings down).
                self._discard_child(child)
                if not self._children:
                    # Nothing left under this Supervisor — it has no more
                    # work, so it's "given up" too, for a parent
                    # Supervisor's SupervisorRef.task to observe.
                    self._give_up_exc = exc
                    self._given_up.set()
                return False
            # one_for_all / rest_for_one: exceeding the shared restart
            # budget means this group can no longer be kept consistent —
            # the whole subtree goes down, propagating to whatever
            # supervises it.
            await self._shutdown_except(child)
            self._give_up_exc = exc
            self._given_up.set()
            return False
        await asyncio.sleep(self._backoff())
        # Restart the *other* affected children (not `child` itself — its
        # own caller will `continue` its own loop in place).
        siblings = [c for c in self._affected_children(child) if c is not child]
        for c in siblings:
            await self._teardown_sibling(c)
        for c in siblings:
            self._respawn_child(c)
        return True

    async def _wait_for_give_up(self):
        await self._given_up.wait()
        return self._give_up_exc

    async def _run_actor_child(self, child):
        while True:
            try:
                # Constructing the actor is inside the try/except: a bug
                # in __init__ (on the first spawn, or on any later
                # restart) must go through the same restart/backoff/
                # give-up path as a bug in a handler, instead of silently
                # killing this task with nothing to show for it but
                # asyncio's generic "exception was never retrieved"
                # warning.
                instance = child.actor_cls(*child.args, **child.kwargs)
                self.node.register_actor(child.name, child.mailbox)
                self.node._live_children[child.name] = child
                self_ref = ActorRef(self.node, child.name, child=child)
                child.driver = asyncio.create_task(
                    self._drive(instance, child.mailbox, self_ref=self_ref)
                )
                await child.driver
            except asyncio.CancelledError:
                self.node.unregister_actor(child.name)
                self.node._live_children.pop(child.name, None)
                # Links don't survive a restart either way (see
                # lapinbeam.links) — always cleared now; only notified
                # below if this generation isn't coming back.
                linked = self.node._clear_links(child.name)
                monitored = self.node._clear_monitors(child.name)
                self.node._clear_groups(child.name)
                self.node._clear_registry(child.name)
                if child.pending_exit is not None:
                    # A link-triggered kill (see lapinbeam.links), not a
                    # real shutdown — route it through the normal
                    # crash/restart path so this Supervisor decides
                    # restart-or-give-up with the same budget and backoff
                    # as any other crash.
                    exc, child.pending_exit = child.pending_exit, None
                    if await self._handle_child_crash(child, exc):
                        continue
                    reason = f"{type(exc).__name__}: {exc}"
                    self.node._notify_links(child.name, linked, reason)
                    self.node._notify_monitors(child.name, monitored, reason)
                    raise exc
                self.node._notify_links(child.name, linked, "shutdown")
                self.node._notify_monitors(child.name, monitored, "shutdown")
                raise
            except Exception as exc:
                self.node.unregister_actor(child.name)
                self.node._live_children.pop(child.name, None)
                linked = self.node._clear_links(child.name)
                monitored = self.node._clear_monitors(child.name)
                self.node._clear_groups(child.name)
                self.node._clear_registry(child.name)
                # `mailbox` is deliberately never replaced here: the
                # message that caused this crash is already gone
                # (dequeued before the handler ran), but any messages
                # sent right after it — still sitting in this same queue
                # when the crash happened — must survive the restart.
                if await self._handle_child_crash(child, exc):
                    continue
                reason = f"{type(exc).__name__}: {exc}"
                self.node._notify_links(child.name, linked, reason)
                self.node._notify_monitors(child.name, monitored, reason)
                raise
            else:
                self.node.unregister_actor(child.name)
                self.node._live_children.pop(child.name, None)
                linked = self.node._clear_links(child.name)
                monitored = self.node._clear_monitors(child.name)
                self.node._clear_groups(child.name)
                self.node._clear_registry(child.name)
                self.node._notify_links(child.name, linked, "normal")
                self.node._notify_monitors(child.name, monitored, "normal")
                self._discard_child(child)
                return

    async def _run_supervisor_child(self, child):
        while True:
            child.supervisor = Supervisor(
                child.strategy, node=self.node,
                max_restarts=child.max_restarts, restart_window=child.restart_window,
            )
            try:
                result = child.build(child.supervisor)
                if inspect.isawaitable(result):
                    await result
                exc = await child.supervisor._wait_for_give_up()
            except asyncio.CancelledError:
                await child.supervisor.shutdown()
                raise
            except Exception as build_exc:
                exc = build_exc
            if await self._handle_child_crash(child, exc):
                continue
            raise exc

    @staticmethod
    async def _drive(instance, mailbox, self_ref=None):
        meta = type(instance).__lapinbeam_actor__
        handlers = meta["handlers"]
        default_handler = meta["default_handler"]
        # Bound once for this actor generation's whole lifetime (unlike
        # current_message_var below, reset on every message) — see
        # current_actor_ref()'s docstring.
        actor_token = current_actor_var.set(self_ref)
        try:
            while True:
                msg, msg_meta = await mailbox.get()
                token = current_message_var.set(msg_meta)
                try:
                    if not handlers:
                        await instance.receive(msg)
                        continue
                    handler_name = handlers.get(type(msg), default_handler)
                    if handler_name is None:
                        raise TypeError(
                            f"{type(instance).__name__} has no @on handler for "
                            f"{type(msg).__name__} messages and no @on(default=True) fallback"
                        )
                    await getattr(instance, handler_name)(msg)
                finally:
                    current_message_var.reset(token)
        finally:
            current_actor_var.reset(actor_token)

    def _allow_restart(self):
        now = time.monotonic()
        self._restart_times = [t for t in self._restart_times
                               if now - t < self.restart_window]
        if len(self._restart_times) >= self.max_restarts:
            return False
        self._restart_times.append(now)
        return True

    def _backoff(self):
        return min(2 ** len(self._restart_times) * 0.05, 1.0)
