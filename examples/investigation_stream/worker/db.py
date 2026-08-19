"""Postgres access — a plain asyncpg pool, no ORM. `investigations` is the
single source of truth: the SSE endpoint reconstructs history from it on
every (re)connect, and the pubsub (`pubsub.py`) only carries *live* deltas
on top of it while a client happens to be connected.
"""

import json
import os

import asyncpg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://investigator:investigator@postgres:5432/investigations"
)

_pool = None


async def connect():
    global _pool
    # A handful of connections is plenty: each investigation only touches
    # Postgres for a brief UPDATE between AI-simulation steps, never holds
    # a connection for the ~1-3s "thinking" time — see investigation.py.
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS investigations (
                id UUID PRIMARY KEY,
                status TEXT NOT NULL,
                steps JSONB NOT NULL DEFAULT '[]',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )


async def disconnect():
    if _pool is not None:
        await _pool.close()


async def create_investigation(investigation_id):
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO investigations (id, status, steps) VALUES ($1, 'en_progreso', '[]')",
            investigation_id,
        )


async def append_step(investigation_id, step):
    """Appends one completed step to the JSONB array and bumps updated_at.
    Fetches-then-writes under the connection's own statement, no separate
    read round-trip — `steps || $2::jsonb` appends server-side."""
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE investigations
            SET steps = steps || $2::jsonb, updated_at = now()
            WHERE id = $1
            """,
            investigation_id,
            json.dumps([step]),
        )


async def mark_status(investigation_id, status):
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE investigations SET status = $2, updated_at = now() WHERE id = $1",
            investigation_id,
            status,
        )


async def get_investigation(investigation_id):
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status, steps, created_at, updated_at FROM investigations WHERE id = $1",
            investigation_id,
        )
    if row is None:
        return None
    return {
        "id": str(row["id"]),
        "status": row["status"],
        "steps": json.loads(row["steps"]),
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }
