"""Durable checkpoint store on SQLite (stdlib). Survives a 'crash' because state lives in the file."""

from __future__ import annotations

import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, saga TEXT, input TEXT, status TEXT, error TEXT,
  lease_owner TEXT, lease_until REAL);
CREATE TABLE IF NOT EXISTS steps (
  run_id TEXT, step TEXT, status TEXT, output TEXT, PRIMARY KEY (run_id, step));
CREATE TABLE IF NOT EXISTS inbox (event_id TEXT PRIMARY KEY, run_id TEXT, received_at REAL);
"""


class Store:
    def __init__(self, path: str):
        self.path = path
        self.db = sqlite3.connect(
            path, timeout=10, isolation_level=None, check_same_thread=False
        )
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)

    def close(self):
        self.db.close()

    # --- runs -----------------------------------------------------------
    def create_run(self, run_id: str, saga: str, input_: dict) -> bool:
        cur = self.db.execute(
            "INSERT OR IGNORE INTO runs (run_id, saga, input, status) VALUES (?,?,?,'running')",
            (run_id, saga, json.dumps(input_)),
        )
        return cur.rowcount == 1

    def get_run(self, run_id: str) -> dict | None:
        row = self.db.execute(
            "SELECT run_id, saga, input, status, error FROM runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "saga": row[1],
            "input": json.loads(row[2]),
            "status": row[3],
            "error": row[4],
        }

    def set_status(self, run_id: str, status: str, error: str | None = None):
        self.db.execute(
            "UPDATE runs SET status=?, error=COALESCE(?, error) WHERE run_id=?",
            (status, error, run_id),
        )

    def claim(
        self, run_id: str, owner: str, ttl: float = 30.0, now: float | None = None
    ) -> bool:
        """Atomic lease so two workers never drive the same run at once."""
        now = time.time() if now is None else now
        cur = self.db.execute(
            "UPDATE runs SET lease_owner=?, lease_until=? WHERE run_id=? AND "
            "(lease_owner IS NULL OR lease_owner=? OR lease_until < ?)",
            (owner, now + ttl, run_id, owner, now),
        )
        return cur.rowcount == 1

    def release(self, run_id: str, owner: str):
        self.db.execute(
            "UPDATE runs SET lease_owner=NULL, lease_until=NULL WHERE run_id=? AND lease_owner=?",
            (run_id, owner),
        )

    # --- steps ----------------------------------------------------------
    def steps(self, run_id: str) -> dict[str, dict]:
        rows = self.db.execute(
            "SELECT step, status, output FROM steps WHERE run_id=?", (run_id,)
        ).fetchall()
        return {
            r[0]: {"status": r[1], "output": json.loads(r[2]) if r[2] else None}
            for r in rows
        }

    def mark(self, run_id: str, step: str, status: str, output=None):
        self.db.execute(
            "INSERT INTO steps (run_id, step, status, output) VALUES (?,?,?,?) "
            "ON CONFLICT(run_id, step) DO UPDATE SET status=excluded.status, "
            "output=COALESCE(excluded.output, steps.output)",
            (run_id, step, status, json.dumps(output) if output is not None else None),
        )

    # --- inbox (dedupe of inbound events) -------------------------------
    def accept_event(self, event_id: str, run_id: str) -> bool:
        cur = self.db.execute(
            "INSERT OR IGNORE INTO inbox (event_id, run_id, received_at) VALUES (?,?,?)",
            (event_id, run_id, time.time()),
        )
        return cur.rowcount == 1
