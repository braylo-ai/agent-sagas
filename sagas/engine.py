"""Saga engine: checkpointed steps, compensating actions, idempotency keys, retries, leases."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Callable

from .store import Store


class Crash(BaseException):
    """Simulates the worker process dying. Not caught by the engine (BaseException)."""


class StepFailed(Exception):
    """Permanent business failure: triggers compensation."""


class TransientError(Exception):
    """Retryable failure (timeout, 503)."""


@dataclass
class Step:
    name: str
    action: Callable[[dict, str], dict]
    compensate: Callable[[dict, str], None] | None = None


class SagaEngine:
    def __init__(
        self,
        store: Store,
        saga: str,
        steps: list[Step],
        max_retries: int = 3,
        idempotent: bool = True,
        owner: str | None = None,
        sleep: Callable[[float], None] = lambda s: None,
        clock: Callable[[], float] | None = None,
        lease_ttl: float = 30.0,
    ):
        self.store = store
        self.saga = saga
        self.steps = steps
        self.max_retries = max_retries
        self.idempotent = idempotent  # False = naive baseline: fresh key per attempt
        self.owner = owner or uuid.uuid4().hex[:8]
        self.sleep = sleep
        self.retries = 0
        self.duplicates = 0
        self.clock = clock
        self.lease_ttl = lease_ttl

    def key(self, run_id: str, step: str, phase: str = "do") -> str:
        if not self.idempotent:
            return uuid.uuid4().hex
        return f"{run_id}:{step}:{phase}"

    def _retry(self, fn):
        for attempt in range(self.max_retries + 1):
            try:
                return fn()
            except TransientError:
                if attempt == self.max_retries:
                    raise
                self.retries += 1
                self.sleep(
                    0.05 * 2**attempt
                )  # exponential backoff (no-op sleeper in tests)

    # --- public API -----------------------------------------------------
    def start(self, run_id: str, input_: dict) -> str:
        self.store.create_run(run_id, self.saga, input_)
        return self.resume(run_id)

    def handle_event(self, event: dict) -> str:
        """At-least-once delivery in, exactly-once effects out: dedupe by event id, deterministic run id."""
        run_id = f"{self.saga}:{event['order_id']}"
        if not self.store.accept_event(event["event_id"], run_id):
            # Redelivery (e.g. we crashed before acking). Never start a second run; just make
            # sure the existing one reaches a terminal state.
            self.duplicates += 1
            if self.store.get_run(run_id) is None:
                self.store.create_run(run_id, self.saga, event)
            return self.resume(run_id)
        return self.start(run_id, event)

    def resume(self, run_id: str) -> str:
        now = self.clock() if self.clock else None
        if not self.store.claim(run_id, self.owner, self.lease_ttl, now):
            return "locked"
        try:
            status = self._drive(run_id)
        except Crash:
            raise  # a dead process runs no cleanup; its lease must expire
        except BaseException:
            self.store.release(run_id, self.owner)
            raise
        self.store.release(run_id, self.owner)
        return status

    # --- internals ------------------------------------------------------
    def _ctx(self, run_id: str) -> tuple[dict, dict]:
        run = self.store.get_run(run_id)
        done = self.store.steps(run_id)
        return run, {
            "input": run["input"],
            "out": {k: v["output"] for k, v in done.items()},
        }

    def _drive(self, run_id: str) -> str:
        run, ctx = self._ctx(run_id)
        if run["status"] in ("completed", "compensated", "stuck"):
            return run["status"]
        if run["status"] == "compensating":
            return self._compensate(run_id)
        done = self.store.steps(run_id)
        for step in self.steps:
            if done.get(step.name, {}).get("status") == "done":
                continue
            try:
                out = self._retry(lambda: step.action(ctx, self.key(run_id, step.name)))
            except (StepFailed, TransientError) as e:
                self.store.mark(run_id, step.name, "failed")
                self.store.set_status(run_id, "compensating", f"{step.name}: {e}")
                return self._compensate(run_id)
            self.store.mark(
                run_id, step.name, "done", out
            )  # checkpoint AFTER the effect
            ctx["out"][step.name] = out
        self.store.set_status(run_id, "completed")
        return "completed"

    def _compensate(self, run_id: str) -> str:
        _, ctx = self._ctx(run_id)
        done = self.store.steps(run_id)
        for step in reversed(self.steps):
            st = done.get(step.name, {}).get("status")
            if st != "done" or step.compensate is None:
                continue
            try:
                self._retry(
                    lambda: step.compensate(ctx, self.key(run_id, step.name, "undo"))
                )
            except (StepFailed, TransientError) as e:
                self.store.set_status(run_id, "stuck", f"compensation {step.name}: {e}")
                return "stuck"
            self.store.mark(run_id, step.name, "compensated")
        self.store.set_status(run_id, "compensated")
        return "compensated"
