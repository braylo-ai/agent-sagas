"""Mock external services (payments, inventory, shipping, email) with idempotency keys and fault injection.

Each service behaves like a Stripe-style API: a request carrying an idempotency key that was seen
before returns the stored response and has no new side effect. Faults are queued per operation:
  "transient"    raise TransientError before the effect
  "permanent"    raise StepFailed before the effect
  "crash_before" raise Crash before the effect (worker died before the request landed)
  "crash_after"  apply the effect, then raise Crash (worker died before it saw the response)
"""

from __future__ import annotations

import itertools
from collections import defaultdict, deque

from .engine import Crash, StepFailed, TransientError


class Service:
    def __init__(self):
        self.responses: dict[str, object] = {}
        self.faults: dict[str, deque] = defaultdict(deque)
        self.requests = 0
        self._ids = itertools.count(1)

    def inject(self, op: str, *faults: str):
        self.faults[op].extend(faults)

    def _call(self, op: str, key: str, effect):
        self.requests += 1
        if key in self.responses:
            return self.responses[key]
        fault = self.faults[op].popleft() if self.faults[op] else None
        if fault == "transient":
            raise TransientError(f"{op} timeout")
        if fault == "permanent":
            raise StepFailed(f"{op} rejected")
        if fault == "crash_before":
            raise Crash(f"crash before {op}")
        result = effect()
        self.responses[key] = result
        if fault == "crash_after":
            raise Crash(f"crash after {op}")
        return result


class Payments(Service):
    def __init__(self):
        super().__init__()
        self.charges: dict[str, dict] = {}

    def charge(self, customer: str, amount: int, key: str) -> dict:
        def eff():
            cid = f"ch_{next(self._ids)}"
            self.charges[cid] = {
                "id": cid,
                "customer": customer,
                "amount": amount,
                "refunded": 0,
            }
            return {"charge_id": cid}

        return self._call("charge", key, eff)

    def refund(self, charge_id: str, key: str) -> dict:
        def eff():
            c = self.charges[charge_id]
            c["refunded"] += c["amount"]
            return {"refunded": c["amount"]}

        return self._call("refund", key, eff)

    def net(self, customer: str) -> int:
        return sum(
            c["amount"] - c["refunded"]
            for c in self.charges.values()
            if c["customer"] == customer
        )

    def charge_count(self, customer: str) -> int:
        return sum(1 for c in self.charges.values() if c["customer"] == customer)


class Inventory(Service):
    def __init__(self, stock: dict[str, int]):
        super().__init__()
        self.stock = dict(stock)
        self.reservations: dict[str, dict] = {}

    def reserve(self, sku: str, qty: int, key: str) -> dict:
        def eff():
            if self.stock.get(sku, 0) < qty:
                raise StepFailed(f"out of stock: {sku}")
            self.stock[sku] -= qty
            rid = f"rs_{next(self._ids)}"
            self.reservations[rid] = {"sku": sku, "qty": qty, "active": True}
            return {"reservation_id": rid}

        return self._call("reserve", key, eff)

    def release(self, rid: str, key: str) -> dict:
        def eff():
            r = self.reservations[rid]
            if r["active"]:
                r["active"] = False
                self.stock[r["sku"]] += r["qty"]
            return {"released": rid}

        return self._call("release", key, eff)


class Shipping(Service):
    def __init__(self):
        super().__init__()
        self.shipments: dict[str, dict] = {}

    def create(self, order_id: str, address: str, key: str) -> dict:
        def eff():
            if not address:
                raise StepFailed("invalid address")
            sid = f"sh_{next(self._ids)}"
            self.shipments[sid] = {"order": order_id, "cancelled": False}
            return {"shipment_id": sid}

        return self._call("ship", key, eff)

    def cancel(self, sid: str, key: str) -> dict:
        def eff():
            self.shipments[sid]["cancelled"] = True
            return {"cancelled": sid}

        return self._call("cancel", key, eff)


class Email(Service):
    def __init__(self):
        super().__init__()
        self.outbox: list[dict] = []

    def send(self, to: str, template: str, key: str) -> dict:
        def eff():
            self.outbox.append({"to": to, "template": template})
            return {"message_id": f"m_{len(self.outbox)}"}

        return self._call(f"email:{template}", key, eff)

    def count(self, to: str, template: str) -> int:
        return sum(
            1 for m in self.outbox if m["to"] == to and m["template"] == template
        )
