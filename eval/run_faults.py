"""Fault-injection campaign: idempotent saga engine vs a naive baseline (fresh key per attempt).

For each order a random fault is injected into a random operation; the 'broker' redelivers the
event to a freshly restarted worker until the run is terminal. Writes results/fault_campaign.json.
"""
import json
import random
import sys
import tempfile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sagas.engine import Crash  # noqa: E402
from sagas.order import World, make_engine  # noqa: E402
from sagas.store import Store  # noqa: E402

OPS = [("inventory", "reserve"), ("payments", "charge"), ("shipping", "ship"),
       ("email", "email:order_confirmation"), ("payments", "refund"), ("inventory", "release")]
FAULTS = ["crash_after", "crash_before", "transient", "transient_x5", "duplicate_delivery", None]


def campaign(idempotent: bool, n: int, seed: int) -> dict:
    rng = random.Random(seed)
    w = World()
    t = [0.0]
    db = tempfile.mktemp(suffix=".db")
    outcomes, faults_used = Counter(), Counter()
    stuck_wrong_net = stuck_held = 0
    dup_charges = dup_confirm = dup_refund_notice = wrong_net = orphan_reservations = 0
    for i in range(1, n + 1):
        svc, op = rng.choice(OPS)
        fault = rng.choice(FAULTS)
        faults_used[fault or "none"] += 1
        if fault == "transient_x5":
            getattr(w, svc).inject(op, *["transient"] * 5)
        elif fault and fault != "duplicate_delivery":
            getattr(w, svc).inject(op, fault)
        ev = {"event_id": f"e{i}", "order_id": f"o{i}", "customer": f"c{i}", "amount": 1000 + i,
              "sku": "widget", "qty": 1, "address": "" if rng.random() < 0.2 else "1 Main St"}
        deliveries = 2 if fault == "duplicate_delivery" else 1
        status, attempts = None, 0
        while attempts < 10 and (deliveries > 0 or status not in ("completed", "compensated", "stuck")):
            attempts += 1
            deliveries -= 1
            t[0] += 60
            eng = make_engine(Store(db), w, idempotent=idempotent, clock=lambda: t[0])
            try:
                status = eng.handle_event(ev)
            except Crash:
                status = "crashed"
        outcomes[status] += 1
        c = f"c{i}"
        dup_charges += max(0, w.payments.charge_count(c) - 1)
        dup_confirm += max(0, w.email.count(c, "order_confirmation") - 1)
        dup_refund_notice += max(0, w.email.count(c, "refund_notice") - 1)
        expected = ev["amount"] if status == "completed" else 0
        if w.payments.net(c) != expected:
            if status == "stuck":
                stuck_wrong_net += 1
            else:
                wrong_net += 1
        if status == "stuck":
            res = (Store(db).steps(f"order:o{i}").get("reserve") or {}).get("output")
            if res and w.inventory.reservations[res["reservation_id"]]["active"]:
                stuck_held += 1
        for svc, op in OPS:  # clear unconsumed faults so they don't leak into the next order
            getattr(w, svc).faults[op].clear()
    active = sum(r["active"] for r in w.inventory.reservations.values())
    orphan_reservations = active - outcomes["completed"] - stuck_held
    return {"idempotent": idempotent, "orders": n, "seed": seed, "outcomes": dict(outcomes),
            "faults_injected": dict(faults_used), "duplicate_charges": dup_charges,
            "duplicate_confirmation_emails": dup_confirm, "duplicate_refund_notices": dup_refund_notice,
            "silent_wrong_net_charge": wrong_net, "orphan_reservations": orphan_reservations,
            "stuck_runs_flagged_for_operator": outcomes["stuck"], "stuck_runs_with_charge_outstanding": stuck_wrong_net,
            "stuck_runs_with_reservation_held": stuck_held}


if __name__ == "__main__":
    out = {"saga_engine": campaign(True, 500, 42), "naive_baseline": campaign(False, 500, 42)}
    (ROOT / "results").mkdir(exist_ok=True)
    (ROOT / "results" / "fault_campaign.json").write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))
