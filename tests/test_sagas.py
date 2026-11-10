import random
import threading

import pytest
from conftest import order

from sagas.engine import Crash
from sagas.store import Store

OPS = ["reserve", "charge", "ship", "email:order_confirmation"]


def assert_completed_once(w, n=1, amount=2500):
    c = f"c{n}"
    assert w.payments.charge_count(c) == 1 and w.payments.net(c) == amount
    assert w.email.count(c, "order_confirmation") == 1
    assert w.email.count(c, "refund_notice") == 0
    assert sum(1 for s in w.shipping.shipments.values() if s["order"] == f"o{n}") == 1


def assert_rolled_back(w, n=1):
    c = f"c{n}"
    assert w.payments.net(c) == 0
    assert w.email.count(c, "order_confirmation") == 0
    assert w.email.count(c, "refund_notice") == (1 if w.payments.charge_count(c) else 0)
    assert all(not r["active"] for r in w.inventory.reservations.values())
    assert w.inventory.stock["widget"] == 1000


def test_happy_path(env):
    assert env.engine().handle_event(order()) == "completed"
    assert_completed_once(env.world)
    assert env.world.inventory.stock["widget"] == 999


@pytest.mark.parametrize("op", OPS)
def test_crash_after_effect_before_checkpoint(env, op):
    env.world.__dict__[{"reserve": "inventory", "charge": "payments", "ship": "shipping"}.get(op, "email")].inject(op, "crash_after")
    with pytest.raises(Crash):
        env.engine().handle_event(order())
    assert env.restart().handle_event(order()) == "completed"  # broker redelivers
    assert_completed_once(env.world)


def test_naive_baseline_double_charges(env):
    """Same crash, no idempotency keys: the retry charges the card twice."""
    env.world.payments.inject("charge", "crash_after")
    with pytest.raises(Crash):
        env.engine(idempotent=False).handle_event(order())
    env.restart(idempotent=False).handle_event(order())
    assert env.world.payments.charge_count("c1") == 2


def test_crash_before_effect(env):
    env.world.payments.inject("charge", "crash_before")
    with pytest.raises(Crash):
        env.engine().handle_event(order())
    assert env.restart().resume("order:o1") == "completed"
    assert_completed_once(env.world)


def test_transient_errors_are_retried(env):
    env.world.payments.inject("charge", "transient", "transient")
    eng = env.engine()
    assert eng.handle_event(order()) == "completed"
    assert eng.retries == 2
    assert_completed_once(env.world)


def test_retries_exhausted_compensates(env):
    env.world.shipping.inject("ship", *["transient"] * 4)
    assert env.engine(max_retries=3).handle_event(order()) == "compensated"
    assert_rolled_back(env.world)


def test_partial_failure_ship_rejected(env):
    assert env.engine().handle_event(order(address="")) == "compensated"
    assert_rolled_back(env.world)
    assert env.world.email.count("c1", "refund_notice") == 1


def test_first_step_fails_nothing_to_undo(env):
    assert env.engine().handle_event(order(qty=5000)) == "compensated"
    assert env.world.payments.charge_count("c1") == 0
    assert_rolled_back(env.world)


def test_crash_during_compensation(env):
    env.world.payments.inject("refund", "crash_after")
    with pytest.raises(Crash):
        env.engine().handle_event(order(address=""))
    assert env.restart().resume("order:o1") == "compensated"
    assert env.world.payments.charges["ch_1"]["refunded"] == 2500  # refunded once, not twice
    assert_rolled_back(env.world)


def test_duplicate_delivery(env):
    eng = env.engine()
    assert eng.handle_event(order()) == "completed"
    assert eng.handle_event(order()) == "completed"
    assert eng.duplicates == 1
    assert_completed_once(env.world)


def test_concurrent_duplicate_delivery_uses_lease(env):
    env.world.payments.inject("charge", "transient")  # widen the race window
    results = []
    barrier = threading.Barrier(2)

    def worker():
        e = env.engine()
        barrier.wait()
        results.append(e.handle_event(order()))
    ts = [threading.Thread(target=worker) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert "completed" in results
    assert env.world.payments.charge_count("c1") == 1


def test_lease_blocks_until_expiry(env):
    env.world.payments.inject("charge", "crash_after")
    with pytest.raises(Crash):
        env.engine().handle_event(order())
    assert env.engine().resume("order:o1") == "locked"  # dead worker's lease still valid
    env.clock.t += 31
    assert env.engine().resume("order:o1") == "completed"


def test_checkpoints_are_durable(env):
    env.world.shipping.inject("ship", "crash_before")
    with pytest.raises(Crash):
        env.engine().handle_event(order())
    steps = Store(env.db).steps("order:o1")
    assert {k: v["status"] for k, v in steps.items()} == {"reserve": "done", "charge": "done"}
    assert steps["charge"]["output"] == {"charge_id": "ch_1"}


def test_compensation_failure_marks_stuck(env):
    env.world.payments.inject("refund", "permanent")
    assert env.engine().handle_event(order(address="")) == "stuck"
    run = Store(env.db).get_run("order:o1")
    assert run["status"] == "stuck" and "refund" in run["error"]


def test_randomised_faults_hold_invariants(env):
    rng = random.Random(7)
    faults = ["transient", "crash_after", "crash_before", None]
    for n in range(1, 41):
        svc = rng.choice([("inventory", "reserve"), ("payments", "charge"), ("shipping", "ship"),
                          ("email", "email:order_confirmation")])
        f = rng.choice(faults)
        if f:
            getattr(env.world, svc[0]).inject(svc[1], f)
        ev = order(n, address=rng.choice(["1 Main St", "1 Main St", ""]))
        for _ in range(5):  # redeliver until terminal
            try:
                st = env.restart().handle_event(ev)
                if st in ("completed", "compensated"):
                    break
            except Crash:
                continue
        if st == "completed":
            assert_completed_once(env.world, n)
        else:
            assert env.world.payments.net(f"c{n}") == 0
            assert env.world.email.count(f"c{n}", "order_confirmation") == 0
