import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sagas.order import World, make_engine  # noqa: E402
from sagas.store import Store  # noqa: E402


class Clock:
    def __init__(self):
        self.t = 1_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def env(tmp_path):
    class Env:
        def __init__(self):
            self.db = str(tmp_path / "sagas.db")
            self.world = World()
            self.clock = Clock()

        def engine(self, **kw):
            """A fresh worker process: new DB connection, new owner id."""
            return make_engine(Store(self.db), self.world, clock=self.clock, **kw)

        def restart(self, **kw):
            self.clock.t += 60  # old worker's 30 s lease has expired
            return self.engine(**kw)

    return Env()


def order(n=1, amount=2500, address="1 Main St", qty=1):
    return {"event_id": f"evt-{n}", "order_id": f"o{n}", "customer": f"c{n}",
            "amount": amount, "sku": "widget", "qty": qty, "address": address}
