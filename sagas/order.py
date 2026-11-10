"""Example saga an agent would run for a customer order: reserve -> charge -> ship -> confirm."""

from __future__ import annotations

from dataclasses import dataclass, field

from .engine import SagaEngine, Step
from .services import Email, Inventory, Payments, Shipping
from .store import Store


@dataclass
class World:
    payments: Payments = field(default_factory=Payments)
    inventory: Inventory = field(default_factory=lambda: Inventory({"widget": 1000}))
    shipping: Shipping = field(default_factory=Shipping)
    email: Email = field(default_factory=Email)


def order_steps(w: World) -> list[Step]:
    def reserve(ctx, key):
        i = ctx["input"]
        return w.inventory.reserve(i["sku"], i["qty"], key)

    def release(ctx, key):
        w.inventory.release(ctx["out"]["reserve"]["reservation_id"], key)

    def charge(ctx, key):
        i = ctx["input"]
        return w.payments.charge(i["customer"], i["amount"], key)

    def refund(ctx, key):
        w.payments.refund(ctx["out"]["charge"]["charge_id"], key)
        w.email.send(ctx["input"]["customer"], "refund_notice", key + ":email")

    def ship(ctx, key):
        i = ctx["input"]
        return w.shipping.create(i["order_id"], i.get("address", ""), key)

    def cancel_ship(ctx, key):
        w.shipping.cancel(ctx["out"]["ship"]["shipment_id"], key)

    def confirm(ctx, key):
        return w.email.send(ctx["input"]["customer"], "order_confirmation", key)

    return [
        Step("reserve", reserve, release),
        Step("charge", charge, refund),
        Step("ship", ship, cancel_ship),
        Step("confirm", confirm, None),
    ]


def make_engine(store: Store, w: World, **kw) -> SagaEngine:
    return SagaEngine(store, "order", order_steps(w), **kw)
