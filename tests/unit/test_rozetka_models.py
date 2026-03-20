"""
Unit tests — app/models/rozetka.py  (Pydantic validation of Rozetka API responses)
"""
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.models.rozetka import RozetkaOrder, RozetkaOrderItem, RozetkaOrdersPage


def _valid_order(**overrides):
    base = dict(
        id=248888186,
        market_id=55,
        created="2026-01-10 09:30:00",
        changed="2026-01-11 14:00:00",
        amount="640.00",
        cost_with_discount="640.00",
        status=6,
        status_group=2,
        total_quantity=2,
    )
    base.update(overrides)
    return base


def test_valid_order_parses():
    order = RozetkaOrder(**_valid_order())
    assert order.id == 248888186
    assert order.status_group == 2
    assert order.cost_with_discount == Decimal("640.00")


def test_missing_id_raises():
    data = _valid_order()
    del data["id"]
    with pytest.raises(ValidationError):
        RozetkaOrder(**data)


def test_empty_purchases_defaults_to_empty_list():
    order = RozetkaOrder(**_valid_order(purchases=[]))
    assert order.purchases == []


def test_purchase_item_parses():
    item = RozetkaOrderItem(
        id=101,
        item_id=9999,
        item_name="Harry Potter and the Philosopher's Stone",
        quantity=1,
        price=Decimal("320.00"),
        cost=Decimal("320.00"),
        cost_with_discount=Decimal("320.00"),
    )
    assert item.item_name == "Harry Potter and the Philosopher's Stone"
    assert item.price == Decimal("320.00")
    assert item.quantity == 1


def test_decimal_coercion_from_string():
    order = RozetkaOrder(**_valid_order(cost_with_discount="999.99"))
    assert order.cost_with_discount == Decimal("999.99")


def test_null_delivery_is_none():
    order = RozetkaOrder(**_valid_order(delivery=None))
    assert order.delivery is None


def test_orders_page_defaults():
    page = RozetkaOrdersPage()
    assert page.orders == []
    assert page.page_count == 1
    assert page.current_page == 1
    assert page.per_page == 20

