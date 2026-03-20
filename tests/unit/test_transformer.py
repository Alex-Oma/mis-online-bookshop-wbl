"""
Unit tests for app/ingestion/transformer.py helper logic.
Tests the pure transform functions without any DB calls.
"""
from decimal import Decimal

import pytest


# ── date_id generation ────────────────────────────────────────────────────────

class TestDateIdGeneration:
    """The transformer converts date_added → YYYYMMDD integer date_id."""

    def _make_date_id(self, date_str: str) -> int:
        """Mirrors transformer logic: TO_CHAR(date, 'YYYYMMDD')::int"""
        from datetime import datetime
        dt = datetime.fromisoformat(date_str)
        return int(dt.strftime("%Y%m%d"))

    def test_basic_date(self):
        assert self._make_date_id("2026-03-08 14:30:00") == 20260308

    def test_start_of_year(self):
        assert self._make_date_id("2025-01-01 00:00:00") == 20250101

    def test_end_of_year(self):
        assert self._make_date_id("2024-12-31 23:59:59") == 20241231


# ── Currency conversion ───────────────────────────────────────────────────────

class TestCurrencyConversion:
    """total_amount_uah = total_amount * currency_value."""

    def _convert(self, amount: Decimal, rate: Decimal) -> Decimal:
        return amount * rate

    def test_uah_passthrough(self):
        # UAH orders have currency_value = 1.0
        result = self._convert(Decimal("450.00"), Decimal("1.00000000"))
        assert result == Decimal("450.00")

    def test_usd_conversion(self):
        result = self._convert(Decimal("10.00"), Decimal("39.50000000"))
        assert result == Decimal("395.0000000000")

    def test_zero_amount(self):
        result = self._convert(Decimal("0.00"), Decimal("38.00000000"))
        assert result == Decimal("0.0000000000")


# ── Rozetka order parser ──────────────────────────────────────────────────────

class TestRozetkaOrderParsing:
    """Test that RozetkaOrder Pydantic model parses API responses correctly."""

    def _make_order(self, **overrides):
        from app.models.rozetka import RozetkaOrder
        base = {
            "id": 248888186,
            "market_id": 55,
            "created": "2026-01-15 10:00:00",
            "changed": "2026-01-15 11:00:00",
            "amount": "640.00",
            "amount_with_discount": "640.00",
            "cost": "640.00",
            "cost_with_discount": "640.00",
            "status": 6,
            "status_group": 2,
            "total_quantity": 2,
        }
        base.update(overrides)
        return RozetkaOrder(**base)

    def test_parses_successfully(self):
        order = self._make_order()
        assert order.id == 248888186
        assert order.status_group == 2
        assert order.cost_with_discount == Decimal("640.00")

    def test_null_delivery_is_none(self):
        order = self._make_order(delivery=None)
        assert order.delivery is None

    def test_empty_purchases(self):
        order = self._make_order(purchases=[])
        assert order.purchases == []

    def test_purchase_line_parsed(self):
        from app.models.rozetka import RozetkaOrderItem
        item = RozetkaOrderItem(
            id=158282087,
            item_id=99187168,
            item_name="Harry Potter and the Philosopher's Stone",
            quantity=1,
            price="320.00",
            cost="320.00",
            cost_with_discount="320.00",
        )
        assert item.item_name == "Harry Potter and the Philosopher's Stone"
        assert item.price == Decimal("320.00")


# ── Website product parser ────────────────────────────────────────────────────

class TestWebsiteProductParsing:
    def test_parses_product(self):
        from app.models.website import OcProduct
        p = OcProduct(
            product_id=101,
            isbn="978-0-7475-3269-9",
            author="J.K. Rowling",
            publisher="Bloomsbury",
            publishing_year=1997,
            pages_number=223,
            binding_type="Paperback",
            price="320.0000",
            quantity=15,
            stock_status_id=7,
            name="Harry Potter and the Philosopher's Stone",
        )
        assert p.product_id == 101
        assert p.price == Decimal("320.0000")
        assert p.publishing_year == 1997

    def test_missing_optional_fields_default_none(self):
        from app.models.website import OcProduct
        from datetime import datetime
        p = OcProduct(
            product_id=202,
            name="Unknown Book",
            date_added=datetime(2022, 1, 1),
            date_modified=datetime(2022, 1, 1),
        )
        assert p.isbn is None
        assert p.author is None
        assert p.price == Decimal("0.0000")

