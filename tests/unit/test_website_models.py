"""
Unit tests — app/models/website.py  (Pydantic validation of OpenCart data)
"""
from decimal import Decimal
from datetime import datetime

import pytest
from pydantic import ValidationError

from app.models.website import OcOrder, OcOrderProduct, OcProduct, OcCategory


class TestOcOrder:
    def _valid(self, **overrides):
        base = dict(
            order_id=1001,
            total="340.50",
            currency_code="UAH",
            currency_value="1.00000000",
            date_added=datetime(2026, 3, 1, 10, 0, 0),
            date_modified=datetime(2026, 3, 1, 10, 5, 0),
        )
        base.update(overrides)
        return base

    def test_valid_order_parses(self):
        order = OcOrder(**self._valid())
        assert order.order_id == 1001
        assert order.total == Decimal("340.50")
        assert order.currency_code == "UAH"

    def test_decimal_coercion_from_string(self):
        order = OcOrder(**self._valid(total="123.4567"))
        assert order.total == Decimal("123.4567")

    def test_missing_required_field_raises(self):
        data = self._valid()
        del data["order_id"]
        with pytest.raises(ValidationError):
            OcOrder(**data)

    def test_none_total_defaults_to_zero(self):
        order = OcOrder(**self._valid(total=None))
        assert order.total == Decimal("0")


class TestOcProduct:
    def test_valid_product_parses(self):
        product = OcProduct(
            product_id=42,
            isbn="978-0-7432-7356-5",
            author="J.K. Rowling",
            name="Harry Potter and the Philosopher's Stone",
            price="299.00",
            quantity=10,
        )
        assert product.isbn == "978-0-7432-7356-5"
        assert product.price == Decimal("299.00")

    def test_optional_fields_default_to_none(self):
        product = OcProduct(product_id=1)
        assert product.author is None
        assert product.main_category_id is None


class TestOcCategory:
    def test_top_level_category_has_parent_zero(self):
        cat = OcCategory(category_id=10, parent_id=0, name="Children's Books")
        assert cat.parent_id == 0
        assert cat.name == "Children's Books"

    def test_child_category(self):
        cat = OcCategory(category_id=20, parent_id=10, name="Fiction")
        assert cat.parent_id == 10

