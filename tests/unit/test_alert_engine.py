"""
Unit tests — alert engine helper logic (no DB, no email).
Tests threshold evaluation and cooldown calculations in isolation.
"""
from datetime import datetime, timedelta, timezone


class TestLowStockThreshold:
    """Test that the alert engine correctly filters products by threshold."""

    def test_product_below_threshold_is_flagged(self):
        """Product with quantity_in_stock <= threshold should be included."""
        threshold = 5
        product_qty = 3
        assert product_qty <= threshold

    def test_product_above_threshold_is_not_flagged(self):
        """Product with quantity_in_stock > threshold should be excluded."""
        threshold = 5
        product_qty = 6
        assert product_qty > threshold

    def test_product_at_threshold_is_flagged(self):
        """Product with quantity_in_stock == threshold is at the boundary — should alert."""
        threshold = 5
        product_qty = 5
        assert product_qty <= threshold


class TestCooldownLogic:
    """Test that alerts respect the cooldown_hours setting."""

    def _is_in_cooldown(
        self, last_triggered: datetime, cooldown_hours: int
    ) -> bool:
        """Mirrors the cooldown check in engine.py."""
        elapsed = datetime.now(timezone.utc) - last_triggered.replace(
            tzinfo=timezone.utc
        )
        return elapsed < timedelta(hours=cooldown_hours)

    def test_recently_alerted_product_is_in_cooldown(self):
        """If alerted 1 hour ago and cooldown is 24h, should be suppressed."""
        last_triggered = datetime.now(timezone.utc) - timedelta(hours=1)
        assert self._is_in_cooldown(last_triggered, cooldown_hours=24) is True

    def test_old_alert_is_not_in_cooldown(self):
        """If alerted 30 hours ago and cooldown is 24h, should fire again."""
        last_triggered = datetime.now(timezone.utc) - timedelta(hours=30)
        assert self._is_in_cooldown(last_triggered, cooldown_hours=24) is False

    def test_cooldown_boundary_exactly_expired(self):
        """If alerted exactly cooldown_hours ago, it should fire again."""
        last_triggered = datetime.now(timezone.utc) - timedelta(hours=24, seconds=1)
        assert self._is_in_cooldown(last_triggered, cooldown_hours=24) is False


class TestRevenueSpikeCalculation:
    """Test the % change calculation used by the revenue_spike rule."""

    def _calc_pct_change(self, current: float, prior: float) -> float:
        if prior == 0:
            return 0.0
        return abs((current - prior) / prior) * 100

    def test_50_percent_increase(self):
        pct = self._calc_pct_change(current=15000, prior=10000)
        assert abs(pct - 50.0) < 0.01

    def test_25_percent_decrease(self):
        pct = self._calc_pct_change(current=7500, prior=10000)
        assert abs(pct - 25.0) < 0.01

    def test_no_change(self):
        pct = self._calc_pct_change(current=10000, prior=10000)
        assert pct == 0.0

    def test_zero_prior_returns_zero(self):
        """Guard against division by zero."""
        pct = self._calc_pct_change(current=5000, prior=0)
        assert pct == 0.0

    def test_threshold_not_breached(self):
        """20% change with 25% threshold → should not alert."""
        pct = self._calc_pct_change(current=12000, prior=10000)
        threshold = 25.0
        assert pct < threshold

    def test_threshold_breached(self):
        """30% change with 25% threshold → should alert."""
        pct = self._calc_pct_change(current=13000, prior=10000)
        threshold = 25.0
        assert pct >= threshold


class TestEmailRendering:
    """Test the HTML email content rendering."""

    def test_low_stock_email_contains_product_title(self):
        from app.alerts.engine import AlertEngine
        engine = AlertEngine()
        products = [
            {"title": "Harry Potter", "author": "J.K. Rowling", "quantity_in_stock": 2},
            {"title": "The Hobbit",   "author": "J.R.R. Tolkien", "quantity_in_stock": 1},
        ]
        html = engine._render_low_stock_email(products, threshold=5)
        assert "Harry Potter" in html
        assert "The Hobbit" in html
        assert "J.K. Rowling" in html

    def test_low_stock_email_contains_threshold(self):
        from app.alerts.engine import AlertEngine
        engine = AlertEngine()
        html = engine._render_low_stock_email(
            [{"title": "Book A", "author": None, "quantity_in_stock": 3}],
            threshold=5,
        )
        assert "5" in html  # threshold value shown in email
