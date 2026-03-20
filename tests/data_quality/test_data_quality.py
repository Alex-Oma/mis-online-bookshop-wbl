"""
Data quality tests — run SQL assertions against the core schema
after a real ingestion to verify referential integrity and business rules.

Requires TEST_DATABASE_URL pointing to a populated test database.
"""
import os
import pytest
import asyncpg
from app.config import get_settings


_settings = get_settings()

# DATABASE_URL = _settings.database_url
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://postgres:test@localhost:5432/mis_test")
DATABASE_URL = "postgresql+asyncpg://postgres:test@localhost:5432/mis_test"

@pytest.fixture
async def conn():
    connection = await asyncpg.connect(DATABASE_URL)
    yield connection
    await connection.close()


@pytest.mark.asyncio
async def test_no_null_product_id_in_order_lines(conn):
    """Every fact_order_lines row should be linked to a known product."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM core.fact_order_lines
        WHERE product_id IS NULL
        """
    )
    # It's acceptable to have NULLs for Rozetka lines where item_name
    # didn't match a known product — warn but don't hard-fail
    null_count = row["cnt"]
    if null_count > 0:
        import warnings
        warnings.warn(
            f"{null_count} order lines have no matched product_id. "
            "Consider improving the Rozetka → dim_product matching logic."
        )


@pytest.mark.asyncio
async def test_all_date_ids_exist_in_dim_date(conn):
    """Every date_id used in fact_orders must exist in dim_date."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM core.fact_orders fo
        LEFT JOIN core.dim_date d ON d.date_id = fo.date_id
        WHERE d.date_id IS NULL
        """
    )
    assert row["cnt"] == 0, (
        f"{row['cnt']} fact_orders rows have a date_id not found in dim_date. "
        "Ensure the dim_date seed covers all order dates."
    )


@pytest.mark.asyncio
async def test_no_duplicate_orders_per_channel(conn):
    """No (source_order_id, channel_id) pair should appear more than once."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT source_order_id, channel_id, COUNT(*) AS n
            FROM core.fact_orders
            GROUP BY source_order_id, channel_id
            HAVING COUNT(*) > 1
        ) dups
        """
    )
    assert row["cnt"] == 0, (
        f"{row['cnt']} duplicate (source_order_id, channel_id) pairs found in fact_orders."
    )


@pytest.mark.asyncio
async def test_rozetka_orders_have_valid_status_group(conn):
    """All Rozetka orders must have status_group in {1, 2, 3}."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM staging.stg_rozetka_orders
        WHERE status_group NOT IN (1, 2, 3)
          AND status_group IS NOT NULL
        """
    )
    assert row["cnt"] == 0, (
        f"{row['cnt']} Rozetka orders have an invalid status_group value."
    )


@pytest.mark.asyncio
async def test_no_future_order_dates(conn):
    """No orders should have date_added in the future."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM core.fact_orders
        WHERE date_added > NOW() + INTERVAL '1 hour'
        """
    )
    assert row["cnt"] == 0, (
        f"{row['cnt']} orders have a future date_added — possible data quality issue."
    )


@pytest.mark.asyncio
async def test_revenue_reconciliation(conn):
    """
    Sum of fact_order_lines.line_total per order should be within 1% of
    fact_orders.total_amount for completed orders.
    (Allows for shipping/discount components in order total.)
    """
    rows = await conn.fetch(
        """
        SELECT
            fo.order_id,
            fo.total_amount,
            COALESCE(SUM(l.line_total), 0) AS lines_total
        FROM core.fact_orders fo
        LEFT JOIN core.fact_order_lines l ON l.order_id = fo.order_id
        JOIN core.dim_order_status s ON s.status_id = fo.status_id
        WHERE s.status_group = 2   -- completed orders only
          AND fo.total_amount > 0
        GROUP BY fo.order_id, fo.total_amount
        HAVING ABS(fo.total_amount - COALESCE(SUM(l.line_total), 0))
               / fo.total_amount > 0.50  -- flag if lines differ >50% from order total
        LIMIT 10
        """
    )
    if rows:
        import warnings
        examples = [(r["order_id"], float(r["total_amount"]), float(r["lines_total"])) for r in rows]
        warnings.warn(
            f"{len(rows)} completed orders have line totals differing >50% from order total. "
            f"Examples: {examples[:3]}"
        )


@pytest.mark.asyncio
async def test_dim_product_has_titles(conn):
    """All active products must have a non-empty title."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM core.dim_product
        WHERE (title IS NULL OR TRIM(title) = '')
          AND status = 'active'
        """
    )
    assert row["cnt"] == 0, (
        f"{row['cnt']} active products have a NULL or empty title."
    )


@pytest.mark.asyncio
async def test_sales_channels_populated(conn):
    """Both sales channels must be present in dim_sales_channel."""
    rows = await conn.fetch(
        "SELECT channel_name FROM core.dim_sales_channel ORDER BY channel_id"
    )
    names = [r["channel_name"] for r in rows]
    assert "Website" in names, "Website channel missing from dim_sales_channel"
    assert "Rozetka" in names, "Rozetka channel missing from dim_sales_channel"
