"""
WebsiteAdapter — extracts data from the live OpenCart MySQL database
and upserts it into the MIS staging tables (staging.stg_website_*).

Uses a synchronous SQLAlchemy connection to OpenCart (MySQL)
then async asyncpg writes to the MIS database (PostgreSQL).
"""
import json
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine, text

from app.config import get_settings
from app.database import get_pool
from app.models.website import (
    OcCategory,
    OcCustomer,
    OcManufacturer,
    OcOrder,
    OcOrderProduct,
    OcProduct,
)

# Set up module-level logger
logger = logging.getLogger(__name__)

# ── SQL queries against OpenCart MySQL ────────────────────────────────────────

# This SQL retrieves all orders modified since the last sync, along with relevant fields needed for reporting and analysis.
# It does not join to order_product or other tables to keep it efficient; order lines are fetched in a separate query using the order IDs.
# The results are ordered by date_modified to ensure we process records in chronological order, which can help with debugging and incremental loads.
_SQL_ORDERS = """
SELECT
    o.order_id, o.store_id, o.customer_id, o.customer_group_id,
    o.payment_city, o.payment_country,
    o.shipping_city, o.shipping_method, o.payment_method,
    o.total, o.order_status_id,
    o.currency_code, o.currency_value,
    o.date_added, o.date_modified
FROM oc_order o
WHERE o.date_modified >= :since
ORDER BY o.date_modified ASC
"""

# This SQL retrieves all order lines for a given set of order IDs.
# It includes product details like name and model, as well as pricing and tax information.
_SQL_ORDER_PRODUCTS = """
SELECT
    op.order_product_id, op.order_id, op.product_id,
    op.name, op.model, op.quantity,
    op.price, op.total, op.tax
FROM oc_order_product op
WHERE op.order_id IN :order_ids
"""

# This SQL retrieves all products modified since the last sync, along with their descriptions and main category.
_SQL_PRODUCTS = """
SELECT
    p.product_id, p.model, p.sku, p.isbn,
    p.quantity, p.stock_status_id, p.manufacturer_id,
    p.price, p.date_available,
    p.publishing_year, p.pages_number,
    p.author, p.publisher, p.pereplet AS binding_type,
    p.status, p.date_added, p.date_modified,
    pd.name, pd.description,
    ptc.category_id AS main_category_id
FROM oc_product p
LEFT JOIN oc_product_description pd
    ON pd.product_id = p.product_id AND pd.language_id = 1
LEFT JOIN oc_product_to_category ptc
    ON ptc.product_id = p.product_id AND ptc.main_category = 1
WHERE p.date_modified >= :since
"""

# This SQL retrieves all categories along with their descriptions.
# Since categories are a relatively small table and often needed for joins, we do a full refresh every time instead of incremental.
_SQL_CATEGORIES = """
SELECT
    c.category_id, c.parent_id, c.status, c.sort_order,
    cd.name, cd.description
FROM oc_category c
LEFT JOIN oc_category_description cd
    ON cd.category_id = c.category_id AND cd.language_id = 1
"""

# This SQL retrieves all manufacturers along with their descriptions.
_SQL_MANUFACTURERS = """
SELECT
    m.manufacturer_id, m.name,
    md.description
FROM oc_manufacturer m
LEFT JOIN oc_manufacturer_description md
    ON md.manufacturer_id = m.manufacturer_id AND md.language_id = 1
"""

# This SQL retrieves all customers modified since the last sync, along with their group and newsletter subscription status.
_SQL_CUSTOMERS = """
SELECT
    c.customer_id, c.customer_group_id, c.store_id,
    c.newsletter AS is_newsletter,
    c.date_added,
    cgd.name AS customer_group_name
FROM oc_customer c
LEFT JOIN oc_customer_group_description cgd
    ON cgd.customer_group_id = c.customer_group_id AND cgd.language_id = 1
WHERE c.date_added >= :since
"""


class WebsiteAdapter:
    """Reads from OpenCart MySQL and writes to MIS staging tables."""

    def __init__(self):
        # Load settings once at initialization (e.g. database URLs, batch sizes)
        self._settings = get_settings()

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> dict:
        """
        Run a full extraction cycle.

        If from_date is None the adapter reads the last successful sync
        timestamp from mis.sync_log; if no prior sync exists it performs
        a full historical load from 2015-01-01.
        """
        # gets a connection pool to the MIS PostgreSQL database for staging and logging
        pool = await get_pool()

        # Determine sync window
        since = from_date or await self._get_last_sync(pool)
        logger.info("WebsiteAdapter: extracting records modified since %s", since)

        # Record sync start
        sync_id = await self._start_sync_log(pool, since)

        try:
            # Create a synchronous SQLAlchemy engine for connecting to the OpenCart MySQL database
            engine = create_engine(self._settings.opencart_db_url)
            # Perform extraction from OpenCart and staging into MIS, then record sync completion with stats
            stats = await self._extract_and_stage(engine, pool, since)
            # Update sync log with success status and stats
            await self._finish_sync_log(pool, sync_id, "success", stats)
            # Log summary of ingested records for visibility
            logger.info("WebsiteAdapter: completed — %s", stats)
            return stats
        except Exception as exc:
            # On any error, update sync log with failure status and error details, then re-raise the exception
            await self._finish_sync_log(pool, sync_id, "failed", {}, str(exc))
            logger.error("WebsiteAdapter: failed — %s", exc, exc_info=True)
            raise

    # ── Extraction & staging ──────────────────────────────────────────────────

    async def _extract_and_stage(self, engine, pool, since: datetime) -> dict:
        '''Extract data from OpenCart and upsert into MIS staging tables.'''
        since_str = since.strftime("%Y-%m-%d %H:%M:%S")
        # Initialize stats dictionary to track number of records ingested for each entity type
        stats = {"orders": 0, "order_lines": 0, "products": 0,
                 "categories": 0, "manufacturers": 0, "customers": 0}

        # Use a synchronous connection to OpenCart MySQL to execute queries and fetch data, then use asyncpg to upsert into MIS PostgreSQL
        with engine.connect() as conn:
            # ── Orders ────────────────────────────────────────────────────────
            # This query retrieves all orders modified since the last sync, ordered by modification date. We then upsert these orders into the staging table.
            rows = conn.execute(text(_SQL_ORDERS), {"since": since_str}).mappings().all()
            # We convert the result rows into OcOrder Pydantic models for easier handling and JSON serialization.
            # The upsert function will handle inserting new records and updating existing ones based on order_id.
            orders = [OcOrder(**dict(r)) for r in rows]
            # If we have any orders to process, we call the upsert helper to write them to the staging table, and then we update our stats with the count of orders processed.
            if orders:
                # Upsert orders into staging table; the upsert logic will handle both inserts and updates based on order_id as the unique key.
                await self._upsert_orders(pool, orders)
                stats["orders"] = len(orders)

                # ── Order lines ───────────────────────────────────────────────
                order_ids = tuple(o.order_id for o in orders)
                # For the orders we just processed, we need to fetch their associated order lines.
                # We execute a separate query that retrieves all order lines for the given set of order IDs.
                # This allows us to keep the initial orders query efficient and only fetch order lines for relevant orders.
                line_rows = conn.execute(
                    text(_SQL_ORDER_PRODUCTS), {"order_ids": order_ids}
                ).mappings().all()
                # Similar to orders, we convert the order line rows into OcOrderProduct Pydantic models for easier handling.
                lines = [OcOrderProduct(**dict(r)) for r in line_rows]
                if lines:
                    # Upsert order lines into staging table; the upsert logic will handle inserts and updates based on order_product_id as the unique key.
                    await self._upsert_order_lines(pool, lines)
                stats["order_lines"] = len(lines)

            # ── Products ──────────────────────────────────────────────────────
            # This query retrieves all products modified since the last sync, along with their descriptions and main category.
            prod_rows = conn.execute(
                text(_SQL_PRODUCTS), {"since": since_str}
            ).mappings().all()
            # We convert the product rows into OcProduct Pydantic models.
            # The upsert function will handle inserting new products and updating existing ones based on product_id.
            products = [OcProduct(**dict(r)) for r in prod_rows]
            if products:
                # Upsert products into staging table; the upsert logic will handle both inserts and updates based on product_id as the unique key.
                await self._upsert_products(pool, products)
            stats["products"] = len(products)

            # ── Categories (always full refresh — small table) ─────────────
            # Since categories are a relatively small table and often needed for joins, we do a full refresh every time instead of incremental.
            cat_rows = conn.execute(text(_SQL_CATEGORIES)).mappings().all()
            # We convert the category rows into OcCategory Pydantic models.
            # The upsert function will handle inserting new categories and updating existing ones based on category_id.
            categories = [OcCategory(**dict(r)) for r in cat_rows]
            if categories:
                # Upsert categories into staging table; the upsert logic will handle both inserts and updates based on category_id as the unique key.
                await self._upsert_categories(pool, categories)
            stats["categories"] = len(categories)

            # ── Manufacturers ────────────────────────────────────────────────
            # This query retrieves all manufacturers along with their descriptions.
            mfr_rows = conn.execute(text(_SQL_MANUFACTURERS)).mappings().all()
            # We convert the manufacturer rows into OcManufacturer Pydantic models.
            # The upsert function will handle inserting new manufacturers and updating existing ones based on manufacturer_id.
            manufacturers = [OcManufacturer(**dict(r)) for r in mfr_rows]
            if manufacturers:
                # Upsert manufacturers into staging table; the upsert logic will handle both inserts and updates based on manufacturer_id as the unique key.
                await self._upsert_manufacturers(pool, manufacturers)
            stats["manufacturers"] = len(manufacturers)

            # ── Customers ────────────────────────────────────────────────────
            # This query retrieves all customers modified since the last sync, along with their group and newsletter subscription status.
            cust_rows = conn.execute(
                text(_SQL_CUSTOMERS), {"since": since_str}
            ).mappings().all()
            # We convert the customer rows into OcCustomer Pydantic models.
            # The upsert function will handle inserting new customers and updating existing ones based on customer_id.
            customers = [OcCustomer(**dict(r)) for r in cust_rows]
            if customers:
                # Upsert customers into staging table; the upsert logic will handle both inserts and updates based on customer_id as the unique key.
                await self._upsert_customers(pool, customers)
            stats["customers"] = len(customers)

        return stats

    # ── Upsert helpers ────────────────────────────────────────────────────────

    async def _upsert_orders(self, pool, orders: list[OcOrder]) -> None:
        # We prepare the records for upsert by converting each OcOrder model into a tuple of values corresponding to the staging table columns.
        records = [
            (
                o.order_id, o.store_id, o.customer_id, o.customer_group_id,
                o.payment_city, o.payment_country,
                o.shipping_city, o.shipping_method, o.payment_method,
                float(o.total), o.order_status_id,
                o.currency_code, float(o.currency_value),
                o.date_added, o.date_modified,
                json.dumps(o.model_dump(mode="json")),
            )
            for o in orders
        ]
        # We use asyncpg's executemany to perform a bulk upsert into the staging.stg_website_orders table.
        async with pool.acquire() as conn:
            # The SQL query inserts new orders into the staging table, but if an order with the same order_id already exists, it updates certain fields (total, order_status_id, date_modified, raw_json) and sets ingested_at to NOW().
            # The SQL statement uses ON CONFLICT on order_id to determine whether to insert a new record or update an existing one.
            await conn.executemany(
                """
                INSERT INTO staging.stg_website_orders (
                    order_id, store_id, customer_id, customer_group_id,
                    payment_city, payment_country,
                    shipping_city, shipping_method, payment_method,
                    total, order_status_id,
                    currency_code, currency_value,
                    date_added, date_modified, raw_json
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                ON CONFLICT (order_id) DO UPDATE SET
                    total          = EXCLUDED.total,
                    order_status_id= EXCLUDED.order_status_id,
                    date_modified  = EXCLUDED.date_modified,
                    raw_json       = EXCLUDED.raw_json,
                    ingested_at    = NOW()
                """,
                records,
            )

    async def _upsert_order_lines(self, pool, lines: list[OcOrderProduct]) -> None:
        # We prepare the records for upsert by converting each OcOrderProduct model into a tuple of values corresponding to the staging table columns.
        records = [
            (
                l.order_product_id, l.order_id, l.product_id,
                l.name, l.model, l.quantity,
                float(l.price), float(l.total), float(l.tax),
                json.dumps(l.model_dump(mode="json")),
            )
            for l in lines
        ]
        # We use asyncpg's executemany to perform a bulk upsert into the staging.stg_website_order_lines table.
        async with pool.acquire() as conn:
            # The SQL query inserts new order lines into the staging table, but if an order line with the same order_product_id already exists, it updates certain fields (quantity, price, total, raw_json) and sets ingested_at to NOW().
            # The SQL statement uses ON CONFLICT on order_product_id to determine whether to insert a new record or update an existing one.
            await conn.executemany(
                """
                INSERT INTO staging.stg_website_order_lines (
                    order_product_id, order_id, product_id,
                    name, model, quantity, price, total, tax, raw_json
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (order_product_id) DO UPDATE SET
                    quantity    = EXCLUDED.quantity,
                    price       = EXCLUDED.price,
                    total       = EXCLUDED.total,
                    raw_json    = EXCLUDED.raw_json,
                    ingested_at = NOW()
                """,
                records,
            )

    async def _upsert_products(self, pool, products: list[OcProduct]) -> None:
        # We prepare the records for upsert by converting each OcProduct model into a tuple of values corresponding to the staging table columns.
        records = [
            (
                p.product_id, p.model, p.sku, p.isbn,
                p.quantity, p.stock_status_id, p.manufacturer_id,
                float(p.price), p.date_available,
                p.publishing_year, p.pages_number,
                p.author, p.publisher, p.binding_type,
                p.status, p.date_added, p.date_modified,
                p.name, p.description, p.main_category_id,
                json.dumps(p.model_dump(mode="json")),
            )
            for p in products
        ]
        # We use asyncpg's executemany to perform a bulk upsert into the staging.stg_website_products table.
        async with pool.acquire() as conn:
            # The SQL query inserts new products into the staging table, but if a product with the same product_id already exists, it updates certain fields (quantity, price, status, date_modified, name, raw_json) and sets ingested_at to NOW().
            # The SQL statement uses ON CONFLICT on product_id to determine whether to insert a new record or update an existing one.
            await conn.executemany(
                """
                INSERT INTO staging.stg_website_products (
                    product_id, model, sku, isbn,
                    quantity, stock_status_id, manufacturer_id,
                    price, date_available,
                    publishing_year, pages_number,
                    author, publisher, binding_type,
                    status, date_added, date_modified,
                    name, description, main_category_id, raw_json
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                    $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
                )
                ON CONFLICT (product_id) DO UPDATE SET
                    quantity       = EXCLUDED.quantity,
                    price          = EXCLUDED.price,
                    status         = EXCLUDED.status,
                    date_modified  = EXCLUDED.date_modified,
                    name           = EXCLUDED.name,
                    raw_json       = EXCLUDED.raw_json,
                    ingested_at    = NOW()
                """,
                records,
            )

    async def _upsert_categories(self, pool, categories: list[OcCategory]) -> None:
        # We prepare the records for upsert by converting each OcCategory model into a tuple of values corresponding to the staging table columns.
        records = [
            (c.category_id, c.parent_id, c.status, c.sort_order, c.name, c.description)
            for c in categories
        ]
        # We use asyncpg's executemany to perform a bulk upsert into the staging.stg_website_categories table.
        async with pool.acquire() as conn:
            # Store categories in a temporary table first then upsert
            # (categories are referenced by products so we do a full refresh)
            # The SQL query inserts new categories into the staging table, but if a category with the same category_id already exists, it updates certain fields (parent_id, status, name) and sets ingested_at to NOW().
            # The SQL statement uses ON CONFLICT on category_id to determine whether to insert a new record or update an existing one.
            await conn.executemany(
                """
                INSERT INTO staging.stg_website_categories (
                    category_id, parent_id, status, sort_order, name, description
                ) VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (category_id) DO UPDATE SET
                    parent_id   = EXCLUDED.parent_id,
                    status      = EXCLUDED.status,
                    name        = EXCLUDED.name,
                    ingested_at = NOW()
                """,
                records,
            )

    async def _upsert_manufacturers(self, pool, mfrs: list[OcManufacturer]) -> None:
        # We prepare the records for upsert by converting each OcManufacturer model into a tuple of values corresponding to the staging table columns.
        records = [(m.manufacturer_id, m.name, m.description) for m in mfrs]
        # We use asyncpg's executemany to perform a bulk upsert into the staging.stg_website_manufacturers table.
        async with pool.acquire() as conn:
            # The SQL query inserts new manufacturers into the staging table, but if a manufacturer with the same manufacturer_id already exists, it updates certain fields (name, description) and sets ingested_at to NOW().
            # The SQL statement uses ON CONFLICT on manufacturer_id to determine whether to insert a new record or update an existing one.
            await conn.executemany(
                """
                INSERT INTO staging.stg_website_manufacturers (
                    manufacturer_id, name, description
                ) VALUES ($1,$2,$3)
                ON CONFLICT (manufacturer_id) DO UPDATE SET
                    name        = EXCLUDED.name,
                    description = EXCLUDED.description,
                    ingested_at = NOW()
                """,
                records,
            )

    async def _upsert_customers(self, pool, customers: list[OcCustomer]) -> None:
        # We prepare the records for upsert by converting each OcCustomer model into a tuple of values corresponding to the staging table columns.
        records = [
            (
                c.customer_id, c.customer_group_id, c.store_id,
                c.is_newsletter, c.date_added, c.customer_group_name,
            )
            for c in customers
        ]
        # We use asyncpg's executemany to perform a bulk upsert into the staging.stg_website_customers table.
        async with pool.acquire() as conn:
            # The SQL query inserts new customers into the staging table, but if a customer with the same customer_id already exists, it updates certain fields (customer_group_id, customer_group_name) and sets ingested_at to NOW().
            # The SQL statement uses ON CONFLICT on customer_id to determine whether to insert a new record or update an existing one.
            await conn.executemany(
                """
                INSERT INTO staging.stg_website_customers (
                    customer_id, customer_group_id, store_id,
                    is_newsletter, date_added, customer_group_name
                ) VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_group_id   = EXCLUDED.customer_group_id,
                    customer_group_name = EXCLUDED.customer_group_name,
                    ingested_at         = NOW()
                """,
                records,
            )

    # ── Sync log helpers ──────────────────────────────────────────────────────

    async def _get_last_sync(self, pool) -> datetime:
        # This query retrieves the completed_at timestamp of the most recent successful sync for the 'website' channel from the mis.sync_log table.
        async with pool.acquire() as conn:
            # We order the results by completed_at in descending order and limit to 1 to get the latest successful sync timestamp.
            row = await conn.fetchrow(
                """
                SELECT completed_at FROM mis.sync_log
                WHERE channel = 'website' AND status = 'success'
                ORDER BY completed_at DESC LIMIT 1
                """
            )
        if row and row["completed_at"]:
            # If we have a completed_at timestamp from the last successful sync, we return that as the starting point for our next incremental load.
            return row["completed_at"]
        # Default: full historical load from 2015
        return datetime(2015, 1, 1)

    async def _start_sync_log(self, pool, since: datetime) -> int:
        # This query inserts a new record into the mis.sync_log table to indicate the start of a new sync process for the 'website' channel.
        async with pool.acquire() as conn:
            # We set the sync_type to 'scheduled', the started_at timestamp to NOW(), and the initial status to 'running'. We also return the generated sync_id for this new log entry so we can update it later when the sync completes.
            row = await conn.fetchrow(
                """
                INSERT INTO mis.sync_log (channel, sync_type, started_at, status)
                VALUES ('website', 'scheduled', NOW(), 'running')
                RETURNING sync_id
                """
            )
        return row["sync_id"]

    async def _finish_sync_log(
        self, pool, sync_id: int, status: str, stats: dict, error: str = None
    ) -> None:
        # This query updates the existing record in the mis.sync_log table for the given sync_id to indicate that the sync process has completed, along with the final status (success or failed), the number of records ingested, and any error details if applicable.
        async with pool.acquire() as conn:
            # We set the completed_at timestamp to NOW(), update the status to either 'success' or 'failed', and record the total number of records ingested (sum of orders and products for simplicity) and any error details if the sync failed.
            await conn.execute(
                """
                UPDATE mis.sync_log SET
                    completed_at     = NOW(),
                    status           = $1,
                    records_ingested = $2,
                    records_failed   = $3,
                    error_detail     = $4
                WHERE sync_id = $5
                """,
                status,
                stats.get("orders", 0) + stats.get("products", 0),
                stats.get("errors", 0),
                error,
                sync_id,
            )

