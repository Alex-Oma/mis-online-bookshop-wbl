"""
Transformer — reads from staging tables and populates the core
star-schema dimension and fact tables.

Implements SCD Type 1 (overwrite) for all dimensions.
Runs after both WebsiteAdapter and RozetkaAdapter have completed.
"""
import logging
from datetime import date, datetime

from app.database import get_pool

# Set up module-level logger
logger = logging.getLogger(__name__)


class Transformer:
    """Transforms staged raw data into the core star schema."""

    async def run(self) -> dict:
        '''
        Main entry point for the transformer.
        Returns a dict with counts of records processed for each dimension and fact table.
        '''
        #  Get a connection pool and acquire a connection
        pool = await get_pool()
        stats = {}

        logger.info("Transformer: starting")
        # Transform dimensions first (publishers, categories, products, customers, order statuses)
        # Then transform facts (orders and order lines) which depend on the dimensions being up-to-date
        # for correct foreign key relationships and data integrity.
        async with pool.acquire() as conn:
            stats["publishers"]  = await self._transform_publishers(conn)
            stats["categories"]  = await self._transform_categories(conn)
            stats["products"]    = await self._transform_products(conn)
            stats["customers"]   = await self._transform_customers(conn)
            stats["order_statuses"] = await self._transform_order_statuses(conn)
            stats["website_orders"] = await self._transform_website_orders(conn)
            stats["rozetka_orders"] = await self._transform_rozetka_orders(conn)

        # Log summary of transformation results
        logger.info("Transformer: completed — %s", stats)
        # Return stats for use in monitoring
        return stats

    # ── Dimensions ────────────────────────────────────────────────────────────

    async def _transform_publishers(self, conn) -> int:
        '''Move manufacturers from staging into dim_publisher.'''
        # On conflict, update name and description to reflect any changes in the source data.
        result = await conn.execute(
            """
            INSERT INTO core.dim_publisher (source_manufacturer_id, name, description)
            SELECT manufacturer_id, name, description
            FROM staging.stg_website_manufacturers
            ON CONFLICT (source_manufacturer_id) DO UPDATE SET
                name        = EXCLUDED.name,
                description = EXCLUDED.description
            """
        )
        # The result of an INSERT in asyncpg is a string like "INSERT 0 1234" where 1234 is the number of rows affected.
        return int(result.split()[-1])

    async def _transform_categories(self, conn) -> int:
        # Two-pass: parents first, then children (single level of nesting assumed)
        # Pass 1: top-level categories (parent_id = 0)
        # The SQL query uses ON CONFLICT to handle updates if category names or sort orders change in the source data.
        # It also sets is_age_group to TRUE for top-level categories, as per the original schema design.
        await conn.execute(
            """
            INSERT INTO core.dim_category (
                source_category_id, source_channel, name,
                parent_id, parent_name, is_age_group, sort_order
            )
            SELECT
                category_id, 'website', name,
                NULL, NULL, TRUE, sort_order
            FROM staging.stg_website_categories
            WHERE parent_id = 0
            ON CONFLICT (source_category_id, source_channel) DO UPDATE SET
                name        = EXCLUDED.name,
                is_age_group= TRUE,
                sort_order  = EXCLUDED.sort_order
            """
        )
        # Pass 2: child categories
        # The SQL query joins the staging categories with the already inserted parent categories in dim_category to get the parent_id and parent_name.
        result = await conn.execute(
            """
            INSERT INTO core.dim_category (
                source_category_id, source_channel, name,
                parent_id, parent_name, is_age_group, sort_order
            )
            SELECT
                c.category_id, 'website', c.name,
                p.category_id AS parent_id,
                p.name        AS parent_name,
                FALSE, c.sort_order
            FROM staging.stg_website_categories c
            JOIN core.dim_category p
                ON p.source_category_id = c.parent_id
               AND p.source_channel = 'website'
            WHERE c.parent_id <> 0
            ON CONFLICT (source_category_id, source_channel) DO UPDATE SET
                name       = EXCLUDED.name,
                parent_id  = EXCLUDED.parent_id,
                parent_name= EXCLUDED.parent_name,
                sort_order = EXCLUDED.sort_order
            """
        )
        # The result of the second insert will give us the count of child categories processed, which is a good indicator of how many categories were handled in total (since top-level categories are usually fewer).
        return int(result.split()[-1])

    async def _transform_products(self, conn) -> int:
        '''Move products from staging into dim_product, linking to publishers and categories.'''
        # The SQL query selects products from the staging table and joins with dim_publisher and dim_category to get the corresponding publisher_id and category_id for the product. It also joins with a staging table for stock statuses to get the stock status name. The ON CONFLICT clause ensures that if a product with the same source_product_id and source_channel already exists, it will update the relevant fields instead of inserting a duplicate.
        result = await conn.execute(
            """
            INSERT INTO core.dim_product (
                source_product_id, source_channel,
                title, author, isbn,
                publisher_id, publishing_year, pages_number,
                binding_type, category_id,
                price, quantity_in_stock, stock_status,
                status, date_available, date_added, last_updated
            )
            SELECT
                p.product_id, 'website',
                p.name, p.author, p.isbn,
                pub.publisher_id, p.publishing_year, p.pages_number,
                p.binding_type,
                cat.category_id,
                p.price, p.quantity,
                ss.name AS stock_status,
                CASE WHEN p.status = 1 THEN 'active' ELSE 'inactive' END,
                p.date_available, p.date_added, NOW()
            FROM staging.stg_website_products p
            LEFT JOIN core.dim_publisher pub
                ON pub.source_manufacturer_id = p.manufacturer_id
            LEFT JOIN core.dim_category cat
                ON cat.source_category_id = p.main_category_id
               AND cat.source_channel = 'website'
            LEFT JOIN staging.stg_website_stock_statuses ss
                ON ss.stock_status_id = p.stock_status_id
            ON CONFLICT (source_product_id, source_channel) DO UPDATE SET
                title             = EXCLUDED.title,
                price             = EXCLUDED.price,
                quantity_in_stock = EXCLUDED.quantity_in_stock,
                stock_status      = EXCLUDED.stock_status,
                status            = EXCLUDED.status,
                last_updated      = NOW()
            """
        )
        # The result of the INSERT statement will indicate how many product records were inserted or updated. This is a key metric for understanding the volume of product data processed during the transformation.
        return int(result.split()[-1])

    async def _transform_customers(self, conn) -> int:
        '''Move customers from staging into dim_customer.'''
        # The SQL query selects customers from the staging table and inserts them into the dim_customer table.
        # It uses ON CONFLICT to handle updates if a customer with the same source_customer_id and source_channel already exists, ensuring that customer group information is kept up-to-date.
        # The date_registered field is derived from the date_added field in the staging table, and is_newsletter is directly taken from the staging data.
        result = await conn.execute(
            """
            INSERT INTO core.dim_customer (
                source_customer_id, source_channel,
                customer_group, date_registered, is_newsletter
            )
            SELECT
                customer_id, 'website',
                customer_group_name,
                date_added::date,
                is_newsletter
            FROM staging.stg_website_customers
            ON CONFLICT (source_customer_id, source_channel) DO UPDATE SET
                customer_group = EXCLUDED.customer_group
            """
        )
        return int(result.split()[-1])

    async def _transform_order_statuses(self, conn) -> int:
        '''Move order statuses from staging into dim_order_status.'''
        # Website order statuses
        # The SQL query selects distinct order statuses from the staging orders table and inserts them into the dim_order_status table.
        # It constructs a name for each status based on its ID (as a placeholder) and assigns a status_group based on predefined logic (e.g., status ID 5 is considered "Delivered" and grouped as success, while certain other IDs are grouped as cancelled). The ON CONFLICT clause ensures that if a status with the same source_id and source_channel already exists, it will not insert a duplicate.
        await conn.execute(
            """
            INSERT INTO core.dim_order_status (source_id, source_channel, name_en, status_group)
            SELECT DISTINCT
                order_status_id, 'website',
                'Status ' || order_status_id::text,   -- placeholder; update from oc_order_status
                CASE
                    WHEN order_status_id IN (5) THEN 2   -- Delivered = success
                    WHEN order_status_id IN (7,13,16,17) THEN 3  -- cancelled
                    ELSE 1
                END
            FROM staging.stg_website_orders
            ON CONFLICT (source_id, source_channel) DO NOTHING
            """
        )
        # Rozetka order statuses (status_group comes directly from API)
        # The SQL query selects distinct order statuses from the Rozetka staging orders table and inserts them into the dim_order_status table.
        result = await conn.execute(
            """
            INSERT INTO core.dim_order_status (source_id, source_channel, name_en, status_group)
            SELECT DISTINCT
                status, 'rozetka',
                'Rozetka status ' || status::text,
                status_group
            FROM staging.stg_rozetka_orders
            WHERE status IS NOT NULL
            ON CONFLICT (source_id, source_channel) DO NOTHING
            """
        )
        # The result of the second insert will give us the count of Rozetka order statuses processed, which is a useful metric for understanding how many unique order statuses were handled from the Rozetka data source.
        return int(result.split()[-1])

    # ── Facts ─────────────────────────────────────────────────────────────────

    async def _transform_website_orders(self, conn) -> int:
        """Move website orders from staging into fact_orders + fact_order_lines."""
        # The SQL query inserts orders from the staging table into the fact_orders table, linking to customers and order statuses via LEFT JOINs to get the corresponding customer_id and status_id for each order.
        # It calculates total_amount_uah by multiplying the total by the currency_value, and sets the channel_id to 1 for website orders.
        # The ON CONFLICT clause ensures that if an order with the same source_order_id and channel_id already exists, it will update the relevant fields instead of inserting a duplicate.
        result = await conn.execute(
            """
            INSERT INTO core.fact_orders (
                source_order_id, channel_id, date_id,
                customer_id, status_id,
                total_amount, total_amount_uah,
                currency_code, currency_value,
                shipping_city, shipping_method, payment_method,
                date_added, date_modified
            )
            SELECT
                o.order_id,
                1 AS channel_id,          -- Website = channel 1
                TO_CHAR(o.date_added, 'YYYYMMDD')::integer AS date_id,
                cust.customer_id,
                stat.status_id,
                o.total,
                o.total * o.currency_value AS total_amount_uah,
                o.currency_code,
                o.currency_value,
                o.shipping_city,
                o.shipping_method,
                o.payment_method,
                o.date_added,
                o.date_modified
            FROM staging.stg_website_orders o
            LEFT JOIN core.dim_customer cust
                ON cust.source_customer_id = o.customer_id
               AND cust.source_channel = 'website'
            LEFT JOIN core.dim_order_status stat
                ON stat.source_id = o.order_status_id
               AND stat.source_channel = 'website'
            ON CONFLICT (source_order_id, channel_id) DO UPDATE SET
                status_id      = EXCLUDED.status_id,
                total_amount   = EXCLUDED.total_amount,
                total_amount_uah = EXCLUDED.total_amount_uah,
                date_modified  = EXCLUDED.date_modified
            """
        )
        # The result of the INSERT statement will indicate how many order records were inserted or updated for the website channel.
        # This is a key metric for understanding the volume of order data processed during the transformation.
        orders_count = int(result.split()[-1])

        # Order lines
        # The SQL query inserts order lines from the staging table into the fact_order_lines table, linking to products and categories via LEFT JOINs to get the corresponding product_id and category_id for each line item.
        await conn.execute(
            """
            INSERT INTO core.fact_order_lines (
                source_line_id, order_id,
                product_id, category_id, channel_id, date_id,
                quantity_sold, unit_price, line_total, line_total_uah, tax
            )
            SELECT
                ol.order_product_id,
                fo.order_id,
                prod.product_id,
                prod.category_id,
                1 AS channel_id,
                TO_CHAR(fo.date_added, 'YYYYMMDD')::integer AS date_id,
                ol.quantity,
                ol.price,
                ol.total,
                ol.total * fo.currency_value AS line_total_uah,
                ol.tax
            FROM staging.stg_website_order_lines ol
            JOIN core.fact_orders fo
                ON fo.source_order_id = ol.order_id
               AND fo.channel_id = 1
            LEFT JOIN core.dim_product prod
                ON prod.source_product_id = ol.product_id
               AND prod.source_channel = 'website'
            ON CONFLICT (source_line_id, channel_id) DO UPDATE SET
                quantity_sold  = EXCLUDED.quantity_sold,
                unit_price     = EXCLUDED.unit_price,
                line_total     = EXCLUDED.line_total,
                line_total_uah = EXCLUDED.line_total_uah
            """
        )
        # The order lines are inserted or updated based on the source_line_id and channel_id.
        # The quantity_sold, unit_price, line_total, and line_total_uah fields are updated to reflect any changes in the order lines from the staging data.
        return orders_count

    async def _transform_rozetka_orders(self, conn) -> int:
        """Move Rozetka orders from staging into fact_orders + fact_order_lines."""
        # Rozetka prices are in UAH, so currency_value = 1
        # The SQL query inserts orders from the Rozetka staging table into the fact_orders table, linking to order statuses via a LEFT JOIN to get the corresponding status_id for each order.
        result = await conn.execute(
            """
            INSERT INTO core.fact_orders (
                source_order_id, channel_id, date_id,
                status_id, total_amount, total_amount_uah,
                currency_code, currency_value,
                shipping_city, date_added, date_modified
            )
            SELECT
                o.rozetka_order_id,
                2 AS channel_id,          -- Rozetka = channel 2
                TO_CHAR(o.created, 'YYYYMMDD')::integer AS date_id,
                stat.status_id,
                o.cost_with_discount,
                o.cost_with_discount,     -- already in UAH
                'UAH', 1.0,
                o.delivery_city,
                o.created,
                o.changed
            FROM staging.stg_rozetka_orders o
            LEFT JOIN core.dim_order_status stat
                ON stat.source_id = o.status
               AND stat.source_channel = 'rozetka'
            ON CONFLICT (source_order_id, channel_id) DO UPDATE SET
                status_id        = EXCLUDED.status_id,
                total_amount     = EXCLUDED.total_amount,
                total_amount_uah = EXCLUDED.total_amount_uah,
                date_modified    = EXCLUDED.date_modified
            """
        )
        # The result of the INSERT statement will indicate how many order records were inserted or updated for the Rozetka channel.
        # This is a key metric for understanding the volume of Rozetka order data processed during the transformation.
        orders_count = int(result.split()[-1])

        # Rozetka order lines — match to dim_product by item_name (best-effort)
        # The SQL query inserts order lines from the Rozetka staging table into the fact_order_lines table, linking to products and categories via a LEFT JOIN to get the corresponding product_id and category_id for each line item.
        await conn.execute(
            """
            INSERT INTO core.fact_order_lines (
                source_line_id, order_id,
                product_id, category_id, channel_id, date_id,
                quantity_sold, unit_price, line_total, line_total_uah, tax
            )
            SELECT
                ol.purchase_id,
                fo.order_id,
                prod.product_id,
                prod.category_id,
                2 AS channel_id,
                fo.date_id,
                ol.quantity,
                ol.price,
                ol.cost_with_discount,
                ol.cost_with_discount,    -- UAH
                0 AS tax
            FROM staging.stg_rozetka_order_lines ol
            JOIN core.fact_orders fo
                ON fo.source_order_id = ol.rozetka_order_id
               AND fo.channel_id = 2
            LEFT JOIN core.dim_product prod
                ON LOWER(TRIM(prod.title)) = LOWER(TRIM(ol.item_name))
            ON CONFLICT (source_line_id, channel_id) DO UPDATE SET
                quantity_sold  = EXCLUDED.quantity_sold,
                unit_price     = EXCLUDED.unit_price,
                line_total     = EXCLUDED.line_total,
                line_total_uah = EXCLUDED.line_total_uah
            """
        )
        # The order lines are inserted or updated based on the source_line_id and channel_id.
        # The quantity_sold, unit_price, line_total, and line_total_uah fields are updated to reflect any changes in the order lines from the staging data.
        return orders_count

