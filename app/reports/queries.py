"""
Pre-defined analytical SQL queries used by the report generator
and the admin dashboard.

All queries return rows compatible with the Pydantic response models
defined in app/models/mis.py.
"""

# ── Dashboard KPIs ────────────────────────────────────────────────────────────

# This query aggregates multiple KPIs into a single result set for efficient retrieval on the dashboard. It includes:
# - Month-to-date revenue and order count
# - Best-selling book title and units sold for the current month
# - Count of products with low stock based on active alert rules
# - Timestamps of the last successful sync for website and Rozetka channels
KPI_SUMMARY = """
WITH mtd AS (
    SELECT
        COALESCE(SUM(fo.total_amount_uah), 0)   AS total_revenue_mtd,
        COUNT(*)                                  AS orders_mtd
    FROM core.fact_orders fo
    WHERE DATE_TRUNC('month', fo.date_added) = DATE_TRUNC('month', NOW())
      AND fo.status_id IN (
          SELECT status_id FROM core.dim_order_status
          WHERE status_group = 2   -- successfully completed
      )
),
top_book AS (
    SELECT p.title, SUM(l.quantity_sold) AS units
    FROM core.fact_order_lines l
    JOIN core.dim_product p ON p.product_id = l.product_id
    WHERE DATE_TRUNC('month', l.date_id::text::date) = DATE_TRUNC('month', NOW())
    GROUP BY p.title
    ORDER BY units DESC
    LIMIT 1
),
low_stock AS (
    SELECT COUNT(*) AS cnt
    FROM core.dim_product p
    JOIN mis.alert_rule r ON r.rule_type = 'low_stock' AND r.is_active = TRUE
    WHERE p.quantity_in_stock <= r.threshold
      AND p.status = 'active'
),
last_sync AS (
    SELECT
        MAX(CASE WHEN channel = 'website' THEN completed_at END) AS last_website,
        MAX(CASE WHEN channel = 'rozetka' THEN completed_at END) AS last_rozetka
    FROM mis.sync_log
    WHERE status = 'success'
)
SELECT
    mtd.total_revenue_mtd,
    mtd.orders_mtd,
    top_book.title  AS top_book_title,
    top_book.units  AS top_book_units,
    low_stock.cnt   AS low_stock_count,
    last_sync.last_website,
    last_sync.last_rozetka
FROM mtd, low_stock, last_sync
LEFT JOIN top_book ON TRUE
"""

# ── Revenue by channel (last 24 months) ──────────────────────────────────────
# This query generates a monthly revenue and order count breakdown by sales channel for the last 24 months.
# It uses a CROSS JOIN to ensure all combinations of months and channels are included, even if there were no sales (resulting in zero revenue and order count for those cases).
REVENUE_BY_CHANNEL = """
SELECT
    d.year,
    d.month,
    d.month_name,
    sc.channel_name,
    COALESCE(SUM(fo.total_amount_uah), 0)  AS total_revenue_uah,
    COUNT(fo.order_id)                      AS order_count
FROM core.dim_date d
CROSS JOIN core.dim_sales_channel sc
LEFT JOIN core.fact_orders fo
    ON  fo.date_id    = d.date_id
    AND fo.channel_id = sc.channel_id
    AND fo.status_id IN (
        SELECT status_id FROM core.dim_order_status WHERE status_group = 2
    )
WHERE d.full_date BETWEEN :from_date AND :to_date
GROUP BY d.year, d.month, d.month_name, sc.channel_name
ORDER BY d.year, d.month, sc.channel_name
"""

# ── Top N books by units sold ─────────────────────────────────────────────────
# This query identifies the top-selling books within a specified date range, optionally filtered by sales channel. It aggregates total units sold and revenue for each book, and orders the results by units sold in descending order, limited to the top N results as specified by the :limit parameter.
TOP_BOOKS = """
SELECT
    p.product_id,
    p.title,
    p.author,
    SUM(l.quantity_sold)      AS units_sold,
    SUM(l.line_total_uah)     AS revenue_uah
FROM core.fact_order_lines l
JOIN core.dim_product p  ON p.product_id = l.product_id
JOIN core.fact_orders fo ON fo.order_id  = l.order_id
WHERE fo.date_added BETWEEN :from_date AND :to_date
  AND (:channel_id::int IS NULL OR l.channel_id = :channel_id::int)
  AND fo.status_id IN (
      SELECT status_id FROM core.dim_order_status WHERE status_group = 2
  )
GROUP BY p.product_id, p.title, p.author
ORDER BY units_sold DESC
LIMIT :limit::int
"""

# ── Sales by age group / category ────────────────────────────────────────────
# This query calculates sales performance by product category, including total units sold, revenue, and percentage of total revenue for each category.
# It also includes the parent category name to allow for analysis by age group or other category hierarchies.
# The results are ordered by revenue in descending order.
SALES_BY_CATEGORY = """
WITH totals AS (
    SELECT SUM(l.line_total_uah) AS grand_total
    FROM core.fact_order_lines l
    JOIN core.fact_orders fo ON fo.order_id = l.order_id
    WHERE fo.date_added BETWEEN :from_date AND :to_date
      AND (:channel_id::int IS NULL OR l.channel_id = :channel_id::int)
      AND fo.status_id IN (
          SELECT status_id FROM core.dim_order_status WHERE status_group = 2
      )
)
SELECT
    c.name          AS category_name,
    c.parent_name,
    SUM(l.quantity_sold)  AS units_sold,
    SUM(l.line_total_uah) AS revenue_uah,
    ROUND(100.0 * SUM(l.line_total_uah) / NULLIF(t.grand_total, 0), 2) AS pct_of_total
FROM core.fact_order_lines l
JOIN core.dim_category c ON c.category_id = l.category_id
JOIN core.fact_orders fo ON fo.order_id   = l.order_id
CROSS JOIN totals t
WHERE fo.date_added BETWEEN :from_date AND :to_date
  AND (:channel_id::int IS NULL OR l.channel_id = :channel_id::int)
  AND fo.status_id IN (
      SELECT status_id FROM core.dim_order_status WHERE status_group = 2
  )
GROUP BY c.name, c.parent_name, t.grand_total
ORDER BY revenue_uah DESC
"""

# ── Seasonal trends (monthly revenue for line chart) ─────────────────────────
# This query aggregates monthly revenue and order count data by season, allowing for analysis of seasonal trends in sales performance.
# It includes all months in the specified date range, even those with no sales, by using a LEFT JOIN between the date dimension and the orders fact table.
# The results are ordered chronologically by year and month to facilitate line chart visualization of trends over time.
SEASONAL_TREND = """
SELECT
    d.year,
    d.month,
    d.month_name,
    d.season,
    COALESCE(SUM(fo.total_amount_uah), 0) AS total_revenue_uah,
    COUNT(fo.order_id)                     AS order_count
FROM core.dim_date d
LEFT JOIN core.fact_orders fo
    ON fo.date_id = d.date_id
    AND (:channel_id::int IS NULL OR fo.channel_id = :channel_id::int)
    AND fo.status_id IN (
        SELECT status_id FROM core.dim_order_status WHERE status_group = 2
    )
WHERE d.full_date BETWEEN :from_date AND :to_date
GROUP BY d.year, d.month, d.month_name, d.season
ORDER BY d.year, d.month
"""

# ── Inventory status ──────────────────────────────────────────────────────────
# This query retrieves the current inventory status of products, including stock levels, pricing, and category information.
# It allows for optional filtering by product status and category, and orders the results by quantity in stock and product title for easy identification of low-stock items.
INVENTORY_STATUS = """
SELECT
    p.product_id,
    p.title,
    p.author,
    p.isbn,
    p.quantity_in_stock,
    p.price,
    p.stock_status,
    p.status,
    c.name AS category_name,
    c.parent_name AS age_group
FROM core.dim_product p
LEFT JOIN core.dim_category c ON c.category_id = p.category_id
WHERE (:status::text IS NULL OR p.status = :status::text)
  AND (:category_id::int IS NULL OR p.category_id = :category_id::int)
ORDER BY p.quantity_in_stock ASC, p.title ASC
"""

# ── Weekly sales summary (for scheduled report) ───────────────────────────────
# This query generates a summary of sales performance for the past week, broken down by sales channel.
# It includes total order count, revenue, units sold, and average order value for each channel, and orders the results by revenue in descending order to highlight the most successful channels.
WEEKLY_SALES_SUMMARY = """
SELECT
    sc.channel_name,
    COUNT(fo.order_id)               AS order_count,
    SUM(fo.total_amount_uah)         AS revenue_uah,
    SUM(l.quantity_sold)             AS units_sold,
    AVG(fo.total_amount_uah)         AS avg_order_value_uah
FROM core.fact_orders fo
JOIN core.dim_sales_channel sc ON sc.channel_id = fo.channel_id
JOIN core.fact_order_lines l   ON l.order_id    = fo.order_id
WHERE fo.date_added >= NOW() - INTERVAL '7 days'
  AND fo.status_id IN (
      SELECT status_id FROM core.dim_order_status WHERE status_group = 2
  )
GROUP BY sc.channel_name
ORDER BY revenue_uah DESC
"""

