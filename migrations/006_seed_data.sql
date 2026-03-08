-- =============================================================
-- Migration 006 — Seed data
-- Roles, sales channels, and the dim_date table (2015–2035)
-- Run ONCE after migrations 001–005.
-- =============================================================

-- ── Roles ─────────────────────────────────────────────────────────────────────
INSERT INTO mis.mis_role (role_name, description) VALUES
    ('admin',   'Full access — all data including revenue figures and user management'),
    ('manager', 'Access to dashboards and reports; revenue figures visible'),
    ('viewer',  'Read-only access to non-financial dashboards only')
ON CONFLICT (role_name) DO NOTHING;

-- ── Sales channels ────────────────────────────────────────────────────────────
INSERT INTO core.dim_sales_channel (channel_id, channel_name, channel_url, is_active)
OVERRIDING SYSTEM VALUE VALUES
    (1, 'Website', 'https://myenglishbooks.com.ua', TRUE),
    (2, 'Rozetka',  'https://rozetka.com.ua',        TRUE)
ON CONFLICT (channel_id) DO NOTHING;

-- Reset sequence so the next auto-generated channel_id starts at 3
SELECT setval(pg_get_serial_sequence('core.dim_sales_channel', 'channel_id'), 2);

-- ── Date dimension: 2015-01-01 → 2035-12-31 ──────────────────────────────────
-- Generates one row per calendar day (~7670 rows total).
-- Covers 9 years of historical data plus 9 years future growth.
INSERT INTO core.dim_date (
    date_id, full_date,
    day_of_month, day_name, day_of_week,
    week_of_year, month, month_name,
    quarter, year,
    is_weekend, season
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER                         AS date_id,
    d::DATE                                                  AS full_date,
    EXTRACT(DAY     FROM d)::SMALLINT                        AS day_of_month,
    TO_CHAR(d, 'Day')                                        AS day_name,
    EXTRACT(ISODOW  FROM d)::SMALLINT                        AS day_of_week,
    EXTRACT(WEEK    FROM d)::SMALLINT                        AS week_of_year,
    EXTRACT(MONTH   FROM d)::SMALLINT                        AS month,
    TO_CHAR(d, 'Month')                                      AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT                        AS quarter,
    EXTRACT(YEAR    FROM d)::SMALLINT                        AS year,
    EXTRACT(ISODOW  FROM d) IN (6, 7)                        AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (3,4,5)   THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6,7,8)   THEN 'Summer'
        WHEN EXTRACT(MONTH FROM d) IN (9,10,11) THEN 'Autumn'
        ELSE 'Winter'
    END                                                      AS season
FROM generate_series('2015-01-01'::DATE, '2035-12-31'::DATE, '1 day') AS g(d)
ON CONFLICT (date_id) DO NOTHING;

-- ── Website order status labels (standard OpenCart defaults) ──────────────────
-- Update status_group values if your OpenCart uses different IDs.
-- status_group: 1=Processing  2=Completed/Successful  3=Cancelled/Failed
INSERT INTO core.dim_order_status (source_id, source_channel, name_en, status_group) VALUES
    (1,  'website', 'Pending',              1),
    (2,  'website', 'Processing',           1),
    (3,  'website', 'Shipped',              1),
    (5,  'website', 'Complete',             2),
    (7,  'website', 'Cancelled',            3),
    (8,  'website', 'Denied',               3),
    (9,  'website', 'Cancelled Reversal',   3),
    (10, 'website', 'Failed',               3),
    (11, 'website', 'Refunded',             3),
    (12, 'website', 'Reversed',             3),
    (13, 'website', 'Chargeback',           3),
    (14, 'website', 'Expired',              3),
    (15, 'website', 'Processed',            2),
    (16, 'website', 'Voided',               3)
ON CONFLICT (source_id, source_channel) DO NOTHING;

-- ── Rozetka order status labels (from Rozetka Seller API docs) ────────────────
INSERT INTO core.dim_order_status (source_id, source_channel, name_en, status_group) VALUES
    (1,  'rozetka', 'New order',                              1),
    (2,  'rozetka', 'Confirmed — wait for shipment',          1),
    (3,  'rozetka', 'Transferred to delivery service',        1),
    (4,  'rozetka', 'Delivering',                             1),
    (5,  'rozetka', 'Stored in local pickup station',         1),
    (6,  'rozetka', 'Parcel received',                        2),
    (7,  'rozetka', 'Was not processed by seller',            3),
    (10, 'rozetka', 'Dispatch is delayed',                    1),
    (11, 'rozetka', 'Parcel was not picked up',               3),
    (12, 'rozetka', 'Product was rejected',                   3),
    (13, 'rozetka', 'Cancelled by Administrator',             3),
    (15, 'rozetka', 'Incorrect waybill',                      3),
    (16, 'rozetka', 'Out of stock / defective',               3),
    (17, 'rozetka', 'Cancellation — payment unacceptable',    3),
    (18, 'rozetka', 'Cancellation — fail to contact buyer',   3),
    (19, 'rozetka', 'Return',                                 3),
    (20, 'rozetka', 'Cancellation — product unacceptable',    3),
    (26, 'rozetka', 'Processing',                             1),
    (40, 'rozetka', 'Cancellation — customer changed mind',   3),
    (42, 'rozetka', 'Out of stock',                           3),
    (45, 'rozetka', 'Cancelled by buyer',                     3),
    (50, 'rozetka', 'Cancellation — order not paid',          3)
ON CONFLICT (source_id, source_channel) DO NOTHING;

-- ── Default alert rules ───────────────────────────────────────────────────────
-- Sensible starting thresholds; the admin can adjust them in the UI.
-- created_by is NULL because no admin user exists yet at migration time.
INSERT INTO mis.alert_rule (rule_type, rule_name, threshold, cooldown_hours, is_active)
VALUES
    ('low_stock',  'Low Stock Warning (≤5 copies)',     5,  24, TRUE),
    ('low_stock',  'Critical Stock (last copy)',         1,  12, TRUE),
    ('no_orders',  'No Orders in 48 Hours',             48,  48, TRUE)
ON CONFLICT DO NOTHING;

