-- =============================================================
-- Migration 004: Core fact tables + indexes
-- =============================================================

-- ── Fact: one row per order ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.fact_orders (
    order_id            BIGSERIAL PRIMARY KEY,
    source_order_id     BIGINT NOT NULL,
    channel_id          INTEGER NOT NULL REFERENCES core.dim_sales_channel(channel_id),
    date_id             INTEGER NOT NULL REFERENCES core.dim_date(date_id),
    customer_id         INTEGER REFERENCES core.dim_customer(customer_id),
    status_id           INTEGER REFERENCES core.dim_order_status(status_id),
    total_amount        NUMERIC(15,4),
    total_amount_uah    NUMERIC(15,4),
    currency_code       VARCHAR(3),
    currency_value      NUMERIC(15,8),
    shipping_city       VARCHAR(128),
    shipping_method     VARCHAR(128),
    payment_method      VARCHAR(128),
    date_added          TIMESTAMP,
    date_modified       TIMESTAMP,
    UNIQUE (source_order_id, channel_id)
);

-- ── Fact: one row per order line item ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.fact_order_lines (
    line_id             BIGSERIAL PRIMARY KEY,
    source_line_id      BIGINT NOT NULL,
    order_id            BIGINT NOT NULL REFERENCES core.fact_orders(order_id),
    product_id          INTEGER REFERENCES core.dim_product(product_id),
    category_id         INTEGER REFERENCES core.dim_category(category_id),
    channel_id          INTEGER NOT NULL REFERENCES core.dim_sales_channel(channel_id),
    date_id             INTEGER NOT NULL REFERENCES core.dim_date(date_id),
    quantity_sold       INTEGER NOT NULL,
    unit_price          NUMERIC(15,4),
    line_total          NUMERIC(15,4),
    line_total_uah      NUMERIC(15,4),
    tax                 NUMERIC(15,4),
    UNIQUE (source_line_id, channel_id)
);

-- ── Analytical indexes ─────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fact_orders_date_id     ON core.fact_orders(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_channel_id  ON core.fact_orders(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status_id   ON core.fact_orders(status_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date_added  ON core.fact_orders(date_added);
CREATE INDEX IF NOT EXISTS idx_fact_lines_product_id   ON core.fact_order_lines(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_lines_category_id  ON core.fact_order_lines(category_id);
CREATE INDEX IF NOT EXISTS idx_fact_lines_date_id      ON core.fact_order_lines(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_lines_channel_id   ON core.fact_order_lines(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_lines_order_id     ON core.fact_order_lines(order_id);
