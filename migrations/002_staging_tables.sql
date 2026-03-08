-- =============================================================
-- Migration 002: Staging tables
-- Raw ingested data from both sales channels — never modified
-- =============================================================

-- ── Website source tables ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.stg_website_orders (
    stg_id              BIGSERIAL PRIMARY KEY,
    order_id            INTEGER NOT NULL,
    store_id            INTEGER,
    customer_id         INTEGER,
    customer_group_id   INTEGER,
    payment_city        VARCHAR(128),
    payment_country     VARCHAR(128),
    shipping_city       VARCHAR(128),
    shipping_method     VARCHAR(128),
    payment_method      VARCHAR(128),
    total               NUMERIC(15,4),
    order_status_id     INTEGER,
    currency_code       VARCHAR(3),
    currency_value      NUMERIC(15,8),
    date_added          TIMESTAMP,
    date_modified       TIMESTAMP,
    raw_json            JSONB,
    ingested_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE (order_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_website_order_lines (
    stg_id              BIGSERIAL PRIMARY KEY,
    order_product_id    INTEGER NOT NULL,
    order_id            INTEGER NOT NULL,
    product_id          INTEGER NOT NULL,
    name                VARCHAR(255),
    model               VARCHAR(64),
    quantity            INTEGER,
    price               NUMERIC(15,4),
    total               NUMERIC(15,4),
    tax                 NUMERIC(15,4),
    raw_json            JSONB,
    ingested_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE (order_product_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_website_products (
    stg_id              BIGSERIAL PRIMARY KEY,
    product_id          INTEGER NOT NULL,
    model               VARCHAR(64),
    sku                 VARCHAR(64),
    isbn                VARCHAR(17),
    quantity            INTEGER,
    stock_status_id     INTEGER,
    manufacturer_id     INTEGER,
    price               NUMERIC(15,4),
    date_available      DATE,
    publishing_year     SMALLINT,
    pages_number        SMALLINT,
    author              VARCHAR(100),
    publisher           VARCHAR(150),
    binding_type        VARCHAR(30),
    status              SMALLINT,
    date_added          TIMESTAMP,
    date_modified       TIMESTAMP,
    name                VARCHAR(255),
    description         TEXT,
    main_category_id    INTEGER,
    raw_json            JSONB,
    ingested_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE (product_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_website_categories (
    stg_id          BIGSERIAL PRIMARY KEY,
    category_id     INTEGER NOT NULL,
    parent_id       INTEGER DEFAULT 0,
    status          SMALLINT DEFAULT 1,
    sort_order      INTEGER DEFAULT 0,
    name            VARCHAR(255),
    description     TEXT,
    ingested_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (category_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_website_manufacturers (
    stg_id              BIGSERIAL PRIMARY KEY,
    manufacturer_id     INTEGER NOT NULL,
    name                VARCHAR(64) NOT NULL,
    description         TEXT,
    ingested_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE (manufacturer_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_website_customers (
    stg_id                  BIGSERIAL PRIMARY KEY,
    customer_id             INTEGER NOT NULL,
    customer_group_id       INTEGER,
    store_id                INTEGER,
    is_newsletter           BOOLEAN DEFAULT FALSE,
    date_added              TIMESTAMP,
    customer_group_name     VARCHAR(64),
    ingested_at             TIMESTAMP DEFAULT NOW(),
    UNIQUE (customer_id)
);

-- A simple lookup used by the transformer to resolve stock status labels
CREATE TABLE IF NOT EXISTS staging.stg_website_stock_statuses (
    stock_status_id     INTEGER NOT NULL,
    language_id         INTEGER NOT NULL DEFAULT 1,
    name                VARCHAR(32),
    PRIMARY KEY (stock_status_id, language_id)
);

-- ── Rozetka source tables ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.stg_rozetka_orders (
    stg_id                  BIGSERIAL PRIMARY KEY,
    rozetka_order_id        BIGINT NOT NULL,
    market_id               INTEGER,
    created                 TIMESTAMP,
    changed                 TIMESTAMP,
    amount                  NUMERIC(12,2),
    amount_with_discount    NUMERIC(12,2),
    cost                    NUMERIC(12,2),
    cost_with_discount      NUMERIC(12,2),
    status                  INTEGER,
    status_group            INTEGER,
    user_phone              VARCHAR(32),
    delivery_city           VARCHAR(128),
    delivery_service        VARCHAR(128),
    ttn                     VARCHAR(64),
    total_quantity          INTEGER,
    raw_json                JSONB,
    ingested_at             TIMESTAMP DEFAULT NOW(),
    UNIQUE (rozetka_order_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_rozetka_order_lines (
    stg_id                  BIGSERIAL PRIMARY KEY,
    rozetka_order_id        BIGINT NOT NULL,
    purchase_id             BIGINT NOT NULL,
    item_id                 BIGINT,
    item_name               VARCHAR(512),
    quantity                INTEGER,
    price                   NUMERIC(12,2),
    cost                    NUMERIC(12,2),
    cost_with_discount      NUMERIC(12,2),
    raw_json                JSONB,
    ingested_at             TIMESTAMP DEFAULT NOW(),
    UNIQUE (purchase_id)
);

-- ── Indexes for staging query performance ─────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_stg_wo_date_modified   ON staging.stg_website_orders(date_modified);
CREATE INDEX IF NOT EXISTS idx_stg_wo_customer_id     ON staging.stg_website_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_wol_order_id       ON staging.stg_website_order_lines(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_wp_manufacturer_id ON staging.stg_website_products(manufacturer_id);
CREATE INDEX IF NOT EXISTS idx_stg_rzo_created        ON staging.stg_rozetka_orders(created);
CREATE INDEX IF NOT EXISTS idx_stg_rzol_order_id      ON staging.stg_rozetka_order_lines(rozetka_order_id);
