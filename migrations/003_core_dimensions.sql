-- =============================================================
-- Migration 003: Core dimension tables (star schema)
-- =============================================================

-- ── Date dimension ─────────────────────────────────────────────────────────
-- Pre-populated by migration 006_seed_data.sql for 2015-01-01 → 2035-12-31

CREATE TABLE IF NOT EXISTS core.dim_date (
    date_id         INTEGER PRIMARY KEY,    -- YYYYMMDD e.g. 20260308
    full_date       DATE NOT NULL UNIQUE,
    day_of_month    SMALLINT,
    day_name        VARCHAR(10),
    day_of_week     SMALLINT,               -- 1=Mon … 7=Sun
    week_of_year    SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    season          VARCHAR(10)             -- Spring | Summer | Autumn | Winter
);

-- ── Sales channel dimension ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.dim_sales_channel (
    channel_id      SERIAL PRIMARY KEY,
    channel_name    VARCHAR(64) NOT NULL,
    channel_url     VARCHAR(255),
    is_active       BOOLEAN DEFAULT TRUE
);

-- ── Category dimension ─────────────────────────────────────────────────────
-- Two-level hierarchy: top-level = age group / genre segment (is_age_group=TRUE)
-- Child categories = sub-genres

CREATE TABLE IF NOT EXISTS core.dim_category (
    category_id         SERIAL PRIMARY KEY,
    source_category_id  INTEGER,
    source_channel      VARCHAR(16),        -- 'website' | 'rozetka' | 'mis'
    name                VARCHAR(255) NOT NULL,
    parent_id           INTEGER REFERENCES core.dim_category(category_id),
    parent_name         VARCHAR(255),
    is_age_group        BOOLEAN DEFAULT FALSE,
    sort_order          INTEGER DEFAULT 0,
    UNIQUE (source_category_id, source_channel)
);

-- ── Publisher dimension ────────────────────────────────────────────────────
-- OpenCart calls these "manufacturers"; in bookshop context they are publishers

CREATE TABLE IF NOT EXISTS core.dim_publisher (
    publisher_id            SERIAL PRIMARY KEY,
    source_manufacturer_id  INTEGER,
    name                    VARCHAR(64) NOT NULL,
    description             TEXT,
    UNIQUE (source_manufacturer_id)
);

-- ── Product dimension ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.dim_product (
    product_id          SERIAL PRIMARY KEY,
    source_product_id   INTEGER,
    source_channel      VARCHAR(16) DEFAULT 'website',
    title               VARCHAR(255) NOT NULL,
    author              VARCHAR(100),
    isbn                VARCHAR(17),
    publisher_id        INTEGER REFERENCES core.dim_publisher(publisher_id),
    publishing_year     SMALLINT,
    pages_number        SMALLINT,
    binding_type        VARCHAR(30),        -- Hardcover / Paperback (pereplet)
    category_id         INTEGER REFERENCES core.dim_category(category_id),
    price               NUMERIC(15,4),
    quantity_in_stock   INTEGER DEFAULT 0,
    stock_status        VARCHAR(32),
    status              VARCHAR(16),        -- 'active' | 'inactive'
    date_available      DATE,
    date_added          TIMESTAMP,
    last_updated        TIMESTAMP DEFAULT NOW(),
    UNIQUE (source_product_id, source_channel)
);

-- ── Customer dimension (PII-minimised) ────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.dim_customer (
    customer_id         SERIAL PRIMARY KEY,
    source_customer_id  INTEGER,
    source_channel      VARCHAR(16),
    customer_group      VARCHAR(32),
    city                VARCHAR(128),
    country             VARCHAR(128),
    date_registered     DATE,
    is_newsletter       BOOLEAN,
    UNIQUE (source_customer_id, source_channel)
);

-- ── Order status dimension ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.dim_order_status (
    status_id       SERIAL PRIMARY KEY,
    source_id       INTEGER,
    source_channel  VARCHAR(16),
    name_en         VARCHAR(64) NOT NULL,
    status_group    SMALLINT,   -- 1=Processing | 2=Completed | 3=Cancelled
    UNIQUE (source_id, source_channel)
);
