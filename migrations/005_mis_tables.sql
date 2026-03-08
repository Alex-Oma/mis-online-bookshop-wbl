-- =============================================================
-- Migration 005 — MIS application tables
-- Users, roles, alerts, audit log, reports, sync log
-- =============================================================

-- ── Users ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mis.mis_user (
    user_id         SERIAL PRIMARY KEY,
    username        VARCHAR(64) NOT NULL UNIQUE,
    email           VARCHAR(96) NOT NULL UNIQUE,
    password_hash   VARCHAR(128) NOT NULL,   -- bcrypt hash, never plain-text
    full_name       VARCHAR(128),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_login      TIMESTAMP
);

-- ── Roles ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mis.mis_role (
    role_id         SERIAL PRIMARY KEY,
    role_name       VARCHAR(32) NOT NULL UNIQUE,
    description     TEXT
);

-- ── User ↔ Role (many-to-many, in practice 1 role per user for this MIS) ─────
CREATE TABLE IF NOT EXISTS mis.mis_user_role (
    user_id     INTEGER NOT NULL REFERENCES mis.mis_user(user_id) ON DELETE CASCADE,
    role_id     INTEGER NOT NULL REFERENCES mis.mis_role(role_id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

-- ── Audit log (append-only — no UPDATE or DELETE allowed) ────────────────────
CREATE TABLE IF NOT EXISTS mis.audit_log (
    log_id          BIGSERIAL PRIMARY KEY,
    user_id         INTEGER REFERENCES mis.mis_user(user_id),
    action          VARCHAR(64) NOT NULL,
    resource        VARCHAR(128),
    ip_address      INET,
    detail          JSONB,
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_log_user_id    ON mis.audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON mis.audit_log(created_at DESC);

-- ── Alert rules ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mis.alert_rule (
    rule_id         SERIAL PRIMARY KEY,
    rule_type       VARCHAR(32) NOT NULL,   -- 'low_stock' | 'revenue_spike' | 'no_orders'
    rule_name       VARCHAR(128) NOT NULL,
    threshold       NUMERIC(12,2) NOT NULL,
    channel_id      INTEGER REFERENCES core.dim_sales_channel(channel_id),
    category_id     INTEGER REFERENCES core.dim_category(category_id),
    is_active       BOOLEAN DEFAULT TRUE,
    cooldown_hours  INTEGER DEFAULT 24,
    notify_email    VARCHAR(96),
    created_by      INTEGER REFERENCES mis.mis_user(user_id),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ── Alert events (fired instances of rules) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS mis.alert_event (
    event_id        BIGSERIAL PRIMARY KEY,
    rule_id         INTEGER NOT NULL REFERENCES mis.alert_rule(rule_id),
    product_id      INTEGER REFERENCES core.dim_product(product_id),
    triggered_at    TIMESTAMP DEFAULT NOW(),
    detail          JSONB,
    email_sent      BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_alert_event_rule_id ON mis.alert_event(rule_id);
CREATE INDEX IF NOT EXISTS idx_alert_event_product  ON mis.alert_event(product_id);

-- ── Generated / scheduled reports ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mis.scheduled_report (
    report_id       SERIAL PRIMARY KEY,
    report_type     VARCHAR(64) NOT NULL,
    format          VARCHAR(8),                -- 'pdf' | 'xlsx'
    parameters      JSONB,
    file_path       VARCHAR(512),
    generated_at    TIMESTAMP DEFAULT NOW(),
    generated_by    INTEGER REFERENCES mis.mis_user(user_id),
    schedule        VARCHAR(32)                -- cron expression (optional)
);

-- ── Ingestion sync log ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mis.sync_log (
    sync_id          SERIAL PRIMARY KEY,
    channel          VARCHAR(16) NOT NULL,   -- 'website' | 'rozetka'
    sync_type        VARCHAR(16),            -- 'scheduled' | 'manual' | 'historical'
    started_at       TIMESTAMP,
    completed_at     TIMESTAMP,
    records_ingested INTEGER DEFAULT 0,
    records_failed   INTEGER DEFAULT 0,
    status           VARCHAR(16),            -- 'running' | 'success' | 'failed'
    error_detail     TEXT
);
CREATE INDEX IF NOT EXISTS idx_sync_log_channel ON mis.sync_log(channel, completed_at DESC);

