CREATE TABLE users (
    id VARCHAR(64) PRIMARY KEY,
    username VARCHAR(128) NOT NULL UNIQUE,
    role VARCHAR(32) NOT NULL,
    password_hash VARBINARY(255) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price_cents BIGINT NOT NULL,
    on_sale BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE inventory (
    sku_id VARCHAR(64) PRIMARY KEY,
    available BIGINT NOT NULL,
    reserved BIGINT NOT NULL DEFAULT 0,
    sold BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    CHECK (available >= 0 AND reserved >= 0 AND sold >= 0)
);

CREATE TABLE campaigns (
    id VARCHAR(64) PRIMARY KEY,
    sku_id VARCHAR(64) NOT NULL,
    participant_limit BIGINT NOT NULL CHECK (participant_limit > 0),
    state VARCHAR(32) NOT NULL,
    starts_at BIGINT NOT NULL DEFAULT 0,
    ends_at BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_campaigns_state_updated (state, updated_at),
    INDEX idx_campaigns_sku_state (sku_id, state)
);

CREATE TABLE admin_audit_logs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    actor VARCHAR(128) NOT NULL,
    action VARCHAR(64) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    previous_value VARCHAR(512) NOT NULL DEFAULT '',
    new_value VARCHAR(512) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_admin_audit_resource (resource_type, resource_id, created_at),
    INDEX idx_admin_audit_actor (actor, created_at)
);

CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL DEFAULT '',
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    state VARCHAR(32) NOT NULL,
    expires_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP + INTERVAL 15 MINUTE),
    expiry_claimed_by VARCHAR(128) NULL,
    expiry_lease_until TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    order_id VARCHAR(64) NOT NULL,
    sku_id VARCHAR(64) NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price_cents BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (order_id, sku_id)
);

CREATE TABLE outbox_events (
    event_id VARCHAR(128) PRIMARY KEY,
    topic VARCHAR(128) NOT NULL,
    event_key VARCHAR(128) NOT NULL,
    payload BLOB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    attempts INT NOT NULL DEFAULT 0,
    claimed_by VARCHAR(128) NULL,
    lease_until TIMESTAMP NULL,
    last_error VARCHAR(1024) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_outbox_pending ON outbox_events (published, created_at, event_id);
CREATE INDEX idx_orders_expiry ON orders (state, expires_at);
CREATE INDEX idx_orders_expiry_lease ON orders (state, expires_at, expiry_lease_until);
CREATE INDEX idx_orders_campaign_id ON orders (campaign_id);

CREATE TABLE payments (
    id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    provider_transaction_id VARCHAR(128) NOT NULL UNIQUE,
    amount_cents BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE flash_sale_requests (
    request_id VARCHAR(128) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    state VARCHAR(32) NOT NULL,
    message VARCHAR(512) NOT NULL DEFAULT '',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_flash_sale_request_user (user_id, updated_at)
);
