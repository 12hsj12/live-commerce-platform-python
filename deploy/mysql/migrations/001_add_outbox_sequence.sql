-- Additive Python schema update. Back up the target schema before applying.
ALTER TABLE outbox_events
    ADD COLUMN sequence BIGINT NOT NULL DEFAULT 0;
