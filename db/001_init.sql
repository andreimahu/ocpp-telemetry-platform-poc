CREATE TABLE IF NOT EXISTS status_history (
    event_id TEXT PRIMARY KEY,
    charger_id TEXT NOT NULL,
    connector_id SMALLINT NOT NULL CHECK (connector_id BETWEEN 0 AND 2),
    status TEXT NOT NULL,
    error_code TEXT NOT NULL,
    reported_at TIMESTAMPTZ,
    gateway_received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS status_history_charger_time_idx
    ON status_history (
        charger_id,
        connector_id,
        (COALESCE(reported_at, gateway_received_at)) DESC
    );

CREATE TABLE IF NOT EXISTS latest_status (
    charger_id TEXT NOT NULL,
    connector_id SMALLINT NOT NULL CHECK (connector_id BETWEEN 0 AND 2),
    status TEXT NOT NULL,
    error_code TEXT NOT NULL,
    reported_at TIMESTAMPTZ,
    gateway_received_at TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (charger_id, connector_id)
);

CREATE INDEX IF NOT EXISTS latest_status_charger_idx
    ON latest_status (charger_id, connector_id);
