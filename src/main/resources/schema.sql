-- Driftwatch Stream schema. Applied on startup by Spring Boot's SQL init.

CREATE TABLE IF NOT EXISTS observations (
    id              BIGSERIAL PRIMARY KEY,
    model_id        TEXT        NOT NULL,
    feature_name    TEXT        NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    predicted_label TEXT,
    observed_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS observations_model_feature_time
    ON observations (model_id, feature_name, observed_at DESC);

CREATE INDEX IF NOT EXISTS observations_observed_at
    ON observations (observed_at);

CREATE TABLE IF NOT EXISTS baselines (
    model_id    TEXT        NOT NULL,
    feature_name TEXT       NOT NULL,
    bins        JSONB       NOT NULL,
    proportions JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (model_id, feature_name)
);

CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    model_id    TEXT        NOT NULL,
    feature_name TEXT       NOT NULL,
    psi         DOUBLE PRECISION NOT NULL,
    ks          DOUBLE PRECISION NOT NULL,
    severity    TEXT        NOT NULL,
    explanation TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alerts_model_created
    ON alerts (model_id, created_at DESC);

-- One alert per model, feature and severity bucket per hour, so a sustained
-- shift does not flood the timeline with duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS alerts_dedupe
    ON alerts (model_id, feature_name, severity, date_trunc('hour', created_at));
