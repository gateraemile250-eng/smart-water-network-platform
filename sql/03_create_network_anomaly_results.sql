-- Phase 8: Network-level anomaly detection results
-- Stores outputs from the statistical baseline and Isolation Forest detectors.

CREATE TABLE IF NOT EXISTS network_anomaly_results (
    timestamp TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,

    baseline_sensor_count INTEGER NOT NULL,
    baseline_sensor_fraction DOUBLE PRECISION NOT NULL,
    is_baseline_anomaly BOOLEAN NOT NULL,

    ml_anomaly_score DOUBLE PRECISION NOT NULL,
    is_ml_anomaly BOOLEAN NOT NULL,

    created_at TIMESTAMP WITHOUT TIME ZONE
        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_baseline_sensor_count
        CHECK (baseline_sensor_count >= 0),

    CONSTRAINT chk_baseline_sensor_fraction
        CHECK (
            baseline_sensor_fraction >= 0
            AND baseline_sensor_fraction <= 1
        )
);

CREATE INDEX IF NOT EXISTS idx_network_anomaly_baseline
    ON network_anomaly_results (is_baseline_anomaly)
    WHERE is_baseline_anomaly = TRUE;

CREATE INDEX IF NOT EXISTS idx_network_anomaly_ml
    ON network_anomaly_results (is_ml_anomaly)
    WHERE is_ml_anomaly = TRUE;