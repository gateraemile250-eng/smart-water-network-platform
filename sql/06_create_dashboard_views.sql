-- Phase 10: Dashboard analytics views
-- Provides a clean PostgreSQL serving layer for operational analytics.
--
-- Important:
-- Anomalies represent abnormal network behaviour and must not be
-- interpreted as confirmed leakage events.


-- ============================================================
-- 1. Daily anomaly analytics
-- ============================================================

CREATE OR REPLACE VIEW dashboard_anomaly_daily AS
SELECT
    timestamp::date AS day,

    COUNT(*) AS observation_count,

    COUNT(*) FILTER (
        WHERE is_ml_anomaly
    ) AS ml_anomaly_count,

    COUNT(*) FILTER (
        WHERE is_baseline_anomaly
    ) AS baseline_anomaly_count,

    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_ml_anomaly)
        / COUNT(*),
        2
    ) AS ml_anomaly_rate_pct,

    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_baseline_anomaly)
        / COUNT(*),
        2
    ) AS baseline_anomaly_rate_pct,

    AVG(ml_anomaly_score) AS avg_ml_anomaly_score,

    MAX(ml_anomaly_score) AS max_ml_anomaly_score

FROM network_anomaly_results

GROUP BY timestamp::date;

-- ============================================================
-- 2. Detailed anomaly investigation
-- ============================================================

CREATE OR REPLACE VIEW dashboard_anomaly_detail AS
SELECT
    ar.timestamp,

    ar.ml_anomaly_score,
    ar.is_ml_anomaly,

    ar.baseline_sensor_count,
    ar.baseline_sensor_fraction,
    ar.is_baseline_anomaly,

    EXISTS (
        SELECT 1
        FROM network_localization_results AS lr
        WHERE lr.timestamp = ar.timestamp
    ) AS has_localization,

    CASE
        WHEN ar.is_ml_anomaly
             AND EXISTS (
                 SELECT 1
                 FROM network_localization_results AS lr
                 WHERE lr.timestamp = ar.timestamp
             )
            THEN 'Localized anomaly'

        WHEN ar.is_ml_anomaly
            THEN 'Anomaly without localization'

        ELSE 'No ML anomaly'
    END AS investigation_status

FROM network_anomaly_results AS ar;

-- ============================================================
-- 3. Localization summary per anomaly timestamp
-- ============================================================

CREATE OR REPLACE VIEW dashboard_localization_summary AS
SELECT
    timestamp,

    COUNT(*) AS candidate_pipe_count,

    MIN(candidate_rank) AS best_candidate_rank,

    MAX(localization_score)
        FILTER (WHERE candidate_rank = 1)
        AS top_candidate_score,

    COUNT(DISTINCT pipe_id) AS unique_candidate_pipes

FROM network_localization_results

GROUP BY timestamp;

-- ============================================================
-- 4. Localization candidate investigation
-- ============================================================

CREATE OR REPLACE VIEW dashboard_localization_candidates AS
SELECT
    lr.timestamp,

    lr.candidate_rank,

    lr.pipe_id,

    lr.localization_score,

    nl.link_type,

    nl.from_node_id,

    nl.to_node_id,

    nl.geometry

FROM network_localization_results AS lr

JOIN network_links AS nl
    ON nl.link_id = lr.pipe_id;