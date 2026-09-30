-- Phase 9: QGIS localization view
-- Exposes ranked candidate pipes with network geometry for spatial analysis.
--
-- Ground-truth leakage information is intentionally excluded.
-- Geometry retains the BattLeDIM network/model coordinate system.

DROP VIEW IF EXISTS localization_qgis_view;

CREATE VIEW localization_qgis_view AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY lr.timestamp, lr.candidate_rank, lr.pipe_id
    )::BIGINT AS feature_id,
    lr.timestamp,
    lr.pipe_id,
    lr.candidate_rank,
    lr.localization_score,
    nl.link_type,
    nl.from_node_id,
    nl.to_node_id,
    nl.geometry
FROM network_localization_results AS lr
JOIN network_links AS nl
    ON nl.link_id = lr.pipe_id;