-- Phase 9: Network anomaly localization results
-- Stores ranked candidate pipes produced by the localization engine.
--
-- Ground-truth leakage information is intentionally excluded.
-- Geometry remains in network_links and can be joined through pipe_id
-- for PostgreSQL/PostGIS analysis and QGIS visualization.


CREATE TABLE IF NOT EXISTS network_localization_results (
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    pipe_id VARCHAR(100) NOT NULL,

    localization_score DOUBLE PRECISION NOT NULL,

    candidate_rank INTEGER NOT NULL,

    created_at TIMESTAMP WITHOUT TIME ZONE
        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_network_localization_results
        PRIMARY KEY (
            timestamp,
            pipe_id
        ),

    CONSTRAINT fk_localization_anomaly
        FOREIGN KEY (timestamp)
        REFERENCES network_anomaly_results(timestamp)
        ON DELETE CASCADE,

    CONSTRAINT fk_localization_pipe
        FOREIGN KEY (pipe_id)
        REFERENCES network_links(link_id),

    CONSTRAINT chk_localization_score
        CHECK (localization_score >= 0),

    CONSTRAINT chk_localization_rank
        CHECK (candidate_rank > 0)
);


-- Efficiently retrieve candidates for one anomaly timestamp
-- in localization-rank order.

CREATE INDEX IF NOT EXISTS idx_localization_timestamp_rank
    ON network_localization_results (
        timestamp,
        candidate_rank
    );


-- Efficiently retrieve all localization results associated
-- with a particular candidate pipe.

CREATE INDEX IF NOT EXISTS idx_localization_pipe
    ON network_localization_results (
        pipe_id
    );