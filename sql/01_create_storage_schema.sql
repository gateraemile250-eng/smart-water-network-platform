-- Smart Water Network Intelligence Platform
-- Core PostgreSQL/PostGIS storage schema


-- ============================================================
-- PostGIS
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;


-- ============================================================
-- 1. Sensor metadata
-- ============================================================

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(20) NOT NULL,
    asset_id VARCHAR(100) NOT NULL,
    asset_type VARCHAR(20) NOT NULL,
    unit VARCHAR(20) NOT NULL,

    CONSTRAINT pk_sensors
        PRIMARY KEY (sensor_id, sensor_type),

    CONSTRAINT chk_sensors_sensor_type
        CHECK (
            sensor_type IN (
                'pressure',
                'flow',
                'level',
                'demand'
            )
        ),

    CONSTRAINT chk_sensors_asset_type
        CHECK (asset_type IN ('node', 'link'))
);


-- ============================================================
-- 2. Fifteen-minute sensor metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS sensor_metrics_15min (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(20) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    reading_count INTEGER NOT NULL,
    avg_value DOUBLE PRECISION NOT NULL,
    min_value DOUBLE PRECISION NOT NULL,
    max_value DOUBLE PRECISION NOT NULL,

    CONSTRAINT pk_sensor_metrics_15min
        PRIMARY KEY (
            sensor_id,
            sensor_type,
            window_start,
            window_end
        ),

    CONSTRAINT fk_sensor_metrics_sensor
        FOREIGN KEY (sensor_id, sensor_type)
        REFERENCES sensors (sensor_id, sensor_type),

    CONSTRAINT chk_sensor_metrics_sensor_type
        CHECK (
            sensor_type IN (
                'pressure',
                'flow',
                'level',
                'demand'
            )
        ),

    CONSTRAINT chk_sensor_metrics_window
        CHECK (window_end > window_start),

    CONSTRAINT chk_sensor_metrics_reading_count
        CHECK (reading_count > 0),

    CONSTRAINT chk_sensor_metrics_values
        CHECK (
            min_value <= avg_value
            AND avg_value <= max_value
        )
);


-- ============================================================
-- 3. Sensor metrics staging
-- ============================================================

CREATE TABLE IF NOT EXISTS sensor_metrics_15min_staging (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(20) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    reading_count INTEGER NOT NULL,
    avg_value DOUBLE PRECISION NOT NULL,
    min_value DOUBLE PRECISION NOT NULL,
    max_value DOUBLE PRECISION NOT NULL
);


-- ============================================================
-- 4. Water-network nodes
-- ============================================================

CREATE TABLE IF NOT EXISTS network_nodes (
    node_id VARCHAR(100) PRIMARY KEY,
    node_type VARCHAR(20) NOT NULL,
    geometry GEOMETRY(POINT),

    CONSTRAINT chk_network_nodes_type
        CHECK (
            node_type IN (
                'junction',
                'tank',
                'reservoir'
            )
        )
);


-- ============================================================
-- 5. Water-network links
-- ============================================================

CREATE TABLE IF NOT EXISTS network_links (
    link_id VARCHAR(100) PRIMARY KEY,
    link_type VARCHAR(20) NOT NULL,
    from_node_id VARCHAR(100) NOT NULL,
    to_node_id VARCHAR(100) NOT NULL,
    geometry GEOMETRY(LINESTRING),

    CONSTRAINT chk_network_links_type
        CHECK (
            link_type IN (
                'pipe',
                'pump',
                'valve'
            )
        ),

    CONSTRAINT fk_network_links_from_node
        FOREIGN KEY (from_node_id)
        REFERENCES network_nodes(node_id),

    CONSTRAINT fk_network_links_to_node
        FOREIGN KEY (to_node_id)
        REFERENCES network_nodes(node_id)
);