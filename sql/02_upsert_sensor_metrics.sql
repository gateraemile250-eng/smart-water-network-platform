-- Merge staged Spark metrics into the serving table.
-- Existing metric keys are updated to keep the load idempotent.

CREATE OR REPLACE PROCEDURE upsert_sensor_metrics()
LANGUAGE SQL
AS $$
    INSERT INTO sensor_metrics_15min (
        window_start,
        window_end,
        sensor_id,
        sensor_type,
        unit,
        reading_count,
        avg_value,
        min_value,
        max_value
    )
    SELECT
        window_start,
        window_end,
        sensor_id,
        sensor_type,
        unit,
        reading_count,
        avg_value,
        min_value,
        max_value
    FROM sensor_metrics_15min_staging

    ON CONFLICT (
        sensor_id,
        sensor_type,
        window_start,
        window_end
    )
    DO UPDATE SET
        unit = EXCLUDED.unit,
        reading_count = EXCLUDED.reading_count,
        avg_value = EXCLUDED.avg_value,
        min_value = EXCLUDED.min_value,
        max_value = EXCLUDED.max_value;
$$;