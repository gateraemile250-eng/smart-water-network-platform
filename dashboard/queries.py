import pandas as pd
from sqlalchemy import text

from dashboard.db import get_engine


# =========================================================
# Database helper
# =========================================================

def fetch_dataframe(sql: str, params=None) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a pandas DataFrame.

    SQLAlchemy text() is used so named parameters such as
    :timestamp are correctly handled before PostgreSQL execution.
    """
    engine = get_engine()

    try:
        return pd.read_sql(
            text(sql),
            engine,
            params=params,
        )
    finally:
        engine.dispose()


# =========================================================
# Network overview
# =========================================================

def get_network_summary() -> pd.DataFrame:
    """
    Return high-level network asset counts.
    """
    sql = """
        SELECT
            (SELECT COUNT(*) FROM sensors) AS sensors,

            (SELECT COUNT(*)
             FROM network_nodes) AS network_nodes,

            (SELECT COUNT(*)
             FROM network_links) AS network_links,

            (SELECT COUNT(*)
             FROM network_links
             WHERE link_type = 'pipe') AS pipes;
    """

    return fetch_dataframe(sql)


# =========================================================
# Anomaly summary
# =========================================================

def get_anomaly_summary() -> pd.DataFrame:
    """
    Return high-level anomaly statistics.
    """
    sql = """
        SELECT
            COUNT(*) AS scoring_timestamps,

            COUNT(*) FILTER (
                WHERE is_ml_anomaly
            ) AS ml_anomaly_timestamps,

            ROUND(
                100.0
                * COUNT(*) FILTER (WHERE is_ml_anomaly)
                / NULLIF(COUNT(*), 0),
                2
            ) AS ml_anomaly_rate_pct,

            COUNT(*) FILTER (
                WHERE is_baseline_anomaly
            ) AS baseline_anomaly_timestamps,

            ROUND(
                100.0
                * COUNT(*) FILTER (WHERE is_baseline_anomaly)
                / NULLIF(COUNT(*), 0),
                2
            ) AS baseline_anomaly_rate_pct,

            MIN(timestamp) AS scoring_start,

            MAX(timestamp) AS scoring_end

        FROM network_anomaly_results;
    """

    return fetch_dataframe(sql)


# =========================================================
# Localization summary
# =========================================================

def get_localization_summary() -> pd.DataFrame:
    """
    Return high-level localization statistics.

    ML anomaly timestamps are compared with timestamps for
    which localization results exist.
    """
    sql = """
        WITH ml AS (
            SELECT DISTINCT timestamp
            FROM network_anomaly_results
            WHERE is_ml_anomaly = TRUE
        ),

        localized AS (
            SELECT DISTINCT timestamp
            FROM network_localization_results
        )

        SELECT

            (
                SELECT COUNT(*)
                FROM ml
            ) AS ml_anomaly_timestamps,

            (
                SELECT COUNT(*)
                FROM localized
            ) AS localized_anomalies,

            (
                SELECT COUNT(*)
                FROM ml
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM localized
                    WHERE localized.timestamp = ml.timestamp
                )
            ) AS unlocalized_anomalies,

            ROUND(
                100.0
                * (
                    SELECT COUNT(*)
                    FROM localized
                )
                / NULLIF(
                    (
                        SELECT COUNT(*)
                        FROM ml
                    ),
                    0
                ),
                2
            ) AS localization_coverage_pct,

            (
                SELECT COUNT(*)
                FROM network_localization_results
            ) AS candidate_rows,

            (
                SELECT COUNT(DISTINCT pipe_id)
                FROM network_localization_results
            ) AS candidate_pipes;

    """

    return fetch_dataframe(sql)


# =========================================================
# Daily anomaly analytics
# =========================================================

def get_daily_anomalies() -> pd.DataFrame:
    """
    Return daily anomaly analytics from the dashboard
    serving view.
    """
    sql = """
        SELECT
            day,
            observation_count,
            ml_anomaly_count,
            baseline_anomaly_count,
            ml_anomaly_rate_pct,
            baseline_anomaly_rate_pct,
            avg_ml_anomaly_score,
            max_ml_anomaly_score

        FROM dashboard_anomaly_daily

        ORDER BY day;
    """

    return fetch_dataframe(sql)


# =========================================================
# Anomaly investigation detail
# =========================================================

def get_anomaly_detail() -> pd.DataFrame:
    """
    Return detailed anomaly investigation data.
    """
    sql = """
        SELECT
            timestamp,
            ml_anomaly_score,
            is_ml_anomaly,
            baseline_sensor_count,
            baseline_sensor_fraction,
            is_baseline_anomaly,
            has_localization,
            investigation_status

        FROM dashboard_anomaly_detail

        ORDER BY timestamp;
    """

    return fetch_dataframe(sql)


# =========================================================
# Localization summary by timestamp
# =========================================================

def get_localization_summary_by_timestamp() -> pd.DataFrame:
    """
    Return localization summaries for localized
    anomaly timestamps.
    """
    sql = """
        SELECT
            timestamp,
            candidate_pipe_count,
            best_candidate_rank,
            top_candidate_score,
            unique_candidate_pipes

        FROM dashboard_localization_summary

        ORDER BY timestamp;
    """

    return fetch_dataframe(sql)


# =========================================================
# Localization candidate pipes
# =========================================================

def get_localization_candidates(timestamp) -> pd.DataFrame:
    """
    Return candidate pipes for one selected anomaly timestamp.
    """
    sql = """
        SELECT
            timestamp,
            candidate_rank,
            pipe_id,
            localization_score,
            link_type,
            from_node_id,
            to_node_id

        FROM dashboard_localization_candidates

        WHERE timestamp = :timestamp

        ORDER BY candidate_rank;
    """

    return fetch_dataframe(
        sql,
        params={
            "timestamp": timestamp,
        },
    )