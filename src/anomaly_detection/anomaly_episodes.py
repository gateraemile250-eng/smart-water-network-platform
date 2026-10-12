"""Build persistent anomaly episodes from network anomaly results."""

from __future__ import annotations

import pandas as pd


PERSISTENCE_OBSERVATIONS = 2
EXPECTED_INTERVAL_MINUTES = 5


EPISODE_COLUMNS = [
    "episode_id",
    "start_time",
    "end_time",
    "duration_minutes",
    "observation_count",
    "max_anomaly_score",
    "mean_anomaly_score",
]


def apply_persistence(
    df: pd.DataFrame,
    anomaly_column: str = "is_ml_anomaly",
    persistence_observations: int = PERSISTENCE_OBSERVATIONS,
) -> pd.DataFrame:
    """
    Identify complete anomalous runs that satisfy the persistence rule.

    A run is considered persistent when it contains at least the required
    number of consecutive anomalous observations.

    The complete run is retained, including the first anomalous observation.
    """

    if persistence_observations < 1:
        raise ValueError(
            "persistence_observations must be at least 1."
        )

    required_columns = {
        "event_time",
        anomaly_column,
        "ml_anomaly_score",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    result = (
        df[
            [
                "event_time",
                anomaly_column,
                "ml_anomaly_score",
            ]
        ]
        .copy()
        .sort_values("event_time")
        .reset_index(drop=True)
    )

    result["event_time"] = pd.to_datetime(
        result["event_time"]
    )

    result[anomaly_column] = (
        result[anomaly_column]
        .astype(bool)
    )

    # A new run begins when:
    #   1. the anomaly flag changes, or
    #   2. the timestamp is no longer exactly 5 minutes after
    #      the previous observation.
    flag_changed = (
        result[anomaly_column]
        != result[anomaly_column].shift()
    )

    time_gap_minutes = (
        result["event_time"]
        .diff()
        .dt.total_seconds()
        .div(60)
    )

    timestamp_gap = (
        time_gap_minutes.notna()
        & (
            time_gap_minutes
            != EXPECTED_INTERVAL_MINUTES
        )
    )

    new_run = (
        flag_changed
        | timestamp_gap
    )

    result["run_id"] = (
        new_run
        .cumsum()
        .astype(int)
    )

    # Calculate the size of every complete run.
    run_sizes = (
        result
        .groupby("run_id")[anomaly_column]
        .transform("size")
    )

    # Keep the complete anomalous run only when it satisfies
    # the persistence requirement.
    result["persistent_anomaly"] = (
        result[anomaly_column]
        & (
            run_sizes
            >= persistence_observations
        )
    )

    return result


def build_anomaly_episodes(
    df: pd.DataFrame,
    anomaly_column: str = "is_ml_anomaly",
    persistence_observations: int = PERSISTENCE_OBSERVATIONS,
) -> pd.DataFrame:
    """
    Convert persistent anomalous runs into operational episodes.

    Each episode represents one complete consecutive anomalous run
    satisfying the configured persistence threshold.
    """

    persistent = apply_persistence(
        df,
        anomaly_column=anomaly_column,
        persistence_observations=persistence_observations,
    )

    persistent = persistent[
        persistent["persistent_anomaly"]
    ].copy()

    if persistent.empty:
        return pd.DataFrame(
            columns=EPISODE_COLUMNS
        )

    episodes = (
        persistent
        .groupby("run_id", as_index=False)
        .agg(
            start_time=("event_time", "min"),
            end_time=("event_time", "max"),
            observation_count=("event_time", "size"),
            max_anomaly_score=(
                "ml_anomaly_score",
                "max",
            ),
            mean_anomaly_score=(
                "ml_anomaly_score",
                "mean",
            ),
        )
    )

    episodes["duration_minutes"] = (
        episodes["end_time"]
        - episodes["start_time"]
    ).dt.total_seconds().div(60)

    episodes = episodes.reset_index(
        drop=True
    )

    episodes["episode_id"] = (
        episodes.index + 1
    )

    episodes = episodes[
        EPISODE_COLUMNS
    ]

    return episodes


if __name__ == "__main__":
    anomaly_results_path = (
        "data/publish/curated/"
        "network_anomaly_results.parquet"
    )

    anomaly_results = pd.read_parquet(
        anomaly_results_path
    )

    episodes = build_anomaly_episodes(
        anomaly_results
    )

    persistent = apply_persistence(
        anomaly_results
    )

    persistent_observations = persistent[
        persistent["persistent_anomaly"]
    ]

    print(
        "Anomaly episode analysis completed."
    )

    print(
        "Persistence rule:",
        f"{PERSISTENCE_OBSERVATIONS} consecutive observations",
    )

    print(
        "Persistence duration:",
        f"{PERSISTENCE_OBSERVATIONS * EXPECTED_INTERVAL_MINUTES} minutes",
    )

    print(
        "Raw ML anomalies:",
        int(
            anomaly_results[
                "is_ml_anomaly"
            ].sum()
        ),
    )

    print(
        "Persistent anomaly observations:",
        len(persistent_observations),
    )

    print(
        "Anomaly episodes:",
        len(episodes),
    )

    if not episodes.empty:
        print("\nEpisode summary:")

        print(
            episodes.to_string(
                index=False
            )
        )

        print("\nEpisode statistics:")

        print(
            episodes[
                [
                    "duration_minutes",
                    "observation_count",
                    "max_anomaly_score",
                    "mean_anomaly_score",
                ]
            ].describe().to_string()
        )

        print(
            "\nValidation:"
        )

        print(
            "Minimum observations per episode:",
            int(
                episodes[
                    "observation_count"
                ].min()
            ),
        )

        print(
            "Episodes below persistence threshold:",
            int(
                (
                    episodes[
                        "observation_count"
                    ]
                    < PERSISTENCE_OBSERVATIONS
                ).sum()
            ),
        )