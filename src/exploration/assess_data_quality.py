"""Assess data quality of BattLeDIM 2018 SCADA datasets."""

import pandas as pd

from src.data.battledim_loader import load_scada_dataset


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DATASETS = {
    "flows": "2018_SCADA_Flows.csv",
    "levels": "2018_SCADA_Levels.csv",
    "pressures": "2018_SCADA_Pressures.csv",
    "demands": "2018_SCADA_Demands.csv",
}

EXPECTED_INTERVAL = pd.Timedelta(minutes=5)


# ---------------------------------------------------------------------
# Data-quality assessment
# ---------------------------------------------------------------------

def assess_dataset(name: str, filename: str) -> dict:
    """Assess core data-quality characteristics of one SCADA dataset."""

    df = load_scada_dataset(filename)

    measurement_columns = df.columns.drop("Timestamp")
    measurements = df[measurement_columns]

    time_differences = df["Timestamp"].diff().dropna()

    unexpected_intervals = (
        time_differences != EXPECTED_INTERVAL
    ).sum()

    duplicate_timestamps = df["Timestamp"].duplicated().sum()
    missing_timestamps = df["Timestamp"].isna().sum()
    missing_measurements = measurements.isna().sum().sum()
    negative_measurements = (measurements < 0).sum().sum()

    return {
        "dataset": name,
        "rows": len(df),
        "sensors": len(measurement_columns),
        "missing_timestamps": missing_timestamps,
        "duplicate_timestamps": duplicate_timestamps,
        "unexpected_intervals": unexpected_intervals,
        "missing_measurements": missing_measurements,
        "negative_measurements": negative_measurements,
    }


# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------

def main() -> None:
    """Run data-quality checks across all BattLeDIM SCADA datasets."""

    results = []

    for dataset_name, filename in DATASETS.items():
        results.append(
            assess_dataset(
                name=dataset_name,
                filename=filename,
            )
        )

    summary = pd.DataFrame(results)

    print("\n" + "=" * 110)
    print("BATTLEDIM 2018 SCADA DATA-QUALITY SUMMARY")
    print("=" * 110)

    print(
        summary.to_string(
            index=False,
        )
    )


if __name__ == "__main__":
    main()