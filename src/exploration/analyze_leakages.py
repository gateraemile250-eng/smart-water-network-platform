"""Analyze BattLeDIM 2018 leakage ground truth."""

from pathlib import Path

import pandas as pd
import yaml

from src.data.battledim_loader import load_scada_dataset


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DATA_DIR = Path("data/raw/battledim")

LEAKAGE_FILE = "2018_Leakages.csv"
CONFIG_FILE = DATA_DIR / "dataset_configuration.yaml"

YEAR_START = pd.Timestamp("2018-01-01 00:00:00")
YEAR_END = pd.Timestamp("2018-12-31 23:55:00")


# ---------------------------------------------------------------------
# Leakage time-series analysis
# ---------------------------------------------------------------------

def analyze_leakage_timeseries() -> pd.DataFrame:
    """Summarize positive leakage values observed in the 2018 time series."""

    df = load_scada_dataset(LEAKAGE_FILE)

    leakage_columns = df.columns.drop("Timestamp")
    summaries = []

    for link_id in leakage_columns:
        active = df[df[link_id] > 0]

        if active.empty:
            summaries.append(
                {
                    "link_id": link_id,
                    "first_active": pd.NaT,
                    "last_active": pd.NaT,
                    "active_records": 0,
                    "max_leakage_m3h": 0.0,
                }
            )
            continue

        summaries.append(
            {
                "link_id": link_id,
                "first_active": active["Timestamp"].iloc[0],
                "last_active": active["Timestamp"].iloc[-1],
                "active_records": len(active),
                "max_leakage_m3h": active[link_id].max(),
            }
        )

    return pd.DataFrame(summaries)


# ---------------------------------------------------------------------
# Official leakage metadata
# ---------------------------------------------------------------------

def load_leakage_metadata() -> pd.DataFrame:
    """Load official 2018 leakage metadata from the BattLeDIM configuration."""

    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_FILE}"
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    leakages = config["leakages"]
    records = []

    for leak in leakages:
        # The BattLeDIM YAML contains empty entries and
        # comma-separated leakage records.
        if leak is None:
            continue

        fields = [field.strip() for field in leak.split(",")]

        if len(fields) != 6:
            raise ValueError(
                f"Unexpected leakage metadata format: {leak}"
            )

        (
            link_id,
            start_time,
            end_time,
            leak_diameter,
            leak_type,
            peak_time,
        ) = fields

        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
        peak_time = pd.to_datetime(peak_time)
        leak_diameter = float(leak_diameter)

        # Keep leakage events that overlap the 2018 historical dataset.
        if start_time <= YEAR_END and end_time >= YEAR_START:
            records.append(
                {
                    "link_id": link_id,
                    "official_start": start_time,
                    "official_end": end_time,
                    "peak_time": peak_time,
                    "leak_type": leak_type,
                    "leak_diameter_m": leak_diameter,
                }
            )

    return pd.DataFrame(records)


# ---------------------------------------------------------------------
# Combined ground-truth summary
# ---------------------------------------------------------------------

def build_ground_truth_summary() -> pd.DataFrame:
    """Combine official metadata with observed 2018 leakage time series."""

    timeseries_summary = analyze_leakage_timeseries()
    metadata = load_leakage_metadata()

    summary = metadata.merge(
        timeseries_summary,
        on="link_id",
        how="inner",
    )

    summary["continues_beyond_2018"] = (
        summary["official_end"] > YEAR_END
    )

    return summary.sort_values("official_start").reset_index(drop=True)


# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------

def main() -> None:
    """Run the BattLeDIM 2018 leakage ground-truth analysis."""

    summary = build_ground_truth_summary()

    print("\n" + "=" * 120)
    print("BATTLEDIM 2018 LEAKAGE GROUND-TRUTH SUMMARY")
    print("=" * 120)

    print(f"Leakage events: {len(summary)}")

    display_columns = [
        "link_id",
        "leak_type",
        "official_start",
        "first_active",
        "peak_time",
        "official_end",
        "max_leakage_m3h",
        "continues_beyond_2018",
    ]

    print("\nGround-truth summary:")
    print(
        summary[display_columns].to_string(
            index=False,
            float_format=lambda value: f"{value:.2f}",
        )
    )


if __name__ == "__main__":
    main()