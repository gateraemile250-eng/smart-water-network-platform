"""Analyze measurement characteristics of BattLeDIM 2018 SCADA data."""

from src.data.battledim_loader import load_scada_dataset


# ---------------------------------------------------------------------
# Dataset configuration
# ---------------------------------------------------------------------

DATASETS = {
    "flows": {
        "filename": "2018_SCADA_Flows.csv",
        "unit": "m³/h",
    },
    "levels": {
        "filename": "2018_SCADA_Levels.csv",
        "unit": "m",
    },
    "pressures": {
        "filename": "2018_SCADA_Pressures.csv",
        "unit": "m",
    },
    "demands": {
        "filename": "2018_SCADA_Demands.csv",
        "unit": "L/h",
    },
}


# ---------------------------------------------------------------------
# Measurement analysis
# ---------------------------------------------------------------------

def summarize_measurements(
    name: str,
    filename: str,
    unit: str,
) -> None:
    """Summarize measurement characteristics for one SCADA dataset."""

    df = load_scada_dataset(filename)

    measurement_columns = df.columns.drop("Timestamp")
    measurements = df[measurement_columns]

    sensor_summary = measurements.agg(
        ["min", "mean", "median", "max", "std"]
    ).T

    print(f"\n{'=' * 70}")
    print(f"DATASET: {name.upper()}")
    print(f"UNIT: {unit}")
    print(f"SENSORS: {len(measurement_columns)}")
    print(f"{'=' * 70}")

    print("\nOverall measurement range:")
    print(f"Minimum: {measurements.min().min():,.2f} {unit}")
    print(f"Maximum: {measurements.max().max():,.2f} {unit}")

    print("\nSensor summary:")
    print(sensor_summary.round(2))


# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------

def main() -> None:
    """Analyze all configured BattLeDIM SCADA datasets."""

    for dataset_name, config in DATASETS.items():
        summarize_measurements(
            name=dataset_name,
            filename=config["filename"],
            unit=config["unit"],
        )


if __name__ == "__main__":
    main()