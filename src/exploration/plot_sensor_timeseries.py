"""Plot representative BattLeDIM sensor measurements over time."""

import matplotlib.pyplot as plt
import pandas as pd

from src.data.battledim_loader import load_scada_dataset

from src.data.battledim_loader import load_scada_dataset


SENSORS = {
    "pressure_n1": {
        "filename": "2018_SCADA_Pressures.csv",
        "sensor": "n1",
        "label": "Pressure — n1",
        "unit": "m",
    },
    "flow_p227": {
        "filename": "2018_SCADA_Flows.csv",
        "sensor": "p227",
        "label": "Flow — p227",
        "unit": "m³/h",
    },
    "pump_flow": {
        "filename": "2018_SCADA_Flows.csv",
        "sensor": "PUMP_1",
        "label": "Pump Flow — PUMP_1",
        "unit": "m³/h",
    },
    "tank_level": {
        "filename": "2018_SCADA_Levels.csv",
        "sensor": "T1",
        "label": "Tank Level — T1",
        "unit": "m",
    },
    "demand_n1": {
        "filename": "2018_SCADA_Demands.csv",
        "sensor": "n1",
        "label": "Demand — n1",
        "unit": "L/h",
    },
}


def plot_sensor(config: dict) -> None:
    """Plot the first seven days for one representative sensor."""

    df = load_scada_dataset(config["filename"])

    start_time = df["Timestamp"].min()
    end_time = start_time + pd.Timedelta(days=7)

    sample = df[
        (df["Timestamp"] >= start_time)
        & (df["Timestamp"] < end_time)
    ]

    plt.figure(figsize=(12, 5))

    plt.plot(
        sample["Timestamp"],
        sample[config["sensor"]],
    )

    plt.title(config["label"])
    plt.xlabel("Time")
    plt.ylabel(f"Measurement ({config['unit']})")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main() -> None:
    """Plot representative BattLeDIM sensor time series."""

    for config in SENSORS.values():
        plot_sensor(config)


if __name__ == "__main__":
    main()