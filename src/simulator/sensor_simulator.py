"""Replay BattLeDIM historical measurements as simulated sensor events."""

import time

from src.data.battledim_loader import load_scada_dataset


SENSOR_UNITS = {
    "pressure": "m",
    "flow": "m3/h",
    "level": "m",
    "demand": "L/h",
}

SCADA_FILES = {
    "pressure": "2018_SCADA_Pressures.csv",
    "flow": "2018_SCADA_Flows.csv",
    "level": "2018_SCADA_Levels.csv",
    "demand": "2018_SCADA_Demands.csv",
}


def validate_event(event):
    """Validate a sensor event against the streaming data contract."""

    required_fields = {
        "event_id",
        "event_time",
        "sensor_id",
        "sensor_type",
        "value",
        "unit",
    }

    if not required_fields.issubset(event):
        return False

    if not event["event_id"]:
        return False

    if not event["event_time"]:
        return False

    if not event["sensor_id"]:
        return False

    sensor_type = event["sensor_type"]

    if sensor_type not in SENSOR_UNITS:
        return False

    if not isinstance(event["value"], (int, float)):
        return False

    if event["unit"] != SENSOR_UNITS[sensor_type]:
        return False

    return True


def create_sensor_events(row, sensor_type):
    """Convert one SCADA row into validated sensor events."""

    if sensor_type not in SENSOR_UNITS:
        raise ValueError(f"Unsupported sensor type: {sensor_type}")

    event_time = row["Timestamp"]
    unit = SENSOR_UNITS[sensor_type]
    events = []

    for sensor_id, value in row.items():
        if sensor_id == "Timestamp":
            continue

        event = {
            "event_id": (
                f"{sensor_type}_{sensor_id}_"
                f"{event_time.strftime('%Y%m%dT%H%M%S')}"
            ),
            "event_time": event_time.isoformat(),
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "value": float(value),
            "unit": unit,
        }

        if not validate_event(event):
            raise ValueError(f"Invalid sensor event: {event}")

        events.append(event)

    return events


def validate_event_batch(events):
    """Validate a batch and confirm that event IDs are unique."""

    if not events:
        return False, False

    all_events_valid = all(
        validate_event(event) for event in events
    )

    event_ids = [event["event_id"] for event in events]
    event_ids_unique = len(event_ids) == len(set(event_ids))

    return all_events_valid, event_ids_unique


def load_sensor_datasets():
    """Load the BattLeDIM SCADA datasets used by the simulator."""

    return {
        sensor_type: load_scada_dataset(filename)
        for sensor_type, filename in SCADA_FILES.items()
    }


def create_timestamp_batch(datasets, row_index):
    """Create all sensor events for one historical timestamp."""

    events = []

    for sensor_type, dataset in datasets.items():
        sensor_events = create_sensor_events(
            dataset.iloc[row_index],
            sensor_type,
        )
        events.extend(sensor_events)

    return events


def replay_sensor_data(max_timestamps=3, replay_delay_seconds=1.0):
    """Replay BattLeDIM measurements in chronological order."""

    if max_timestamps <= 0:
        raise ValueError("max_timestamps must be greater than zero.")

    if replay_delay_seconds < 0:
        raise ValueError(
            "replay_delay_seconds cannot be negative."
        )

    datasets = load_sensor_datasets()

    available_timestamps = min(
        len(dataset) for dataset in datasets.values()
    )

    timestamps_to_replay = min(
        max_timestamps,
        available_timestamps,
    )

    for row_index in range(timestamps_to_replay):
        events = create_timestamp_batch(
            datasets,
            row_index,
        )

        all_events_valid, event_ids_unique = (
            validate_event_batch(events)
        )

        if not all_events_valid:
            raise ValueError(
                f"Invalid event detected in batch {row_index + 1}."
            )

        if not event_ids_unique:
            raise ValueError(
                f"Duplicate event ID detected in batch {row_index + 1}."
            )

        event_time = events[0]["event_time"]

        print(f"\nTimestamp batch {row_index + 1}")
        print(f"Event time: {event_time}")
        print(f"Total events: {len(events)}")
        print(f"All events valid: {all_events_valid}")
        print(f"Event IDs unique: {event_ids_unique}")
        print(f"Example event: {events[0]}")

        # Do not wait after the final replayed batch.
        if row_index < timestamps_to_replay - 1:
            time.sleep(replay_delay_seconds)


def main():
    """Run a short BattLeDIM sensor replay demonstration."""

    replay_sensor_data(
        max_timestamps=3,
        replay_delay_seconds=1.0,
    )


if __name__ == "__main__":
    main()