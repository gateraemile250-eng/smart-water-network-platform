"""Tests for the sensor-event data contract used by the simulator."""

import pandas as pd
import pytest

from src.simulator.sensor_simulator import (
    SENSOR_UNITS,
    create_sensor_events,
    validate_event,
    validate_event_batch,
)


def make_event(**overrides):
    """Build a valid event, optionally overriding fields."""

    event = {
        "event_id": "pressure_n1_20180101T000000",
        "event_time": "2018-01-01T00:00:00",
        "sensor_id": "n1",
        "sensor_type": "pressure",
        "value": 40.5,
        "unit": "m",
    }
    event.update(overrides)
    return event


def test_valid_event_is_accepted():
    assert validate_event(make_event()) is True


def test_integer_value_is_accepted():
    assert validate_event(make_event(value=40)) is True


@pytest.mark.parametrize(
    "field",
    ["event_id", "event_time", "sensor_id", "sensor_type", "value", "unit"],
)
def test_missing_required_field_is_rejected(field):
    event = make_event()
    del event[field]

    assert validate_event(event) is False


@pytest.mark.parametrize("field", ["event_id", "event_time", "sensor_id"])
def test_empty_text_field_is_rejected(field):
    assert validate_event(make_event(**{field: ""})) is False


def test_unknown_sensor_type_is_rejected():
    assert validate_event(make_event(sensor_type="temperature")) is False


def test_wrong_unit_for_sensor_type_is_rejected():
    assert validate_event(make_event(unit="L/h")) is False


def test_non_numeric_value_is_rejected():
    assert validate_event(make_event(value="40.5")) is False


@pytest.mark.parametrize("sensor_type", sorted(SENSOR_UNITS))
def test_every_sensor_type_accepts_its_own_unit(sensor_type):
    event = make_event(
        sensor_type=sensor_type,
        unit=SENSOR_UNITS[sensor_type],
    )

    assert validate_event(event) is True


def make_row():
    """One SCADA row with two sensors."""

    return pd.Series(
        {
            "Timestamp": pd.Timestamp("2018-01-01 00:05:00"),
            "n1": 40.5,
            "n2": 41.25,
        }
    )


def test_create_sensor_events_builds_one_event_per_sensor():
    events = create_sensor_events(make_row(), "pressure")

    assert [event["sensor_id"] for event in events] == ["n1", "n2"]
    assert all(event["unit"] == "m" for event in events)
    assert all(event["sensor_type"] == "pressure" for event in events)
    assert events[0]["value"] == pytest.approx(40.5)
    assert events[0]["event_time"] == "2018-01-01T00:05:00"


def test_event_ids_are_deterministic():
    # The same reading always gets the same event_id. This is why
    # replaying the simulator creates duplicates unless the lake
    # de-duplicates on event_id.
    first = create_sensor_events(make_row(), "pressure")
    second = create_sensor_events(make_row(), "pressure")

    assert [e["event_id"] for e in first] == [
        e["event_id"] for e in second
    ]
    assert first[0]["event_id"] == "pressure_n1_20180101T000500"


def test_create_sensor_events_rejects_unsupported_type():
    with pytest.raises(ValueError):
        create_sensor_events(make_row(), "temperature")


def test_batch_with_unique_valid_events():
    events = [
        make_event(event_id="a"),
        make_event(event_id="b"),
    ]

    assert validate_event_batch(events) == (True, True)


def test_batch_detects_duplicate_event_ids():
    events = [
        make_event(event_id="a"),
        make_event(event_id="a"),
    ]

    assert validate_event_batch(events) == (True, False)


def test_batch_detects_invalid_event():
    events = [
        make_event(event_id="a"),
        make_event(event_id="b", unit="wrong"),
    ]

    assert validate_event_batch(events) == (False, True)


def test_empty_batch_is_invalid():
    assert validate_event_batch([]) == (False, False)
