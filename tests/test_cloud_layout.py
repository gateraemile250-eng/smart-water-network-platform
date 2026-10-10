"""Tests that the layout constants agree with each other."""

from src.cloud.layout import (
    CONFIG_SENSOR_KEYS,
    EXPECTED_SENSOR_COUNTS,
    KEY_COLUMNS,
    PROCESSED_COLUMNS,
    SENSOR_FILES,
    processed_key,
)
from src.data.quality_checks import EXPECTED_EVENTS_PER_TIMESTAMP, EXPECTED_UNITS


def test_there_are_twelve_distinct_monthly_keys():
    keys = [processed_key(2018, month) for month in range(1, 13)]

    assert len(set(keys)) == 12
    assert all(key.startswith("processed/sensor_readings/year=2018/month=") for key in keys)


def test_all_sensor_groups_are_defined_consistently():
    assert set(SENSOR_FILES) == set(EXPECTED_SENSOR_COUNTS) == set(CONFIG_SENSOR_KEYS)
    assert set(SENSOR_FILES) == set(EXPECTED_UNITS)


def test_sensor_total_matches_the_stream_quality_rule():
    assert sum(EXPECTED_SENSOR_COUNTS.values()) == EXPECTED_EVENTS_PER_TIMESTAMP == 119


def test_the_key_columns_are_part_of_the_table():
    assert set(KEY_COLUMNS) <= set(PROCESSED_COLUMNS)
