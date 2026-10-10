"""Names, prefixes and expectations for the processed readings dataset.

Everything that decides where data goes lives here, so the quality gate,
the converter and (later) the uploader cannot disagree.
"""

from pathlib import Path

DATA_DIR = Path("data/raw/battledim")
PUBLISH_DIR = Path("data/publish")

YEAR = 2018

SENSOR_FILES = {
    "pressure": "2018_SCADA_Pressures.csv",
    "flow": "2018_SCADA_Flows.csv",
    "level": "2018_SCADA_Levels.csv",
    "demand": "2018_SCADA_Demands.csv",
}

# Keys of the sensor lists inside dataset_configuration.yaml.
CONFIG_SENSOR_KEYS = {
    "pressure": "pressure_sensors",
    "flow": "flow_sensors",
    "level": "level_sensors",
    "demand": "amrs",
}

EXPECTED_SENSOR_COUNTS = {
    "pressure": 33,
    "flow": 3,
    "level": 1,
    "demand": 82,
}

PROCESSED_PREFIX = "processed/sensor_readings"
PART_FILE = "part-0000.parquet"

# Columns of the processed readings table, in file order.
PROCESSED_COLUMNS = ["event_time", "sensor_id", "sensor_type", "unit", "value"]

# The natural key of one reading. Two rows with the same key describe
# the same measurement.
KEY_COLUMNS = ["sensor_type", "sensor_id", "event_time"]


def processed_key(year: int, month: int) -> str:
    """Return the S3 key (and local relative path) of one monthly file."""

    return f"{PROCESSED_PREFIX}/year={year}/month={month:02d}/{PART_FILE}"
