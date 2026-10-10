"""Tests for the publishing quality gate and de-duplication."""

import numpy as np
import pandas as pd

from src.cloud.build import to_long_format
from src.cloud.layout import PROCESSED_COLUMNS
from src.cloud.quality_gate import (
    check_dedup,
    check_month,
    check_source,
    deduplicate,
    has_errors,
    merge_results,
)
from cloud_helpers import (
    CONFIG,
    SMALL,
    detail_of,
    failed_errors,
    failed_warnings,
    make_frames,
)


def run(frames):
    return check_source(frames, CONFIG, SMALL)


# ---------------------------------------------------------------- source


def test_valid_data_passes_the_source_gate():
    results = run(make_frames())

    assert not has_errors(results)
    assert all(result["passed"] for result in results)


def test_missing_table_fails():
    frames = make_frames()
    del frames["demand"]

    assert "scada_tables_loaded" in failed_errors(run(frames))


def test_missing_timestamp_column_fails():
    frames = make_frames()
    frames["flow"] = frames["flow"].rename(columns={"Timestamp": "Time"})

    assert "timestamp_column_present" in failed_errors(run(frames))


def test_wrong_sensor_count_fails():
    frames = make_frames()
    frames["pressure"] = frames["pressure"].drop(columns=["p2"])

    assert "sensor_counts" in failed_errors(run(frames))


def test_unknown_sensor_id_fails_and_is_named():
    frames = make_frames()
    frames["demand"] = frames["demand"].rename(columns={"d2": "d99"})
    results = run(frames)

    assert "sensor_ids_match_configuration" in failed_errors(results)
    assert "d99" in detail_of(results, "sensor_ids_match_configuration")


def test_wrong_row_count_fails():
    frames = make_frames()
    frames["pressure"] = frames["pressure"].iloc[:-1]

    assert "row_count" in failed_errors(run(frames))


def test_unparseable_timestamps_fail():
    frames = make_frames()

    for name in frames:
        frames[name]["Timestamp"] = frames[name]["Timestamp"].astype(str)

    assert "timestamp_dtype" in failed_errors(run(frames))


def test_missing_timestamp_value_fails():
    frames = make_frames()

    for name in frames:
        frames[name].loc[2, "Timestamp"] = pd.NaT

    assert "timestamps_not_missing" in failed_errors(run(frames))


def test_duplicate_timestamp_fails():
    frames = make_frames()

    for name in frames:
        frames[name].loc[3, "Timestamp"] = frames[name].loc[2, "Timestamp"]

    assert "timestamps_unique" in failed_errors(run(frames))


def test_time_gap_fails():
    frames = make_frames()

    for name in frames:
        frames[name].loc[3:, "Timestamp"] += pd.Timedelta(minutes=5)

    results = run(frames)

    assert "timestamps_regular_step" in failed_errors(results)
    assert "first at row" in detail_of(results, "timestamps_regular_step")


def test_out_of_order_timestamps_fail():
    frames = make_frames()

    for name in frames:
        frames[name] = frames[name].iloc[::-1].reset_index(drop=True)

    assert "timestamps_increasing" in failed_errors(run(frames))


def test_timestamps_must_match_across_files():
    frames = make_frames()
    frames["flow"].loc[1, "Timestamp"] += pd.Timedelta(minutes=1)

    assert "timestamps_consistent_across_files" in failed_errors(run(frames))


def test_nan_value_fails_and_names_the_sensor_type():
    frames = make_frames()
    frames["pressure"].loc[1, "p1"] = np.nan
    results = run(frames)

    assert "values_finite" in failed_errors(results)
    assert "pressure" in detail_of(results, "values_finite")


def test_infinite_value_fails():
    frames = make_frames()
    frames["flow"].loc[0, "f1"] = np.inf

    assert "values_finite" in failed_errors(run(frames))


def test_text_in_a_value_column_fails():
    frames = make_frames()
    frames["demand"]["d1"] = frames["demand"]["d1"].astype(object)
    frames["demand"].loc[0, "d1"] = "abc"

    assert "values_numeric" in failed_errors(run(frames))


def test_negative_value_fails():
    frames = make_frames()
    frames["level"].loc[2, "t1"] = -0.5

    assert "values_non_negative" in failed_errors(run(frames))


def test_implausibly_high_value_only_warns():
    frames = make_frames()
    frames["pressure"].loc[0, "p1"] = 5000.0
    results = run(frames)

    assert not has_errors(results)
    assert "values_within_plausible_range" in failed_warnings(results)


# ----------------------------------------------------------- de-duplication


def small_long_frame():
    return to_long_format(make_frames(), 2018, 2)


def test_no_duplicates_leaves_the_frame_untouched():
    frame = small_long_frame()
    cleaned, stats = deduplicate(frame)

    assert len(cleaned) == len(frame)
    assert (stats.input_rows, stats.duplicate_rows_detected) == (len(frame), 0)
    assert (stats.rows_removed, stats.conflicting_keys) == (0, 0)
    assert stats.final_rows == len(frame)


def test_exact_duplicates_are_detected_and_removed_once():
    frame = small_long_frame()
    repeated = pd.concat([frame, frame.iloc[:3]], ignore_index=True)

    cleaned, stats = deduplicate(repeated)

    assert stats.input_rows == len(frame) + 3
    assert stats.duplicate_rows_detected == 3
    assert stats.rows_removed == 3
    assert stats.final_rows == len(frame) == len(cleaned)
    assert not cleaned.duplicated(subset=["sensor_type", "sensor_id", "event_time"]).any()


def test_the_first_copy_is_kept():
    frame = small_long_frame()
    repeated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)

    cleaned, _ = deduplicate(repeated)

    assert cleaned.iloc[0]["value"] == frame.iloc[0]["value"]


def test_conflicting_duplicates_are_reported_and_nothing_is_removed():
    frame = small_long_frame()
    conflicting = frame.iloc[[0]].copy()
    conflicting["value"] = conflicting["value"] + 1
    repeated = pd.concat([frame, conflicting], ignore_index=True)

    cleaned, stats = deduplicate(repeated)

    assert stats.conflicting_keys == 1
    assert stats.rows_removed == 0
    assert len(cleaned) == len(repeated)
    assert "no_conflicting_duplicates" in failed_errors(check_dedup(stats))


def test_removed_duplicates_are_a_warning_not_an_error():
    frame = small_long_frame()
    repeated = pd.concat([frame, frame.iloc[:2]], ignore_index=True)
    _, stats = deduplicate(repeated)
    results = check_dedup(stats)

    assert not has_errors(results)
    assert "duplicate_rows" in failed_warnings(results)


# ----------------------------------------------------------------- month


def month_results(frame, timestamps=3):
    return check_month(frame, 2018, 2, timestamps, SMALL.sensors_per_timestamp)


def test_valid_month_passes():
    assert not has_errors(month_results(small_long_frame()))


def test_month_with_missing_rows_fails():
    frame = small_long_frame().iloc[:-1]

    assert "month_row_count" in failed_errors(month_results(frame))


def test_month_with_a_missing_sensor_fails_completeness():
    frame = small_long_frame()
    frame = frame[frame["sensor_id"] != "d1"].reset_index(drop=True)

    assert "month_timestamps_complete" in failed_errors(month_results(frame))


def test_wrong_unit_fails():
    frame = small_long_frame()
    frame.loc[0, "unit"] = "psi"

    assert "month_units" in failed_errors(month_results(frame))


def test_unknown_sensor_type_fails():
    frame = small_long_frame()
    frame.loc[0, "sensor_type"] = "temperature"

    assert "month_units" in failed_errors(month_results(frame))


def test_wrong_columns_fail():
    frame = small_long_frame()[["event_time", "value"]]

    assert "columns_exact" in failed_errors(month_results(frame))


def test_rows_from_another_month_fail():
    frame = to_long_format(make_frames(), 2018, 1)

    assert "month_time_bounds" in failed_errors(month_results(frame, timestamps=2))


def test_repeated_key_in_a_month_fails():
    frame = small_long_frame()
    repeated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)

    assert "month_key_unique" in failed_errors(month_results(repeated))


# ----------------------------------------------------------------- merge


def test_merge_keeps_the_first_failure_and_marks_passes_with_a_count():
    good = [{"name": "x", "severity": "ERROR", "passed": True, "detail": "ok"}]
    bad = [{"name": "x", "severity": "ERROR", "passed": False, "detail": "2018-03 broke"}]

    merged = merge_results([good, good, bad])

    assert merged[0]["passed"] is False
    assert merged[0]["detail"] == "2018-03 broke"
    assert "(all 2 months)" in merge_results([good, good])[0]["detail"]


def test_processed_columns_match_the_design():
    assert PROCESSED_COLUMNS == [
        "event_time", "sensor_id", "sensor_type", "unit", "value",
    ]
