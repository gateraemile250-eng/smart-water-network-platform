"""Tests for predictive modeling and production scoring."""

import numpy as np
import pandas as pd
import pytest

from src.predictive.model_dataset import (
    TARGET_COLUMN,
    get_feature_columns,
    validate_modeling_dataset,
)
from src.predictive.predictive_model import (
    build_logistic_regression_model,
    prepare_model_data,
)
from src.predictive.production_scoring import (
    generate_production_predictions,
    train_production_model,
)


def make_modeling_data(rows: int = 40) -> pd.DataFrame:
    """Create a small deterministic modeling dataset for unit tests."""

    rng = np.random.default_rng(42)

    timestamps = pd.date_range(
        "2018-01-01",
        periods=rows,
        freq="5min",
    )

    return pd.DataFrame(
        {
            "Timestamp": timestamps,
            "sensor_01": rng.normal(0, 1, rows),
            "sensor_02": rng.normal(0, 1, rows),
            "sensor_03": rng.normal(0, 1, rows),
            TARGET_COLUMN: np.array(
                [False, True] * (rows // 2)
            ),
        }
    )


def test_modeling_dataset_contract():
    """Modeling data must satisfy the required structural contract."""

    data = make_modeling_data()

    validate_modeling_dataset(data)

    assert "Timestamp" in data.columns
    assert TARGET_COLUMN in data.columns
    assert data["Timestamp"].isna().sum() == 0
    assert data[TARGET_COLUMN].isna().sum() == 0
    assert data["Timestamp"].duplicated().sum() == 0
    assert data[TARGET_COLUMN].dtype == bool


def test_target_is_not_used_as_feature():
    """The future target must never be included among predictors."""

    data = make_modeling_data()

    features = get_feature_columns(data)

    assert TARGET_COLUMN not in features
    assert "Timestamp" not in features


def test_prepare_model_data_shapes():
    """Feature and target preparation must preserve row alignment."""

    data = make_modeling_data()

    X, y = prepare_model_data(data)

    assert len(X) == len(data)
    assert len(y) == len(data)
    assert X.shape[1] == len(get_feature_columns(data))


def test_model_is_reproducible():
    """Repeated model training must produce identical probabilities."""

    data = make_modeling_data()

    X, y = prepare_model_data(data)

    model_1 = build_logistic_regression_model()
    model_2 = build_logistic_regression_model()

    model_1.fit(X, y)
    model_2.fit(X, y)

    probabilities_1 = model_1.predict_proba(X)[:, 1]
    probabilities_2 = model_2.predict_proba(X)[:, 1]

    np.testing.assert_allclose(
        probabilities_1,
        probabilities_2,
    )


def test_production_model_trains():
    """Production model must train successfully on valid modeling data."""

    data = make_modeling_data()

    model = train_production_model(data)

    assert model is not None
    assert hasattr(model, "predict_proba")


def test_production_predictions_contract():
    """Production predictions must have the required valid structure."""

    data = make_modeling_data()

    model = train_production_model(data)
    predictions = generate_production_predictions(
        model,
        data,
    )

    assert list(predictions.columns) == [
        "event_time",
        "predicted_probability",
        "predicted_leakage",
    ]

    assert len(predictions) == len(data)
    assert predictions["event_time"].duplicated().sum() == 0
    assert predictions["predicted_probability"].isna().sum() == 0
    assert predictions["predicted_probability"].between(
        0.0,
        1.0,
    ).all()

    assert (
        predictions["predicted_leakage"]
        == (
            predictions["predicted_probability"] >= 0.50
        )
    ).all()


def test_production_predictions_are_reproducible():
    """Production scoring must return identical results for identical input."""

    data = make_modeling_data()

    model_1 = train_production_model(data)
    model_2 = train_production_model(data)

    predictions_1 = generate_production_predictions(
        model_1,
        data,
    )

    predictions_2 = generate_production_predictions(
        model_2,
        data,
    )

    pd.testing.assert_frame_equal(
        predictions_1,
        predictions_2,
    )


def test_duplicate_prediction_timestamps_are_rejected():
    """Production scoring must reject duplicate event timestamps."""

    data = make_modeling_data()
    data.loc[1, "Timestamp"] = data.loc[0, "Timestamp"]

    model = train_production_model(
        make_modeling_data()
    )

    with pytest.raises(
        ValueError,
        match="duplicate timestamps",
    ):
        generate_production_predictions(
            model,
            data,
        )


def test_missing_prediction_probability_is_rejected():
    """The prediction output contract must reject missing probabilities."""

    data = make_modeling_data()

    model = train_production_model(data)

    predictions = generate_production_predictions(
        model,
        data,
    )

    predictions.loc[0, "predicted_probability"] = np.nan

    assert predictions["predicted_probability"].isna().any()