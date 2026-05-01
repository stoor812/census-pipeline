import pandas as pd
import pytest

from src.pipeline.validate import validate


def _valid_df(**overrides):
    """Minimal silver-format DataFrame that passes all validation checks."""
    data = {
        "dguid": ["2021A000011124", "2021A000011125"],
        "characteristic_id": pd.array([1, 2], dtype="Int64"),
        "geo_name": ["Riding A", "Riding B"],
        "characteristic_name": ["Population, 2021", "Population, 2016"],
        "geo_level": [
            "Federal electoral district (2013 Representation Order)",
            "Federal electoral district (2013 Representation Order)",
        ],
        "count_total": [1000.0, 2000.0],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def test_empty_df_raises():
    with pytest.raises(ValueError, match="empty"):
        validate(pd.DataFrame())


def test_missing_critical_column_raises():
    df = _valid_df()
    df = df.drop(columns=["characteristic_name"])
    with pytest.raises(ValueError, match="critical columns missing"):
        validate(df)


def test_null_in_critical_column_raises():
    df = _valid_df(dguid=["2021A000011124", None])
    with pytest.raises(ValueError, match="null values found in critical columns"):
        validate(df)


def test_duplicate_composite_key_raises():
    df = _valid_df(
        dguid=["2021A000011124", "2021A000011124"],
        characteristic_id=pd.array([1, 1], dtype="Int64"),
    )
    with pytest.raises(ValueError, match="duplicate"):
        validate(df)


def test_valid_data_passes_and_returns_summary():
    summary = validate(_valid_df())
    assert isinstance(summary, dict)
    assert summary["rows"] == 2
    assert "geo_level_warnings" in summary
    assert "null_rate_warnings" in summary
    assert "null_rate" in summary
