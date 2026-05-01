import pandas as pd
import pytest

from src.pipeline.transform import transform_data, COLUMN_RENAME, COLUMNS_TO_DROP


def _raw_df(**overrides):
    """Minimal bronze-format DataFrame with all expected raw columns."""
    data = {
        "CENSUS_YEAR": ["2021"],
        "DGUID": ["2021A000011124"],
        "ALT_GEO_CODE": ["11124"],
        "GEO_LEVEL": ["Federal electoral district (2013 Representation Order)"],
        "GEO_NAME": ["Some Riding"],
        "DATA_QUALITY_FLAG": ["2"],
        "CHARACTERISTIC_ID": ["1"],
        "CHARACTERISTIC_NAME": ["Population, 2021"],
        "CHARACTERISTIC_NOTE": ["1"],
        "C1_COUNT_TOTAL": ["1000"],
        "C2_COUNT_MEN+": ["500"],
        "C3_COUNT_WOMEN+": ["500"],
        "C10_RATE_TOTAL": ["100.0"],
        "C11_RATE_MEN+": ["50.0"],
        "C12_RATE_WOMEN+": ["50.0"],
        "TNR_SF": [""],
        "TNR_LF": [""],
        "SYMBOL": [""],
        "SYMBOL.1": [""],
        "SYMBOL.2": [""],
        "SYMBOL.3": [""],
        "SYMBOL.4": [""],
        "SYMBOL.5": [""],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def test_column_renaming():
    df = transform_data(_raw_df())
    for raw, clean in COLUMN_RENAME.items():
        assert clean in df.columns, f"Expected renamed column '{clean}' not found"
        assert raw not in df.columns, f"Raw column '{raw}' should have been renamed"


def test_columns_dropped():
    df = transform_data(_raw_df())
    for col in COLUMNS_TO_DROP:
        assert col not in df.columns, f"Column '{col}' should have been dropped"


def test_float_casting_with_suppression_symbols():
    df = _raw_df(**{
        "C1_COUNT_TOTAL": ["F"],
        "C2_COUNT_MEN+": ["x"],
        "C3_COUNT_WOMEN+": ["..."],
    })
    result = transform_data(df)
    assert pd.isna(result["count_total"].iloc[0]), "StatCan 'F' symbol should become NaN"
    assert pd.isna(result["count_men"].iloc[0]), "StatCan 'x' symbol should become NaN"
    assert pd.isna(result["count_women"].iloc[0]), "StatCan '...' symbol should become NaN"


def test_characteristic_id_cast_to_int64():
    result = transform_data(_raw_df())
    assert result["characteristic_id"].dtype == pd.Int64Dtype(), (
        f"characteristic_id should be Int64, got {result['characteristic_id'].dtype}"
    )
