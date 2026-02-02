import pytest
from unittest.mock import patch
from datetime import datetime

from pyspark.sql import SparkSession

from dq_suite.profile.report_transformations import (
    extract_top_value,
    create_profiling_table,
    create_profiling_attributes,
    write_profiling_metadata_to_unity,
)


@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


def test_extract_top_value():
    # Test empty stats
    assert extract_top_value({}) is None

    # Test value counts all 1
    stats = {"value_counts_without_nan": {"a": 1, "b": 1}}
    assert extract_top_value(stats) is None

    # Test single top value
    stats = {"value_counts_without_nan": {"a": 3, "b": 1}}
    assert extract_top_value(stats) == "a"

    # Test multiple top values
    stats = {"value_counts_without_nan": {"a": 2, "b": 2, "c": 1}}
    result = extract_top_value(stats)
    assert set(result) == {"a", "b"}


def test_create_profiling_table():
    profiling_json = {
        "analysis": {"title": "test_table", "date_end": "2026-01-30T12:00:00"},
        "table": {"n": 10, "n_cells_missing": 2, "n_var": 3, "n_duplicates": 1},
    }
    result = create_profiling_table(profiling_json, "dataset1")
    assert result["bronTabelId"] == "dataset1_test_table"
    assert result["aantalRecords"] == 10
    assert isinstance(result["dqDatum"], datetime)


def test_create_profiling_attributes():
    profiling_json = {
        "analysis": {"title": "test_table", "date_end": "2026-01-30T12:00:00"},
        "variables": {
            "col1": {
                "p_missing": 0.1,
                "min": 1,
                "max": 10,
                "n_distinct": 5,
                "type": "integer",
                "value_counts_without_nan": {"a": 3, "b": 2},
            }
        },
    }
    result = create_profiling_attributes(profiling_json, "dataset1", "profiling_table_1")
    assert len(result) == 1
    attr = result[0]
    assert attr["bronAttribuutId"] == "dataset1_test_table_col1"
    assert attr["topVoorkomenWaardes"] == "a"
    assert attr["vulgraad"] == 0.1


@patch("dq_suite.profile.report_transformations.write_to_unity_catalog")
def test_write_profiling_metadata_to_unity(mock_write, spark):
    profiling_json = {
        "analysis": {"title": "test_table", "date_end": "2026-01-30T12:00:00"},
        "table": {"n": 10, "n_cells_missing": 2, "n_var": 3, "n_duplicates": 1},
        "variables": {
            "col1": {
                "p_missing": 0.1,
                "min": 1,
                "max": 10,
                "n_distinct": 5,
                "type": "integer",
                "value_counts_without_nan": {"a": 3, "b": 2},
            }
        },
    }

    write_profiling_metadata_to_unity(profiling_json, "dataset1", "catalog1", spark)

    # Check that write_to_unity_catalog is called twice (table + attributes)
    assert mock_write.call_count == 2

    # Inspect first call (profiling table)
    args, kwargs = mock_write.call_args_list[0]
    df_arg = kwargs["df"]
    assert df_arg.count() == 1
    assert "profilingTabelId" in df_arg.columns

    # Inspect second call (profiling attributes)
    args, kwargs = mock_write.call_args_list[1]
    df_arg = kwargs["df"]
    assert df_arg.count() == 1
    assert "profilingAttribuutId" in df_arg.columns