from datetime import datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from src.dq_suite.output_transformations import (
    create_empty_dataframe,
    list_of_dicts_to_df,
)

from .test_data.test_schema import SCHEMA as AFWIJKING_SCHEMA


@pytest.fixture()
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


@pytest.mark.usefixtures("spark")
class TestCreateEmptyDataframe:
    def test_create_empty_dataframe_returns_empty_dataframe(self, spark):
        empty_dataframe = create_empty_dataframe(
            spark_session=spark,
            schema=AFWIJKING_SCHEMA,
        )
        assert len(empty_dataframe.head(1)) == 0


@pytest.mark.usefixtures("spark")
class TestListOfDictsToDf:
    def test_list_of_dicts_to_df_raises_type_error(self, spark):
        with pytest.raises(TypeError):
            list_of_dicts_to_df(
                list_of_dicts={}, spark_session=spark, schema=AFWIJKING_SCHEMA
            )

    def test_list_of_dicts_to_df_returns_dataframe(self, spark):
        current_timestamp = datetime.now()
        source_data = [
            {"the_string": "test_string", "the_timestamp": current_timestamp}
        ]

        actual_df = list_of_dicts_to_df(
            list_of_dicts=source_data,
            spark_session=spark,
            schema=AFWIJKING_SCHEMA,
        )

        expected_data = [("test_string", current_timestamp)]
        expected_df = spark.createDataFrame(
            expected_data, ["the_string", "the_timestamp"]
        )
        assert_df_equality(actual_df, expected_df)
