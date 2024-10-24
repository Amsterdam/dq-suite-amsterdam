from datetime import datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from src.dq_suite.output_transformations import (
    construct_regel_id,
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


@pytest.mark.usefixtures("spark")
class TestConstructRegelId:
    def test_output_columns_list_raises_type_error(self, spark):
        df = spark.createDataFrame([("123", "456")], ["123", "456"])
        with pytest.raises(TypeError):
            construct_regel_id(df=df, output_columns_list="123")

    def test_construct_regel_id_returns_correct_hash(self, spark):
        input_data = [
            ("test_regelNaam", "test_regelParameters", "test_bronTabelId")
        ]
        input_df = spark.createDataFrame(
            input_data, ["regelNaam", "regelParameters", "bronTabelId"]
        )

        actual_df = construct_regel_id(
            df=input_df,
            output_columns_list=[
                "regelId",
                "regelNaam",
            ],
        )

        expected_data = [(5287467170918921248, "test_regelNaam")]
        expected_df = spark.createDataFrame(
            expected_data, ["regelId", "regelNaam"]
        )
        expected_df.schema["regelId"].nullable = False
        assert_df_equality(actual_df, expected_df)
