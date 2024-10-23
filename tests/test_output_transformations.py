
import pytest
from pyspark.sql import SparkSession

from src.dq_suite.output_transformations import create_empty_dataframe

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
