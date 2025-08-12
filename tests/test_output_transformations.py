import json
from datetime import datetime
from unittest.mock import Mock

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from src.dq_suite.common import ValidationSettings
from src.dq_suite.output_transformations import (
    add_regel_id_column,
    create_empty_dataframe,
    filter_df_based_on_deviating_values,
    get_afwijking_data,
    get_bronattribuut_data,
    get_brondataset_data,
    get_brontabel_data,
    get_grouped_ids_per_deviating_value,
    get_parameters_from_results,
    get_regel_data,
    get_target_attr_for_rule,
    get_unique_deviating_values,
    get_validatie_data,
    list_of_dicts_to_df,
    get_highest_severity_from_validation_result,
)

from .test_data.test_schema import SCHEMA as AFWIJKING_SCHEMA
from .test_data.test_schema import SCHEMA2 as AFWIJKING_SCHEMA2


@pytest.mark.usefixtures("rules_file_path")  # From conftest.py
@pytest.fixture()
def read_test_rules_as_dict(rules_file_path):
    with open(rules_file_path, "r") as json_file:
        dq_rules_json_string = json_file.read()
    return json.loads(dq_rules_json_string)


@pytest.mark.usefixtures("result_file_path")
@pytest.fixture()
def read_test_result_as_dict(result_file_path):
    with open(result_file_path, "r") as json_file:
        dq_result_json_string = json_file.read()
    return json.loads(dq_result_json_string)


@pytest.fixture()
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


@pytest.fixture
def validation_settings_obj():
    spark_session_mock = Mock(spec=SparkSession)
    validation_settings_obj = ValidationSettings(
        spark_session=spark_session_mock,
        catalog_name="the_catalog",
        table_name="the_table_name",
        validation_name="the_validation",
        dataset_layer="the_layer",
        dataset_name="the_dataset_name",
        unique_identifier="the_id",
    )
    return validation_settings_obj


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
    def test_output_columns_list_raises_value_error(self, spark):
        df = spark.createDataFrame([("123", "456")], ["123", "456"])
        with pytest.raises(ValueError):
            add_regel_id_column(df=df)

    def test_construct_regel_id_returns_correct_hash(self, spark):
        input_data = [
            ("test_regelNaam", "test_regelParameters", "test_bronTabelId")
        ]
        input_df = spark.createDataFrame(
            input_data, ["regelNaam", "regelParameters", "bronTabelId"]
        )

        actual_df = add_regel_id_column(df=input_df)

        expected_data = [
            (
                "test_regelNaam",
                "test_regelParameters",
                "test_bronTabelId",
                "287467170918921248",
            )
        ]
        expected_df = spark.createDataFrame(
            expected_data,
            ["regelNaam", "regelParameters", "bronTabelId", "regelId"],
        )
        expected_df.schema["regelId"].nullable = False
        assert_df_equality(actual_df, expected_df)


class TestGetParametersFromResults:
    def test_get_parameters_from_results_with_and_without_batch_id(self):
        result = {
            "kwargs": {
                "param1": 10,
                "param2": "example",
                "batch_id": 123,
            }
        }
        result2 = {"kwargs": {"param1": 10, "param2": "example"}}
        expected_output = {"param1": 10, "param2": "example"}

        assert get_parameters_from_results(result) == expected_output
        assert get_parameters_from_results(result2) == expected_output

    def get_parameters_from_results(self):
        result = {"kwargs": {}}

        expected_output = [{}]
        assert get_parameters_from_results(result) == expected_output

    def get_parameters_from_results_raises_key_error(self):
        result = {}

        with pytest.raises(KeyError):
            get_parameters_from_results(result)


class TestGetTargetAttrForRule:
    def test_get_target_attr_for_rule_with_column(self):
        result = {"kwargs": {"column": "age", "column_list": ["age", "name"]}}
        expected_output = "age"
        assert get_target_attr_for_rule(result) == expected_output

    def test_get_target_attr_for_rule_without_column(self):
        result = {"kwargs": {"column_list": ["age", "name"]}}
        expected_output = ["age", "name"]
        assert get_target_attr_for_rule(result) == expected_output

    def test_get_target_attr_for_rule_no_kwargs_key_raises_key_error(self):
        result = {}
        with pytest.raises(KeyError):
            get_target_attr_for_rule(result)


class TestGetUniqueDeviatingValues:
    def test_get_unique_deviating_values_empty_list(self):
        result = get_unique_deviating_values([])
        expected_output = set()
        assert result == expected_output

    def test_get_unique_deviating_values_list_of_strings(self):
        result = get_unique_deviating_values(["apple", "banana", "cherry"])
        expected_output = {"apple", "banana", "cherry"}
        assert result == expected_output

    def test_get_unique_deviating_values_with_duplicate_strings(self):
        result = get_unique_deviating_values(["apple", "banana", "apple"])
        expected_output = {"apple", "banana"}
        assert result == expected_output

    def test_get_unique_deviating_values_with_duplicate_dicts(self):
        result = get_unique_deviating_values(
            [
                {"key1": "value1", "key2": "value2"},
                {"key1": "value1", "key2": "value2"},  # same dict
            ]
        )
        expected_output = {(("key1", "value1"), ("key2", "value2"))}
        assert result == expected_output

    def test_get_unique_deviating_values_with_mixed_dicts_and_strings(self):
        result = get_unique_deviating_values(
            [
                "apple",
                {"key1": "value1", "key2": "value2"},
                "banana",
                {"key1": "value1", "key2": "value2"},  # same dict
                "apple",  # same string
            ]
        )
        expected_output = {
            "apple",
            "banana",
            (("key1", "value1"), ("key2", "value2")),
        }
        assert result == expected_output


@pytest.mark.usefixtures("spark")
class TestFilterDfBasedOnDeviatingValues:
    def test_filter_df_based_on_deviating_values_none_value(self, spark):
        data = [("test", None, 20), ("John", None, 24), ("Alice", "Jansen", 45)]
        df = spark.createDataFrame(data, AFWIJKING_SCHEMA2)

        result_df = filter_df_based_on_deviating_values(None, "achternaam", df)
        expected_data = [("test", None, 20), ("John", None, 24)]
        expected_df = spark.createDataFrame(expected_data, AFWIJKING_SCHEMA2)
        assert_df_equality(result_df, expected_df)

    def test_filter_df_based_on_deviating_values_single_attribute(self, spark):
        data = [
            ("Alice", "Jansen", 30),
            ("John", "Doe", 42),
            ("Alice", "Taylor", 28),
        ]
        df = spark.createDataFrame(data, AFWIJKING_SCHEMA2)
        result_df = filter_df_based_on_deviating_values("Alice", "voornaam", df)
        expected_data = [("Alice", "Jansen", 30), ("Alice", "Taylor", 28)]
        expected_df = spark.createDataFrame(expected_data, AFWIJKING_SCHEMA2)
        assert_df_equality(result_df, expected_df)

    def test_filter_df_based_on_deviating_values_compound_key(self, spark):
        data = [
            ("Alice", "Jansen", 30),
            ("John", "Doe", 42),
            ("Alice", "Taylor", 28),
        ]
        df = spark.createDataFrame(data, AFWIJKING_SCHEMA2)

        result_df = filter_df_based_on_deviating_values(
            [("voornaam", "Alice"), ("achternaam", "Jansen")],
            ["voornaam", "achternaam"],
            df,
        )
        expected_data = [("Alice", "Jansen", 30)]
        expected_df = spark.createDataFrame(expected_data, AFWIJKING_SCHEMA2)
        assert_df_equality(result_df, expected_df)


@pytest.mark.usefixtures("spark")
class TestGetGroupedIdsPerDeviatingValue:
    def test_get_grouped_ids_per_deviating_value(self, spark):
        data = [
            ("Alice", "Jansen", 30),
            ("John", "Doe", 25),
            ("Alice", "Smith", 30),
            ("John", "Doe", 25),
        ]
        df = spark.createDataFrame(data, AFWIJKING_SCHEMA2)
        filtered_df = df.filter(df.voornaam == "Alice")
        unique_identifier = ["voornaam", "achternaam"]
        grouped_ids = get_grouped_ids_per_deviating_value(
            filtered_df, unique_identifier
        )

        expected_grouped_ids = [["Alice", "Jansen"], ["Alice", "Smith"]]
        assert grouped_ids == expected_grouped_ids


@pytest.mark.usefixtures("read_test_rules_as_dict")
class TestGetDatasetData:
    def test_get_dataset_data_raises_type_error(self):
        with pytest.raises(TypeError):
            get_brondataset_data(dq_rules_dict="123")

    def test_get_dataset_data_returns_correct_list(
        self, read_test_rules_as_dict
    ):
        test_output = get_brondataset_data(
            dq_rules_dict=read_test_rules_as_dict
        )
        expected_result = [
            {"bronDatasetId": "the_dataset", "medaillonLaag": "the_layer"}
        ]
        assert test_output == expected_result


@pytest.mark.usefixtures("read_test_rules_as_dict")
class TestGetTableData:
    def test_get_table_data_raises_type_error(self):
        with pytest.raises(TypeError):
            get_brondataset_data(dq_rules_dict="123")

    def test_get_table_data_returns_correct_list(self, read_test_rules_as_dict):
        test_output = get_brontabel_data(dq_rules_dict=read_test_rules_as_dict)
        expected_result = [
            {
                "bronTabelId": "the_dataset_the_table",
                "tabelNaam": "the_table",
                "uniekeSleutel": "id",
            },
            {
                "bronTabelId": "the_dataset_the_other_table",
                "tabelNaam": "the_other_table",
                "uniekeSleutel": "other_id",
            },
            {
                "bronTabelId": "the_dataset_the_third_table_name",
                "tabelNaam": "the_third_table_name",
                "uniekeSleutel": "id",
            },
        ]
        assert test_output == expected_result


@pytest.mark.usefixtures("read_test_rules_as_dict")
class TestGetAttributeData:
    def test_get_attribute_data_raises_type_error(self):
        with pytest.raises(TypeError):
            get_bronattribuut_data(dq_rules_dict="123")

    def test_get_attribute_data_returns_correct_list(
        self, read_test_rules_as_dict
    ):
        test_output = get_bronattribuut_data(
            dq_rules_dict=read_test_rules_as_dict
        )
        expected_result = [
            {
                "bronAttribuutId": "the_dataset_the_table_the_column",
                "attribuutNaam": "the_column",
                "bronTabelId": "the_dataset_the_table",
            },
            {
                "bronAttribuutId": "the_dataset_the_other_table_the_other_column",
                "attribuutNaam": "the_other_column",
                "bronTabelId": "the_dataset_the_other_table",
            },
        ]
        assert test_output == expected_result


@pytest.mark.usefixtures("read_test_rules_as_dict")
class TestGetRegelData:
    def test_get_regel_data_raises_type_error(self):
        with pytest.raises(TypeError):
            get_regel_data(dq_rules_dict="123")

    def test_get_regel_data_returns_correct_list(self, read_test_rules_as_dict):
        test_output = get_regel_data(dq_rules_dict=read_test_rules_as_dict)
        expected_result = [
            {
                "regelNaam": "ExpectColumnDistinctValuesToEqualSet",
                "severity" : "fatal",
                "regelParameters": {
                    "column": "the_column",
                    "value_set": [1, 2, 3],
                },
                "bronTabelId": "the_dataset_the_table",
                "attribuut": "the_column",
                "norm": None,
            },
            {
                "regelNaam": "ExpectColumnValuesToBeBetween",
                "severity" : "fatal",
                "regelParameters": {
                    "column": "the_other_column",
                    "min_value": 6,
                    "max_value": 10000,
                },
                "bronTabelId": "the_dataset_the_other_table",
                "attribuut": "the_other_column",
                "norm": None,
            },
            {
                "regelNaam": "ExpectTableRowCountToBeBetween",
                "severity" : "fatal",
                "regelParameters": {"min_value": 1, "max_value": 1000},
                "bronTabelId": "the_dataset_the_other_table",
                "attribuut": None,
                "norm": None,
            },
        ]
        assert test_output == expected_result


@pytest.mark.usefixtures("read_test_result_as_dict", "validation_settings_obj")
class TestGetValidatieData:
    def test_get_validatie_data_raises_type_error(
        self, validation_settings_obj
    ):
        with pytest.raises(TypeError):
            get_validatie_data(
                validation_settings_obj=validation_settings_obj,
                run_time=datetime.now(),
                validation_output="123",
            )

    def test_get_validatie_data_returns_correct_list(
        self, read_test_result_as_dict, validation_settings_obj
    ):
        dtt_now = datetime.now()
        test_output = get_validatie_data(
            validation_settings_obj=validation_settings_obj,
            run_time=dtt_now,
            validation_output=read_test_result_as_dict,
        )
        test_sample = test_output[0]

        expected_result = {
            "aantalValideRecords": 23537,
            "aantalReferentieRecords": 23538,
            "dqResultaat": "success",
            "percentageValideRecords": 0.99,
            "regelNaam": "ExpectColumnDistinctValuesToEqualSet",
            "regelParameters": {
                "column": "the_column",
                "value_set": [1, 2, 3],
            },
            "bronTabelId": "the_dataset_name_the_table_name",
            "dqDatum": dtt_now,
        }
        for key in test_sample.keys():
            assert test_sample[key] == expected_result[key]


@pytest.mark.usefixtures("spark")
@pytest.mark.usefixtures("read_test_result_as_dict", "validation_settings_obj")
class TestGetAfwijkingData:
    def test_get_afwijking_data_raises_type_error(
        self, spark, validation_settings_obj
    ):
        with pytest.raises(TypeError):
            mock_data = [("str1", "str2")]
            mock_df = spark.createDataFrame(
                mock_data, ["the_string", "the_other_string"]
            )
            get_afwijking_data(
                df=mock_df,
                validation_settings_obj=validation_settings_obj,
                run_time=datetime.now(),
                validation_output="123",
            )


def test_get_highest_severity_from_validation_result():
    validation_result = {
        "results": [
            {
                "success": False,
                "expectation_config": {"type": "expect_column_values_to_not_be_null"}
            },
            {
                "success": False,
                "expectation_config": {"type": "expect_column_values_to_be_unique"}
            },
            {
                "success": True,
                "expectation_config": {"type": "expect_column_values_to_not_be_null"}
            }
        ]
    }
 
    rules_dict = {
        "rules": [
            {
                "rule_name": "ExpectColumnValuesToNotBeNull",
                "parameters": {"column": "name"},
                "severity": "warning"
            },
            {
                "rule_name": "ExpectColumnValuesToBeUnique",
                "parameters": {"column": "id"},
                "severity": "fatal"
            }
        ]
    }
    result = get_highest_severity_from_validation_result(validation_result, rules_dict)
    assert result == "fatal"

def test_get_highest_severity_all_successful():
    validation_result = {
        "results": [
            {
                "success": True,
                "expectation_config": {"type": "expect_column_values_to_not_be_null"}
            }
        ]
    }

    rules_dict = {
        "rules": [
            {
                "rule_name": "ExpectColumnValuesToNotBeNull",
                "parameters": {"column": "name"},
                "severity": "warning"
            }
        ]
    }

    result = get_highest_severity_from_validation_result(validation_result, rules_dict)
    assert result == "ok"

def test_get_highest_severity_no_matching_severity():
    validation_result = {
        "results": [
            {
                "success": False,
                "expectation_config": {"type": "expect_column_values_to_be_unique"}
            }
        ]
    }

    rules_dict = {
        "rules": [
            {
                "rule_name": "ExpectColumnValuesToNotBeNull",
                "parameters": {"column": "name"},
                "severity": "warning"
            }
        ]
    }

    result = get_highest_severity_from_validation_result(validation_result, rules_dict)
    assert result == "ok"

    # TODO: fix test. Also: this is not a proper unit test, needs more
    #  mocking and fewer calls to other functions inside.
    # def test_get_afwijking_data_returns_correct_list(
    #     self, spark, read_test_result_as_dict, validation_settings_obj
    # ):
    #     dtt_now = datetime.now()
    #     input_data = [("id1", None), ("id2", "the_value")]
    #     input_df = spark.createDataFrame(input_data, ["the_key", "the_column"])
    #     test_output = get_afwijking_data(
    #         df=input_df,
    #         validation_settings_obj=validation_settings_obj,
    #         run_time=dtt_now,
    #         validation_output=read_test_result_as_dict,
    #     )
    #     test_sample = test_output[0]
    #
    #     expected_result = {
    #         "identifierVeldWaarde": [["id1"]],
    #         "afwijkendeAttribuutWaarde": None,
    #         "regelNaam": "ExpectColumnDistinctValuesToEqualSet",
    #         "regelParameters": {"column": "the_column", "value_set": [1, 2, 3]},
    #         "bronTabelId": "the_dataset_name_the_table_name",
    #         "dqDatum": dtt_now,
    #     }
    #     for key in test_sample.keys():
    #         assert test_sample[key] == expected_result[key]