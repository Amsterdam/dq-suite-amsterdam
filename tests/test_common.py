from dataclasses import is_dataclass
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.dq_suite.common import (
    DataQualityRulesDict,
    DatasetDict,
    Rule,
    RulesDict,
    ValidationSettings,
    get_full_table_name,
    is_empty_dataframe,
    enforce_column_order,
    enforce_schema,
)


@pytest.fixture()
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()


class TestRule:
    expected_rule_name = "the_rule"
    expected_parameters = {"q": 42}
    rule_obj = Rule(
        rule_name=expected_rule_name, parameters=expected_parameters
    )

    def test_initialisation_with_wrong_typed_rule_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert Rule(rule_name=123, parameters={})

    def test_initialisation_with_wrong_typed_parameters_raises_type_error(self):
        with pytest.raises(TypeError):
            assert Rule(rule_name="the_rule", parameters=123)

    def test_rule_is_dataclass(self):
        assert is_dataclass(self.rule_obj)

    def test_get_value_from_rule_by_existing_key(self):
        assert self.rule_obj["rule_name"] == self.expected_rule_name
        assert self.rule_obj["parameters"] == self.expected_parameters

    def test_get_value_from_rule_by_non_existing_key_raises_key_error(self):
        with pytest.raises(KeyError):
            assert self.rule_obj["wrong_key"]

    def test_initialisation_with_wrong_typed_norm_raises_type_error(self):
        with pytest.raises(TypeError):
            Rule(rule_name="the_rule", parameters={}, norm="not_an_int")

    def test_rule_with_norm_value(self):
        rule_with_norm = Rule(
            rule_name="test_rule", parameters={"test": "value"}, norm=100
        )
        assert rule_with_norm["norm"] == 100

    def test_rule_with_none_norm_value(self):
        rule_with_none_norm = Rule(
            rule_name="test_rule", parameters={"test": "value"}, norm=None
        )
        assert rule_with_none_norm["norm"] is None


class TestRulesDict:
    rule_obj = Rule(rule_name="the_rule", parameters={"q": 42})
    expected_unique_identifier = "id"
    expected_table_name = "the_table"
    expected_rules_list = [rule_obj]
    rules_dict_obj = RulesDict(
        unique_identifier=expected_unique_identifier,
        table_name=expected_table_name,
        rules_list=expected_rules_list,
    )

    def test_initialisation_with_wrong_typed_unique_identifier_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert RulesDict(
                unique_identifier=123,
                table_name=self.expected_table_name,
                rules_list=self.expected_rules_list,
            )

    def test_initialisation_with_wrong_typed_table_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert RulesDict(
                unique_identifier=self.expected_unique_identifier,
                table_name=123,
                rules_list=self.expected_rules_list,
            )

    def test_initialisation_with_wrong_typed_rules_list_raises_type_error(self):
        with pytest.raises(TypeError):
            assert RulesDict(
                unique_identifier=self.expected_unique_identifier,
                table_name=self.expected_table_name,
                rules_list=123,
            )

    def test_rules_dict_is_dataclass(self):
        assert is_dataclass(self.rules_dict_obj)

    def test_get_value_from_rule_dict_by_existing_key(self):
        assert (
            self.rules_dict_obj["unique_identifier"]
            == self.expected_unique_identifier
        )
        assert self.rules_dict_obj["table_name"] == self.expected_table_name
        assert self.rules_dict_obj["rules_list"] == self.expected_rules_list

    def test_get_value_from_rule_dict_by_non_existing_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            assert self.rules_dict_obj["wrong_key"]


class TestDatasetDict:
    expected_dataset_name = "the_dataset"
    expected_layer_name = "brons"
    dataset_obj = DatasetDict(
        name=expected_dataset_name, layer=expected_layer_name
    )

    def test_initialisation_with_wrong_typed_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DatasetDict(name=123, layer="brons")

    def test_initialisation_with_wrong_typed_layer_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DatasetDict(name="the_dataset", layer=123)

    def test_rule_is_dataclass(self):
        assert is_dataclass(self.dataset_obj)

    def test_get_value_from_rule_by_existing_key(self):
        assert self.dataset_obj["name"] == self.expected_dataset_name
        assert self.dataset_obj["layer"] == self.expected_layer_name

    def test_get_value_from_dataset_by_non_existing_key_raises_key_error(self):
        with pytest.raises(KeyError):
            assert self.dataset_obj["wrong_key"]


class TestDataQualityRulesDict:
    rule_obj = Rule(rule_name="the_rule", parameters={"q": 42})
    expected_unique_identifier = "id"
    expected_table_name = "the_table"
    expected_rules_list = [rule_obj]
    rules_dict_obj = RulesDict(
        unique_identifier=expected_unique_identifier,
        table_name=expected_table_name,
        rules_list=expected_rules_list,
    )
    expected_rules_dict_obj_list = [rules_dict_obj]
    expected_dataset_name = "the_dataset"
    expected_layer_name = "brons"
    dataset_obj = DatasetDict(
        name=expected_dataset_name, layer=expected_layer_name
    )

    def test_initialisation_with_wrong_typed_dataset_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DataQualityRulesDict(
                dataset=123,
                tables=[
                    RulesDict(
                        unique_identifier="id",
                        table_name="the_table",
                        rules_list=[
                            Rule(rule_name="the_rule", parameters={"q": 42})
                        ],
                    )
                ],
            )

    def test_initialisation_with_wrong_typed_tables_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DataQualityRulesDict(dataset=self.dataset_obj, tables=123)

    def test_get_value_from_data_quality_rules_dict_by_existing_key(self):
        data_quality_rules_dict = DataQualityRulesDict(
            dataset=self.dataset_obj, tables=self.expected_rules_dict_obj_list
        )
        assert data_quality_rules_dict["dataset"] == self.dataset_obj
        assert (
            data_quality_rules_dict["tables"]
            == self.expected_rules_dict_obj_list
        )

    def test_get_value_from_rule_dict_by_non_existing_key_raises_key_error(
        self,
    ):
        data_quality_rules_dict = DataQualityRulesDict(
            dataset=self.dataset_obj, tables=self.expected_rules_dict_obj_list
        )
        with pytest.raises(KeyError):
            assert data_quality_rules_dict["wrong_key"]


def test_is_empty_dataframe(spark):
    # Test with empty dataframe
    empty_df = spark.createDataFrame([], StructType([StructField("col", StringType(), True)]))
    assert is_empty_dataframe(empty_df) is True
    
    # Test with non-empty dataframe
    non_empty_df = spark.createDataFrame([("test",)], StructType([StructField("col", StringType(), True)]))
    assert is_empty_dataframe(non_empty_df) is False


def test_get_full_table_name():
    catalog_name = "catalog_dev"
    table_name = "the_table"
    expected_catalog_name = f"{catalog_name}.data_quality.{table_name}"

    name = get_full_table_name(catalog_name=catalog_name, table_name=table_name)
    assert name == expected_catalog_name
    
    # Test with custom schema
    custom_schema = "custom_schema"
    expected_custom = f"{catalog_name}.{custom_schema}.{table_name}"
    name_custom = get_full_table_name(
        catalog_name=catalog_name, table_name=table_name, schema_name=custom_schema
    )
    assert name_custom == expected_custom
    
    # Test with _prd suffix
    catalog_prd = "catalog_prd"
    expected_prd = f"{catalog_prd}.data_quality.{table_name}"
    name_prd = get_full_table_name(catalog_name=catalog_prd, table_name=table_name)
    assert name_prd == expected_prd
    
    with pytest.raises(ValueError):
        get_full_table_name(
            catalog_name="catalog_wrong_suffix", table_name=table_name
        )


def test_enforce_column_order(spark):
    # Create test schema
    schema = StructType([
        StructField("col_b", StringType(), True),
        StructField("col_a", StringType(), True),
        StructField("col_c", StringType(), True)
    ])
    
    # Create dataframe with different column order
    test_data = [("val_a", "val_b", "val_c")]
    df = spark.createDataFrame(test_data, ["col_a", "col_b", "col_c"])
    
    # Enforce column order
    result_df = enforce_column_order(df, schema)
    
    # Check if columns are in the correct order
    assert result_df.columns == ["col_b", "col_a", "col_c"]


def test_enforce_schema(spark):
    # Create test schema to enforce
    target_schema = StructType([
        StructField("col_a", StringType(), True),
        StructField("col_b", IntegerType(), True)
    ])
    
    # Create dataframe with different schema and extra column
    test_data = [("val_a", "123", "extra_val")]
    df = spark.createDataFrame(test_data, ["col_a", "col_b", "col_extra"])
    
    # Enforce schema
    result_df = enforce_schema(df, target_schema)
    
    # Check if schema is enforced correctly
    assert result_df.columns == ["col_a", "col_b"]
    assert len(result_df.columns) == 2  # Extra column should be removed
    
    # Collect data to verify types are cast correctly
    result_data = result_df.collect()
    assert len(result_data) == 1
    assert result_data[0]["col_a"] == "val_a"
    assert result_data[0]["col_b"] == 123  # Should be converted to int


def test_enforce_schema_with_column_removal(spark):
    # Test that columns not in target schema are removed
    target_schema = StructType([
        StructField("keep_me", StringType(), True)
    ])
    
    # Create dataframe with extra columns that should be removed
    test_data = [("keep_value", "remove_value1", "remove_value2")]
    df = spark.createDataFrame(test_data, ["keep_me", "remove_me1", "remove_me2"])
    
    result_df = enforce_schema(df, target_schema)
    
    # Should only have the column that's in the target schema
    assert result_df.columns == ["keep_me"]
    assert len(result_df.columns) == 1


def test_enforce_schema_column_removal_edge_case(spark):
    # Create a test case that specifically tests the column dropping logic
    target_schema = StructType([
        StructField("wanted", StringType(), True)
    ])
    
    # Create a dataframe where we have multiple columns that need to be dropped
    # Make sure "wanted" column is NOT the first column to trigger the drop path
    test_data = [("unwanted1", "wanted_value", "unwanted2", "unwanted3")]
    df = spark.createDataFrame(test_data, ["unwanted1", "wanted", "unwanted2", "unwanted3"])
    
    result_df = enforce_schema(df, target_schema)
    
    # Verify only the wanted column remains
    assert result_df.columns == ["wanted"]
    result_data = result_df.collect()
    assert result_data[0]["wanted"] == "wanted_value"


def test_merge_df_with_unity_table(spark):
    from unittest.mock import patch, Mock
    from src.dq_suite.common import merge_df_with_unity_table
    
    # Mock DeltaTable
    mock_delta_table = Mock()
    mock_merge_builder = Mock()
    mock_when_matched = Mock()
    mock_when_not_matched = Mock()
    
    mock_delta_table.alias.return_value = mock_merge_builder
    mock_merge_builder.merge.return_value = mock_when_matched
    mock_when_matched.whenMatchedUpdate.return_value = mock_when_not_matched
    mock_when_not_matched.whenNotMatchedInsert.return_value = mock_when_not_matched
    
    with patch('src.dq_suite.common.DeltaTable') as mock_delta_class:
        mock_delta_class.forName.return_value = mock_delta_table
        
        # Test with brondataset
        test_data = [("dataset1", "bronze")]
        df = spark.createDataFrame(test_data, ["bronDatasetId", "medaillonLaag"])
        merge_df_with_unity_table(df, "catalog_dev", "brondataset", spark)
        mock_delta_class.forName.assert_called()
        
        # Test with brontabel
        test_data_tabel = [("tabel1", "table_name", "key")]
        df_tabel = spark.createDataFrame(test_data_tabel, ["bronTabelId", "tabelNaam", "uniekeSleutel"])
        merge_df_with_unity_table(df_tabel, "catalog_dev", "brontabel", spark)
        
        # Test with bronattribuut
        test_data_attr = [("attr1", "attribute_name", "tabel1")]
        df_attr = spark.createDataFrame(test_data_attr, ["bronAttribuutId", "attribuutNaam", "bronTabelId"])
        merge_df_with_unity_table(df_attr, "catalog_dev", "bronattribuut", spark)
        
        # Test with regel
        test_data_regel = [("regel1", "rule_name", "{}", 100, "tabel1", "attr")]
        df_regel = spark.createDataFrame(test_data_regel, ["regelId", "regelNaam", "regelParameters", "norm", "bronTabelId", "attribuut"])
        merge_df_with_unity_table(df_regel, "catalog_dev", "regel", spark)
        
    # Test with unknown table name
    with pytest.raises(ValueError, match="Unknown metadata table name"):
        merge_df_with_unity_table(df, "catalog_dev", "unknown_table", spark)


class TestValidationSettings:
    spark_session_mock = Mock(spec=SparkSession)
    validation_settings_obj = ValidationSettings(
        spark_session=spark_session_mock,
        catalog_name="the_catalog",
        table_name="the_table",
        validation_name="the_validation",
        unique_identifier="the_identifier",
        dataset_layer="the_layer",
        dataset_name="the_name",
    )

    def test_initialisation_with_wrong_typed_spark_session_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=123,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
            )

    def test_initialisation_with_wrong_typed_catalog_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name=123,
                table_name="the_table",
                validation_name="the_validation",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
            )

    def test_initialisation_with_wrong_typed_table_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name=123,
                validation_name="the_validation",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
            )

    def test_initialisation_with_wrong_typed_validation_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name=123,
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
            )

    def test_initialisation_with_wrong_typed_unique_identifier_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation",
                unique_identifier=123,
                dataset_layer="the_layer",
                dataset_name="the_name",
            )

    def test_initialisation_with_wrong_typed_dataset_layer_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation",
                unique_identifier="the_identifier",
                dataset_layer=123,
                dataset_name="the_name",
            )

    def test_initialisation_with_wrong_typed_dataset_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name=123,
            )

    def test_initialisation_with_wrong_typed_batch_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
                batch_name=123,
            )

    def test_initialisation_with_wrong_typed_data_context_root_dir_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation_name",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
                data_context_root_dir=123,
            )

    def test_initialisation_with_wrong_typed_slack_webhook_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation_name",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
                slack_webhook=123,
            )

    def test_initialisation_with_wrong_typed_ms_teams_webhook_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation_name",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
                ms_teams_webhook=123,
            )

    def test_initialisation_with_wrong_valued_notify_on_raises_value_error(
        self,
    ):
        with pytest.raises(ValueError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                validation_name="the_validation_name",
                unique_identifier="the_identifier",
                dataset_layer="the_layer",
                dataset_name="the_name",
                notify_on="haha_this_is_wrong",
            )

    def test_set_expectation_suite_name(self):
        assert hasattr(self.validation_settings_obj, "_expectation_suite_name")

        self.validation_settings_obj._set_expectation_suite_name()
        assert (
            self.validation_settings_obj._expectation_suite_name
            == f"{self.validation_settings_obj.validation_name}_expectation_suite"
        )

    def test_set_checkpoint_name(self):
        assert hasattr(self.validation_settings_obj, "_checkpoint_name")

        self.validation_settings_obj._set_checkpoint_name()
        assert (
            self.validation_settings_obj._checkpoint_name
            == f"{self.validation_settings_obj.dataset_layer}/{self.validation_settings_obj.dataset_name}/{self.validation_settings_obj.table_name}"
        )

    def test_set_run_name(self):
        assert hasattr(self.validation_settings_obj, "_run_name")

        self.validation_settings_obj._set_run_name()
        assert (
            self.validation_settings_obj._run_name
            == f"%Y%m%d-%H%M%S-{self.validation_settings_obj.validation_name}"
        )

    def test_set_expectation_suite_name_with_batch_name(self):
        validation_settings_with_batch = ValidationSettings(
            spark_session=self.spark_session_mock,
            catalog_name="the_catalog",
            table_name="the_table",
            validation_name="the_validation",
            unique_identifier="the_identifier",
            dataset_layer="the_layer",
            dataset_name="the_name",
            batch_name="test_batch"
        )
        validation_settings_with_batch._set_expectation_suite_name()
        assert validation_settings_with_batch._expectation_suite_name == "batch-test_batch"

    def test_set_data_source_name(self):
        assert hasattr(self.validation_settings_obj, "_data_source_name")
        self.validation_settings_obj._set_data_source_name()
        expected_name = f"{self.validation_settings_obj.catalog_name}/{self.validation_settings_obj.dataset_layer}"
        assert self.validation_settings_obj._data_source_name == expected_name

    def test_set_data_asset_name(self):
        assert hasattr(self.validation_settings_obj, "_data_asset_name")
        self.validation_settings_obj._set_data_asset_name()
        assert self.validation_settings_obj._data_asset_name == self.validation_settings_obj.dataset_name

    def test_set_validation_definition_name(self):
        assert hasattr(self.validation_settings_obj, "_validation_definition_name")
        self.validation_settings_obj._set_validation_definition_name()
        expected_name = f"{self.validation_settings_obj.validation_name}_validation_definition"
        assert self.validation_settings_obj._validation_definition_name == expected_name

    def test_set_batch_definition_name(self):
        validation_settings_with_batch = ValidationSettings(
            spark_session=self.spark_session_mock,
            catalog_name="the_catalog",
            table_name="the_table",
            validation_name="the_validation",
            unique_identifier="the_identifier",
            dataset_layer="the_layer",
            dataset_name="the_name",
            batch_name="test_batch"
        )
        validation_settings_with_batch._set_batch_definition_name()
        assert validation_settings_with_batch._batch_definition_name == "test_batch"
