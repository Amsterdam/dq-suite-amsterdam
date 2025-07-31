from dataclasses import dataclass
from typing import Literal

from delta.tables import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


@dataclass()
class Rule:
    """
    Groups the name of the GX validation rule together with the
    parameters required to apply this rule.
    """

    rule_name: str  # Name of the GX expectation
    parameters: Dict[str, Any]  # Collection of parameters required for
    severity: Literal["fatal", "error", "warning"] | None  # Indicates the impact level of a rule if it fails.
    # evaluating the expectation
    norm: int | None = None  # TODO/check: what is the meaning of this field? Add documentation.

    def __post_init__(self):
        if not isinstance(self.rule_name, str):
            raise TypeError("'rule_name' should be of type str")

        if not isinstance(self.parameters, dict):
            raise TypeError("'parameters' should be of type Dict[str, Any]")

        if not isinstance(self.norm, int):
            if self.norm is not None:
                raise TypeError("'norm' should be of type int")

        if (self.severity is not None and self.severity not in ('fatal', 'error', 'warning')):
            raise ValueError("'severity' must be one of ('fatal', 'error', 'warning') or None")

    def __getitem__(self, key) -> str | Dict[str, Any] | int | None:
        if key == "rule_name":
            return self.rule_name
        elif key == "parameters":
            return self.parameters
        elif key == "norm":
            return self.norm
        elif key == "severity":
            return self.severity
        raise KeyError(key)


RulesList = list[Rule]  # a list of DQ rules


@dataclass()
class RulesDict:
    """
    Groups a list of Rule-objects together with the name of the table
    these rules are to be applied to, as well as a unique identifier used for
    identifying outliers.
    """

    unique_identifier: str  # TODO: List[str] for more complex keys?
    table_name: str
    rules: RulesList

    def __post_init__(self):
        if not isinstance(self.unique_identifier, str):
            raise TypeError("'unique_identifier' should be of type str")

        if not isinstance(self.table_name, str):
            raise TypeError("'table_name' should be of type str")

        if not isinstance(self.rules, list):
            raise TypeError("'rules' should be RulesList")

    def __getitem__(self, key) -> str | RulesList | None:
        if key == "unique_identifier":
            return self.unique_identifier
        elif key == "table_name":
            return self.table_name
        elif key == "rules":
            return self.rules
        raise KeyError(key)


@dataclass()
class DatasetDict:
    """
    Groups the name and the medallion layer of the dataset where the
    rules apply to.
    """

    name: str
    layer: str

    def __post_init__(self):
        if not isinstance(self.name, str):
            raise TypeError("'name' should be of type str")

        if not isinstance(self.layer, str):
            raise TypeError("'layer' should be of type str")

    def __getitem__(self, key) -> str | None:
        if key == "name":
            return self.name
        elif key == "layer":
            return self.layer
        raise KeyError(key)


RulesDictList = List[RulesDict]  # a list of dictionaries containing DQ rules


@dataclass()
class DataQualityRulesDict:
    """
    Groups a list of Table-objects together with the definition of the dataset
    these tables are a part of.
    """

    dataset: DatasetDict
    tables: RulesDictList

    def __post_init__(self):
        if not isinstance(self.dataset, DatasetDict):
            raise TypeError("'dataset' should be DatasetDict")

        if not isinstance(self.tables, list):
            raise TypeError("'tables' should be RulesDictList")

    def __getitem__(self, key) -> DatasetDict | RulesDictList | None:
        if key == "dataset":
            return self.dataset
        elif key == "tables":
            return self.tables
        raise KeyError(key)


# TODO: replace by df.isEmpty()
def is_empty_dataframe(df: DataFrame) -> bool:
    return len(df.take(1)) == 0


def get_full_table_name(
    catalog_name: str, table_name: str, schema_name: str = "data_quality"
) -> str:
    if not (catalog_name.endswith("_dev") | catalog_name.endswith("_prd")):
        raise ValueError(
            f"Incorrect catalog name '{catalog_name}', should "
            f"end with '_dev' or '_prd'."
        )
    return f"{catalog_name}.{schema_name}.{table_name}"


def enforce_column_order(df: DataFrame, schema: StructType) -> DataFrame:
    return df.select(schema.names)


def enforce_schema(df: DataFrame, schema_to_enforce: StructType) -> DataFrame:
    df = enforce_column_order(df=df, schema=schema_to_enforce)

    for column_name in df.columns:
        df = df.withColumn(
            column_name,
            col(column_name).cast(schema_to_enforce[column_name].dataType),
        )

    for column_name in df.columns:
        if column_name not in schema_to_enforce.names:
            # remove all columns not present in schema_to_enforce
            df = df.drop(column_name)

    return df


def write_to_unity_catalog(
    df: DataFrame,
    catalog_name: str,
    table_name: str,
    schema: StructType,
    mode: Literal["append", "overwrite"] = "append",
) -> None:  # pragma: no cover
    df = enforce_schema(df=df, schema_to_enforce=schema)
    full_table_name = get_full_table_name(
        catalog_name=catalog_name, table_name=table_name
    )
    df.write.mode(mode).option("overwriteSchema", "true").saveAsTable(
        full_table_name
    )  # TODO: write as delta-table? .format("delta")


def merge_df_with_unity_table(
    df: DataFrame,
    catalog_name: str,
    table_name: str,
    spark_session: SparkSession,
) -> None:
    """
    This function takes a dataframe with new records to be merged
    into an existing delta table.
    The upsert operation is based on the regel_id column.
    """
    df_new_alias = f"{table_name}_new_records"

    if table_name == "brondataset":
        merge_dict = {
            "bronDatasetId": f"{df_new_alias}.bronDatasetId",
            "medaillonLaag": f"{df_new_alias}.medaillonLaag",
        }
        merge_on = "bronDatasetId"
    elif table_name == "brontabel":
        merge_dict = {
            "bronTabelId": f"{df_new_alias}.bronTabelId",
            "tabelNaam": f"{df_new_alias}.tabelNaam",
            "uniekeSleutel": f"{df_new_alias}.uniekeSleutel",
        }
        merge_on = "bronTabelId"
    elif table_name == "bronattribuut":
        merge_dict = {
            "bronAttribuutId": f"{df_new_alias}.bronAttribuutId",
            "attribuutNaam": f"{df_new_alias}.attribuutNaam",
            "bronTabelId": f"{df_new_alias}.bronTabelId",
        }
        merge_on = "bronAttribuutId"
    elif table_name == "regel":
        merge_dict = {
            "regelId": f"{df_new_alias}.regelId",
            "regelNaam": f"{df_new_alias}.regelNaam",
            "regelParameters": f"{df_new_alias}.regelParameters",
            "norm": f"{df_new_alias}.norm",
            "bronTabelId": f"{df_new_alias}.bronTabelId",
            "attribuut": f"{df_new_alias}.attribuut",
            "severity": f"{df_new_alias}.severity",
        }
        merge_on = "regelId"
    else:
        raise ValueError(f"Unknown metadata table name '{table_name}'")

    full_table_name = get_full_table_name(
        catalog_name=catalog_name, table_name=table_name
    )
    unity_catalog_table = DeltaTable.forName(
        sparkSession=spark_session, tableOrViewName=full_table_name
    )
    (
        unity_catalog_table.alias(table_name)
        .merge(
            source=df.alias(df_new_alias),
            condition=f"{table_name}.{merge_on}={df_new_alias}.{merge_on}",
        )
        .whenMatchedUpdate(set=merge_dict)
        .whenNotMatchedInsert(values=merge_dict)
    ).execute()


@dataclass()
class ValidationSettings:
    """
    Contains all user input required for running a validation. Typically,
    this means catalog, table and validation names and a SparkSession object.

    spark_session: SparkSession object
    catalog_name: name of unity catalog
    dataset_layer: name of layer where dataset is located: landing_zone,
    bronze, silver, gold
    dataset_name: data set (source system) name
    table_name: name of table in unity catalog
    validation_name: name of data quality check
    unique_identifier: ***<insert explanation>***
    batch_name: name of the batch to validate
    data_context_root_dir: path to write GX data
    context - default "/dbfs/great_expectations/"
    slack_webhook: webhook, recommended to store in key vault. If not None,
        a Slack notification will be sent
    ms_teams_webhook: webhook, recommended to store in key vault. If not None,
        an MS Teams notification will be sent
    notify_on: when to send notifications, can be equal to "all",
        "success" or "failure"
    """

    spark_session: SparkSession
    catalog_name: str
    dataset_layer: str
    dataset_name: str
    table_name: str
    validation_name: str
    unique_identifier: str
    batch_name: str | None = None
    data_context_root_dir: str = "/dbfs/great_expectations/"
    slack_webhook: str | None = None
    ms_teams_webhook: str | None = None
    notify_on: Literal["all", "success", "failure"] = "failure"

    def __post_init__(self):
        if not isinstance(self.spark_session, SparkSession):
            raise TypeError("'spark_session' should be of type SparkSession")
        if not isinstance(self.catalog_name, str):
            raise TypeError("'catalog_name' should be of type str")
        if not isinstance(self.dataset_layer, str):
            raise TypeError("'dataset_layer' should be of type str")
        if not isinstance(self.dataset_name, str):
            raise TypeError("'dataset_name' should be of type str")
        if not isinstance(self.table_name, str):
            raise TypeError("'table_name' should be of type str")
        if not isinstance(self.validation_name, str):
            raise TypeError("'validation_name' should be of type str")
        if not isinstance(self.unique_identifier, str):
            if self.unique_identifier is not None:
                raise TypeError("'unique_identifier' should be of type str")
        if not isinstance(self.batch_name, str):
            if self.batch_name is not None:
                raise TypeError("'batch_name' should be of type str")
        if not isinstance(self.data_context_root_dir, str):
            raise TypeError("'data_context_root_dir' should be of type str")
        if not isinstance(self.slack_webhook, str):
            if self.slack_webhook is not None:
                raise TypeError("'slack_webhook' should be of type str")
        if not isinstance(self.ms_teams_webhook, str):
            if self.ms_teams_webhook is not None:
                raise TypeError("'ms_teams_webhook' should be of type str")
        if self.notify_on not in ["all", "success", "failure"]:
            raise ValueError(
                "'notify_on' should be equal to 'all', 'success' or 'failure'"
            )
        self._initialise_or_update_name_parameters()

    def _initialise_or_update_name_parameters(self):
        # TODO/check: nearly all names are related to 'validation_name' - do we want
        #  to allow for custom names via parameters?
        self._set_checkpoint_name()
        self._set_run_name()
        self._set_data_source_name()
        self._set_data_asset_name()
        self._set_validation_definition_name()
        self._set_batch_definition_name()
        self._set_expectation_suite_name()

    def _set_checkpoint_name(self):
        self._checkpoint_name = (
            f"{self.dataset_layer}/{self.dataset_name}/{self.table_name}"
        )

    def _set_run_name(self):
        self._run_name = f"%Y%m%d-%H%M%S-{self.validation_name}"

    def _set_data_source_name(self):
        self._data_source_name = f"{self.catalog_name}/{self.dataset_layer}"

    def _set_data_asset_name(self):
        self._data_asset_name = self.dataset_name

    def _set_validation_definition_name(self):
        self._validation_definition_name = (
            f"{self.validation_name}_validation_definition"
        )

    def _set_batch_definition_name(self):
        self._batch_definition_name = f"{self.batch_name}"

    def _set_expectation_suite_name(self):
        # TODO: remove conditional once CustomSlackRenderer is implemented?
        if self.batch_name is not None:
            self._expectation_suite_name = (
                f"batch-{self._batch_definition_name}"
            )
        else:
            self._expectation_suite_name = (
                f"{self.validation_name}_expectation_suite"
            )