from dataclasses import dataclass
from typing import Any, Dict, List, Literal
import yaml

from great_expectations import ExpectationSuite, get_context
from great_expectations.data_context import (
    AbstractDataContext,
    EphemeralDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.exceptions import DataContextError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from delta.tables import *


@dataclass()
class Rule:
    """
    Groups the name of the GX validation rule together with the
    parameters required to apply this rule.
    """

    rule_name: str  # Name of the GX expectation
    parameters: List[Dict[str, Any]]  # Collection of parameters required for
    # evaluating the expectation

    def __post_init__(self):
        if not isinstance(self.rule_name, str):
            raise TypeError("'rule_name' should be of type str")

        if not isinstance(self.parameters, list):
            raise TypeError(
                "'parameters' should be of type List[Dict[str, Any]]"
            )

    def __getitem__(self, key) -> str | List[Dict[str, Any]] | None:
        if key == "rule_name":
            return self.rule_name
        elif key == "parameters":
            return self.parameters
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
    rules_list: RulesList

    def __post_init__(self):
        if not isinstance(self.unique_identifier, str):
            raise TypeError("'unique_identifier' should be of type str")

        if not isinstance(self.table_name, str):
            raise TypeError("'table_name' should be of type str")

        if not isinstance(self.rules_list, list):
            raise TypeError("'rules_list' should be RulesList")

    def __getitem__(self, key) -> str | RulesList | None:
        if key == "unique_identifier":
            return self.unique_identifier
        elif key == "table_name":
            return self.table_name
        elif key == "rules_list":
            return self.rules_list
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

    def __getitem__(self, key) -> str | RulesList | None:
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
        if not isinstance(self.dataset, dict):
            raise TypeError("'dataset' should be DatasetDict")

        if not isinstance(self.tables, list):
            raise TypeError("'tables' should be RulesDictList")

    def __getitem__(self, key) -> str | RulesDictList | None:
        if key == "dataset":
            return self.dataset
        elif key == "tables":
            return self.tables
        raise KeyError(key)


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
    table_merge_id: str,
    df_merge_id: str,
    merge_dict: dict,
    spark_session: SparkSession,
) -> None:
    """
    This function takes a dataframe with new records to be merged
    into an existing delta table. The upsert operation is based on
    the regel_id column.
    """
    full_table_name = get_full_table_name(
        catalog_name=catalog_name, table_name=table_name
    )
    df_alias = f'{table_name}_df'
    regelTabel = DeltaTable.forName(spark_session, full_table_name)
    regelTabel.alias(table_name) \
        .merge(
            df.alias(df_alias),
            f'{table_name}.{table_merge_id} = {df_alias}.{df_merge_id}'
        ) \
        .whenMatchedUpdate(set = merge_dict) \
        .whenNotMatchedInsert(values = merge_dict) \
        .execute()


def get_data_context() -> AbstractDataContext:  # pragma: no cover - part of GX
    return get_context(
        project_config=DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults(),
            analytics_enabled=False
        )
    )


@dataclass()
class ValidationSettings:
    """
    spark_session: SparkSession object
    catalog_name: name of unity catalog
    table_name: name of table in unity catalog
    check_name: name of data quality check
    data_context_root_dir: path to write GX data
    context - default "/dbfs/great_expectations/"
    data_context: a data context object
    expectation_suite_name: name of the GX expectation suite
    checkpoint_name: name of the GX checkpoint
    run_name: name of the data quality run
    send_slack_notification: indicator to use GX's built-in Slack
    notification action
    slack_webhook: webhook, recommended to store in key vault
    send_ms_teams_notification: indicator to use GX's built-in Microsoft
    Teams notification action
    ms_teams_webhook: webhook, recommended to store in key vault
    notify_on: when to send notifications, can be equal to "all",
    "success" or "failure"
    """

    spark_session: SparkSession
    catalog_name: str
    table_name: str
    check_name: str
    data_context_root_dir: str = "/dbfs/great_expectations/"
    data_context: AbstractDataContext | None = None
    expectation_suite_name: str | None = None
    checkpoint_name: str | None = None
    run_name: str | None = None
    send_slack_notification: bool = False
    slack_webhook: str | None = None
    send_ms_teams_notification: bool = False
    ms_teams_webhook: str | None = None
    notify_on: Literal["all", "success", "failure"] = "failure"

    def __post_init__(self):
        if not isinstance(self.spark_session, SparkSession):
            raise TypeError("'spark_session' should be of type SparkSession")
        if not isinstance(self.catalog_name, str):
            raise TypeError("'catalog_name' should be of type str")
        if not isinstance(self.table_name, str):
            raise TypeError("'table_name' should be of type str")
        if not isinstance(self.check_name, str):
            raise TypeError("'check_name' should be of type str")
        if not isinstance(self.data_context_root_dir, str):
            raise TypeError("'data_context_root_dir' should be of type str")
        if self.notify_on not in ["all", "success", "failure"]:
            raise ValueError(
                "'notify_on' should be equal to 'all', 'success' or 'failure'"
            )

    def initialise_or_update_attributes(self):  # pragma: no cover - complex
        # function
        self._set_data_context()

        # TODO/check: do we want to allow for custom names via parameters?
        self._set_expectation_suite_name()
        self._set_checkpoint_name()
        self._set_run_name()

        # Finally, add/retrieve the suite to/from the data context
        try:
            self.data_context.suites.get(name=self.expectation_suite_name)
        except DataContextError:
            self.data_context.suites.add(
                suite=ExpectationSuite(name=self.expectation_suite_name)
            )

    def _set_data_context(self):  # pragma: no cover - uses part of GX
        self.data_context = get_data_context()

    def _set_expectation_suite_name(self):
        self.expectation_suite_name = f"{self.check_name}_expectation_suite"

    def _set_checkpoint_name(self):
        self.checkpoint_name = f"{self.check_name}_checkpoint"

    def _set_run_name(self):
        self.run_name = f"%Y%m%d-%H%M%S-{self.check_name}"
