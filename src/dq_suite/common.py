from dataclasses import dataclass
from typing import Any, Dict, List, Literal

from great_expectations import get_context
from great_expectations.data_context import AbstractDataContext
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


RulesDictList = List[RulesDict]  # a list of dictionaries containing DQ rules


@dataclass()
class DataQualityRulesDict:
    tables: RulesDictList

    def __post_init__(self):
        if not isinstance(self.tables, list):
            raise TypeError("'tables' should be RulesDictList")

    def __getitem__(self, key) -> RulesDictList | None:
        if key == "tables":
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


def get_data_context(
    data_context_root_dir: str = "/dbfs/great_expectations/",
) -> AbstractDataContext:  # pragma: no cover - part of GX
    return get_context(context_root_dir=data_context_root_dir)


@dataclass()
class ValidationSettings:
    spark_session: SparkSession
    catalog_name: str
    table_name: str
    check_name: str
    data_context_root_dir: str = "/dbfs/great_expectations/"
    data_context: AbstractDataContext | None = None
    expectation_suite_name: str | None = None
    checkpoint_name: str | None = None
    run_name: str | None = None

    def __post_init__(self):
        if not isinstance(self.spark_session, SparkSession):
            raise TypeError("'spark_session' should be of type SparkSession")
        if not isinstance(self.catalog_name, str):
            raise TypeError("'catalog_name' should be of type str")
        if not isinstance(self.table_name, str):
            raise TypeError("'table_name' should be of type str")
        if not isinstance(self.check_name, str):
            raise TypeError("'check_name' should be of type str")

    def initialise_or_update_attributes(self):  # pragma: no cover - complex
        # function
        self._set_data_context()

        # TODO/check: do we want to allow for custom names?
        self._set_expectation_suite_name()
        self._set_checkpoint_name()
        self._set_run_name()

        # Finally, apply the (new) suite name to the data context
        self.data_context.add_or_update_expectation_suite(
            expectation_suite_name=self.expectation_suite_name
        )

    def _set_data_context(self):  # pragma: no cover - uses part of GX
        self.data_context = get_data_context(
            data_context_root_dir=self.data_context_root_dir
        )

    def _set_expectation_suite_name(self):
        self.expectation_suite_name = f"{self.check_name}_expectation_suite"

    def _set_checkpoint_name(self):
        self.checkpoint_name = f"{self.check_name}_checkpoint"

    def _set_run_name(self):
        self.run_name = f"%Y%m%d-%H%M%S-{self.check_name}"
