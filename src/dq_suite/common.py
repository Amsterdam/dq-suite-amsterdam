from dataclasses import dataclass
from typing import Any, Dict, List, Literal

from great_expectations import get_context
from great_expectations.data_context import AbstractDataContext
from pyspark.sql import DataFrame, SparkSession


@dataclass()
class Rule:
    """
    Groups the name of the GX validation rule together with the
    parameters required to apply this rule.
    """

    rule_name: str  # Name of the GX expectation
    parameters: List[Dict[str, Any]]  # Collection of parameters required for
    # evaluating the expectation

    def __getitem__(self, key) -> str | List[Dict[str, Any]] | None:
        if key == "rule_name":
            return self.rule_name
        elif key == "parameters":
            return self.parameters
        raise KeyError(key)


@dataclass()
class RulesDict:
    """
    Groups a list of Rule-objects together with the name of the table
    these rules are to be applied to, as well as a unique identifier used for
    identifying outliers.
    """

    unique_identifier: str  # TODO: List[str] for more complex keys?
    table_name: str
    rules_list: List[Rule]

    def __getitem__(self, key) -> str | List[Rule] | None:
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

    def __getitem__(self, key) -> RulesDictList | None:
        if key == "tables":
            return self.tables
        raise KeyError(key)


def get_full_table_name(
    catalog_name: str, table_name: str, schema_name: str = "data_quality"
) -> str:
    return f"{catalog_name}.{schema_name}.{table_name}"


def write_to_unity_catalog(
    df: DataFrame,
    catalog_name: str,
    table_name: str,
    # schema: StructType,
    mode: Literal["append", "overwrite"] = "append",
) -> None:
    # TODO: enforce schema?
    # df = enforce_schema(df=df, schema_to_enforce=schema)
    full_table_name = get_full_table_name(
        catalog_name=catalog_name, table_name=table_name
    )
    df.write.mode(mode).option("overwriteSchema", "true").saveAsTable(
        full_table_name
    )  # TODO: write as delta-table? .format("delta")


def get_data_context(
    data_context_root_dir: str = "/dbfs/great_expectations/",
) -> AbstractDataContext:
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

    def initialise_or_update_attributes(self):
        self._set_data_context()

        # TODO/check: do we want to allow for custom names?
        self._set_expectation_suite_name()
        self._set_checkpoint_name()
        self._set_run_name()

        # Finally, apply the (new) suite name to the data context
        self.data_context.add_or_update_expectation_suite(
            expectation_suite_name=self.expectation_suite_name
        )

    def _set_data_context(self):
        self.data_context = get_data_context(
            data_context_root_dir=self.data_context_root_dir
        )

    def _set_expectation_suite_name(self):
        self.expectation_suite_name = f"{self.check_name}_expectation_suite"

    def _set_checkpoint_name(self):
        self.checkpoint_name = f"{self.check_name}_checkpoint"

    def _set_run_name(self):
        self.run_name = f"%Y%m%d-%H%M%S-{self.check_name}"
