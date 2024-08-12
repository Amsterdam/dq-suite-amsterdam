from dataclasses import dataclass
from typing import Any, Dict, List, Literal

from pyspark.sql import DataFrame


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
