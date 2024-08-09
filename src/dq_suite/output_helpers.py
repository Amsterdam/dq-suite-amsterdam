from typing import Literal

from pyspark.sql import DataFrame


def get_full_table_name(
    catalog_name: str, table_name: str, schema_name: str = "dataquality"
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
