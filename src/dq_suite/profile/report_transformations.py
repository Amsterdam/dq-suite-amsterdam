from datetime import datetime
from typing import Dict, Any, List, Union

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, xxhash64

from dq_suite.schemas.profilingtabel import SCHEMA as PROFILINGTABEL_SCHEMA
from dq_suite.schemas.profilingattribuut import (
    SCHEMA as PROFILINGATTRIBUUT_SCHEMA,
)
from dq_suite.common import write_to_unity_catalog


def extract_top_value(stats: dict) -> Union[None, Any, List[Any]]:
    vc = stats.get("value_counts_without_nan")
    if not vc:
        return None
    max_count = max(vc.values())
    if max_count <= 1:
        return None
    top_values = [value for value, count in vc.items() if count == max_count]
    if len(top_values) == 1:
        return top_values[0]
    return top_values


def create_profiling_table(profiling_json: dict, dataset_name: str) -> Dict:
    analysis = profiling_json["analysis"]
    bronTabelId = f"{dataset_name}_{analysis['title']}"
    table = profiling_json["table"]

    end_ts = datetime.fromisoformat(analysis["date_end"])

    return {
        "profilingTabelId": None,
        "bronTabelId": bronTabelId,
        "aantalRecords": table["n"],
        "aantalNullRecords": table["n_cells_missing"],
        "aantalAttributen": table["n_var"],
        "aantalNietUniekeRecords": table["n_duplicates"],
        "dqDatum": end_ts,
    }


def create_profiling_attributes(
    profiling_json: dict, dataset_name: str, profiling_tabel_id: str
) -> Dict:
    analysis = profiling_json["analysis"]
    attributes = []
    end_ts = datetime.fromisoformat(analysis["date_end"])
    for col, stats in profiling_json["variables"].items():
        bronAttribuutId = f"{dataset_name}_{analysis['title']}_{col}"
        top_value = extract_top_value(stats)
        attributes.append(
            {
                "profilingAttribuutId": None,
                "profilingTabelId": profiling_tabel_id,
                "bronAttribuutId": bronAttribuutId,
                "vulgraad": stats.get("p_missing"),
                "minWaarde": str(stats.get("min")),
                "maxWaarde": str(stats.get("max")),
                "aantalUniekeWaardes": stats.get("n_distinct"),
                "topVoorkomenWaardes": (
                    str(top_value) if top_value is not None else None
                ),
                "dataType": stats.get("type"),
                "dqDatum": end_ts,
            }
        )
    return attributes


def write_profiling_metadata_to_unity(
    profiling_json: dict,
    dataset_name: str,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    tabel_df = spark_session.createDataFrame(
        [Row(**create_profiling_table(profiling_json, dataset_name))],
        schema=PROFILINGTABEL_SCHEMA,
    )
    tabel_df = tabel_df.withColumn(
        "profilingTabelId", xxhash64(col("bronTabelId")).substr(2, 20)
    )

    write_to_unity_catalog(
        df=tabel_df,
        catalog_name=catalog_name,
        table_name="profilingtabel",
        schema=PROFILINGTABEL_SCHEMA,
    )

    profiling_tabel_id = tabel_df.collect()[0]["profilingTabelId"]
    attribuut_rows = create_profiling_attributes(
        profiling_json, dataset_name, profiling_tabel_id
    )
    attribuut_df = spark_session.createDataFrame(
        [Row(**row) for row in attribuut_rows], schema=PROFILINGATTRIBUUT_SCHEMA
    )
    attribuut_df = attribuut_df.withColumn(
        "profilingAttribuutId",
        xxhash64(col("profilingTabelId"), col("bronAttribuutId")).substr(2, 20),
    )

    write_to_unity_catalog(
        df=attribuut_df,
        catalog_name=catalog_name,
        table_name="profilingattribuut",
        schema=PROFILINGATTRIBUUT_SCHEMA,
    )
