from datetime import datetime
from typing import Any, Dict, List, Union

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, lit, xxhash64

from dq_suite.common import write_to_unity_catalog, merge_df_with_unity_table  
from dq_suite.schemas.profilingattribuut import (
    SCHEMA as PROFILINGATTRIBUUT_SCHEMA,
)
from dq_suite.schemas.profilingtabel import SCHEMA as PROFILINGTABEL_SCHEMA
from dq_suite.schemas.team import SCHEMA as TEAM_SCHEMA

from .generic_rules import has_geometry_column, derive_team_from_dataset, normalize_numeric_type


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


def create_profiling_table(profiling_json: dict, dataset_name: str, layer_name: str) -> Dict:
    analysis = profiling_json["analysis"]
    bronTabelId = f"{dataset_name}_{layer_name}_{analysis['title']}"
    table = profiling_json["table"]
    teamId = dataset_name.split("_")[0]

    end_ts = datetime.fromisoformat(analysis["date_end"])

    return {
        "teamId" : teamId,
        "profilingTabelId": None,
        "bronTabelId": bronTabelId,
        "tabelNaam": analysis["title"],
        "medaillonLaag": layer_name,
        "aantalRecords": table["n"],
        "aantalNullRecords": table["n_cells_missing"],
        "aantalAttributen": table["n_var"],
        "aantalNietUniekeRecords": table["n_duplicates"],
        "dqDatum": end_ts,
    }


def create_profiling_attributes(
    profiling_json: dict,
    dataset_name: str,
    profiling_tabel_id: str,
    layer_name: str,
    df: DataFrame,
) -> Dict:
    analysis = profiling_json["analysis"]
    attributes = []
    end_ts = datetime.fromisoformat(analysis["date_end"])
    for col, stats in profiling_json["variables"].items():
        bronAttribuutId = f"{dataset_name}_{layer_name}_{analysis['title']}_{col}"
        top_value = extract_top_value(stats)
        data_type = stats.get("type")
        if data_type == "Numeric":
            data_type = normalize_numeric_type(data_type, stats)
        if has_geometry_column(df, col):
            data_type = type(df[col].dropna().iloc[0]).__name__

        attributes.append(
            {
                "profilingAttribuutId": None,
                "profilingTabelId": profiling_tabel_id,
                "bronAttribuutId": bronAttribuutId,
                "attribuutNaam": col,  
                "missingDataPercentage": stats.get("p_missing"),
                "minWaarde": str(stats.get("min")),
                "maxWaarde": str(stats.get("max")),
                "aantalUniekeWaardes": stats.get("n_distinct"),
                "topVoorkomendeWaardes": (
                    str(top_value) if top_value is not None else None
                ),
                "dataType": data_type,
                "dqDatum": end_ts,
            }
        )
    return attributes


def write_profiling_metadata_to_unity(
    profiling_json: dict,
    catalog_name: str,
    dataset_name: str,
    layer_name: str,
    spark_session: SparkSession,
    df: DataFrame,
) -> None:
    team = derive_team_from_dataset(dataset_name)
    
    merge_df_with_unity_table(
        df=spark_session.createDataFrame([{
            "teamId": team["teamid"],
            "teamNaam": team["teamname"],
            "teamdescription": team["teamname"]
        }]),
        catalog_name=catalog_name,
        table_name="team",
        spark_session=spark_session,
    )
    
    tabel_df = spark_session.createDataFrame(
        [Row(**create_profiling_table(profiling_json, dataset_name, layer_name))],
        schema=PROFILINGTABEL_SCHEMA,
    )
    tabel_df = tabel_df.withColumn(
        "profilingTabelId", xxhash64(col("bronTabelId"),col("dqDatum")).substr(2, 20)
    )

    write_to_unity_catalog(
        df=tabel_df,
        catalog_name=catalog_name,
        table_name="profilingtabel",
        schema=PROFILINGTABEL_SCHEMA,
    )

    profiling_tabel_id = tabel_df.collect()[0]["profilingTabelId"]
    attribuut_rows = create_profiling_attributes(
        profiling_json, dataset_name, profiling_tabel_id, layer_name, df
    )
    attribuut_df = spark_session.createDataFrame(
        [Row(**row) for row in attribuut_rows], schema=PROFILINGATTRIBUUT_SCHEMA
    )
    attribuut_df = attribuut_df.withColumn(
        "profilingAttribuutId",
        xxhash64(lit(profiling_tabel_id), col("bronAttribuutId"), col("dqDatum")).substr(2, 20),
    )

    write_to_unity_catalog(
        df=attribuut_df,
        catalog_name=catalog_name,
        table_name="profilingattribuut",
        schema=PROFILINGATTRIBUUT_SCHEMA,
    )
