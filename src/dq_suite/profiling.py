import json
from pyspark.sql import SparkSession, DataFrame

# from .output_transformations import create_profiling_tabel, create_profiling_attribuut
from ydata_profiling import ProfileReport


def profiling(df: DataFrame, spark_session: SparkSession):
    # def profiling(df: DataFrame, df_name: str, spark_session: SparkSession):
    # Profiling raport
    report = ProfileReport(
        df,
        title="-",
        infer_dtypes=False,
        interactions=None,
        missing_diagrams=None,
        correlations={
            "auto": {"calculate": False},
            "pearson": {"calculate": True},
            "spearman": {"calculate": True},
        },
    )

    # Convert JSON string to dictionary
    report_json = json.loads(report.to_json())

    # # ProfilingTabel DataFrame
    # profiling_tabel = create_profiling_tabel(df, df_name, report_json, spark_session)

    # # ProfilingAttribuut DataFrame
    # profiling_attribuut_list = []
    # for attribute in report_json.get("variables", {}):
    #     # create ProfilingAttribut for each attribute
    #     profiling_attribuut = create_profiling_attribuut(df_name, attribute, report_json, spark_session)
    #     profiling_attribuut_list.append(profiling_attribuut)

    # # unify ProfilingAttribut DataFrames
    # profiling_attribuut_df = profiling_attribuut_list[0]  # first assignment
    # for attribuut_df in profiling_attribuut_list[1:]:
    #     profiling_attribuut_df = profiling_attribuut_df.union(attribuut_df)  # union dfs

    # return profiling_tabel, profiling_attribuut_df
    return report