import json
from typing import Dict, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from ydata_profiling import ProfileReport

from .generic_rules import create_dq_rules, save_rules_to_file
from .report_transformations import write_profiling_metadata_to_unity


def profile_and_create_rules(
    df: DataFrame,
    dataset_name: str,
    table_name: str,
    catalog_name: str,
    spark_session: SparkSession,
    generate_rules: bool = True,
    rule_path: str = None,
) -> Union[Dict, Tuple[Dict, Dict]]:
    """
    Create a profiling report and a DQ rules file based on the profiling report.
    """
    # Generate profiling report
    report = ProfileReport(
        df,
        title=table_name,
        explorative=True,
        infer_dtypes=False,
        interactions=None,
        missing_diagrams=None,
        correlations={
            "auto": {"calculate": False},
            "pearson": {"calculate": False},
            "spearman": {"calculate": False},
        },
    )

    # Convert profiling report to JSON
    profiling_json = json.loads(report.to_json())
    report_html = report.to_notebook_iframe()

    if generate_rules:
        dq_rules = create_dq_rules(dataset_name, table_name, profiling_json, df)
        save_rules_to_file(dq_rules, rule_path)

    write_profiling_metadata_to_unity(
        profiling_json=profiling_json,
        dataset_name=dataset_name,
        catalog_name=catalog_name,
        spark_session=spark_session,
        df=df,
    )
    return report_html