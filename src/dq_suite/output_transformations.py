from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.dq_suite import ValidationSettings
from src.dq_suite.common import DataQualityRulesDict


def extract_dq_validatie_data(
    df_name: str, dq_result: dict, catalog_name: str, spark: SparkSession
) -> None:
    """
    [insert explanation here]

    :param df_name: Name of the tables
    :param dq_result:  # TODO: add dataclass?
    :param catalog_name:
    :param spark:
    """

    # Access run_time attribute
    run_time = dq_result["meta"]["run_id"].run_time
    # Extracted data
    extracted_data = []
    for result in dq_result["results"]:
        element_count = int(result["result"].get("element_count", 0))
        unexpected_count = int(result["result"].get("unexpected_count", 0))
        aantal_valide_records = element_count - unexpected_count
        expectation_type = result["expectation_config"]["expectation_type"]
        attribute = result["expectation_config"]["kwargs"].get("column")
        dq_regel_id = f"{df_name}_{expectation_type}_{attribute}"
        output = result["success"]
        output_text = "success" if output else "failure"
        extracted_data.append(
            {
                "regelId": dq_regel_id,
                "aantalValideRecords": aantal_valide_records,
                "aantalReferentieRecords": element_count,
                "dqDatum": run_time,
                "dqResultaat": output_text,
            }
        )
    # Create a DataFrame
    # TODO: create a PySpark dataframe directly, without use of pandas
    df_dq_validatie = pd.DataFrame(extracted_data)
    if not df_dq_validatie.empty:
        spark.createDataFrame(df_dq_validatie).write.mode("append").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"{catalog_name}.dataquality.validatie")
    else:
        # TODO: implement (raise error?)
        pass


def extract_dq_afwijking_data(
    df_name: str,
    dq_result: dict,  # TODO: add dataclass?
    df: DataFrame,
    unique_identifier: str,
    catalog_name: str,
    spark: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param df_name: Name of the tables
    :param dq_result:
    :param df: A DataFrame containing the invalid (deviated) result
    :param unique_identifier:
    :param catalog_name:
    :param spark:
    """
    # Extracting information from the JSON
    run_time = dq_result["meta"]["run_id"].run_time  # Access run_time attribute
    # Extracted data for df
    extracted_data = []

    # To store unique combinations of value and IDs
    unique_entries = set()

    for result in dq_result["results"]:
        expectation_type = result["expectation_config"]["expectation_type"]
        attribute = result["expectation_config"]["kwargs"].get("column")
        dq_regel_id = f"{df_name}_{expectation_type}_{attribute}"
        afwijkende_attribuut_waarde = result["result"].get(
            "partial_unexpected_list", []
        )
        for value in afwijkende_attribuut_waarde:
            if value is None:
                filtered_df = df.filter(col(attribute).isNull())
                ids = (
                    filtered_df.select(unique_identifier)
                    .rdd.flatMap(lambda x: x)
                    .collect()
                )
            else:
                filtered_df = df.filter(col(attribute) == value)
                ids = (
                    filtered_df.select(unique_identifier)
                    .rdd.flatMap(lambda x: x)
                    .collect()
                )

            for id_value in ids:
                entry = id_value
                if (
                    entry not in unique_entries
                ):  # Check for uniqueness before appending
                    unique_entries.add(entry)
                    extracted_data.append(
                        {
                            "regelId": dq_regel_id,
                            "identifierVeldWaarde": id_value,
                            "afwijkendeAttribuutWaarde": value,
                            "dqDatum": run_time,
                        }
                    )

    # Create a DataFrame
    # TODO: create a PySpark dataframe directly, without use of pandas
    df_dq_afwijking = pd.DataFrame(extracted_data)
    if not df_dq_afwijking.empty:
        spark.createDataFrame(df_dq_afwijking).write.mode("append").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"{catalog_name}.dataquality.afwijking")
    else:
        # TODO: implement (raise error?)
        pass


def create_brontabel(
    dq_rules_dict: DataQualityRulesDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    Function takes the table name and their unique identifier from the provided
    Data Quality rules to create a DataFrame containing this metadata.

    :param dq_rules_dict:
    :param catalog_name:
    :param spark_session:
    """
    extracted_data = []
    for param in dq_rules_dict["tables"]:
        name = param["table_name"]
        unique_identifier = param["unique_identifier"]
        extracted_data.append(
            {"bronTabelId": name, "uniekeSleutel": unique_identifier}
        )

    # TODO: create a PySpark dataframe directly, without use of pandas
    df_brontable = pd.DataFrame(extracted_data)
    spark_session.createDataFrame(df_brontable).write.mode("append").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog_name}.dataquality.brontabel")


def create_bronattribute(
    dq_rules_dict: DataQualityRulesDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    This function takes attributes/columns for each table specified in the Data
    Quality rules and creates a DataFrame containing these attribute details.

    :param dq_rules_dict:
    :param catalog_name:
    :param spark_session:
    """
    extracted_data = []
    used_ids = set()  # To keep track of used IDs
    for param in dq_rules_dict["tables"]:
        bron_tabel = param["table_name"]
        for rule in param["rules"]:
            parameters = rule.get("parameters", [])
            for parameter in parameters:
                if isinstance(parameter, dict) and "column" in parameter:
                    attribute_name = parameter["column"]
                    # Create a unique ID
                    unique_id = f"{bron_tabel}_{attribute_name}"
                    # Check if the ID is already used
                    if unique_id not in used_ids:
                        used_ids.add(unique_id)
                        extracted_data.append(
                            {
                                "bronAttribuutId": unique_id,
                                "attribuutNaam": attribute_name,
                                "bronTabelId": bron_tabel,
                            }
                        )

    # TODO: create a PySpark dataframe directly, without use of pandas
    df_bronattribuut = pd.DataFrame(extracted_data)
    spark_session.createDataFrame(df_bronattribuut).write.mode("append").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog_name}.dataquality.bronattribuut")


def create_dqRegel(
    dq_rules_dict: DataQualityRulesDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    Function extracts information about Data Quality rules applied to each
    attribute/column for tables specified in the Data Quality rules and creates
    a DataFrame containing these rule details.

    :param dq_rules_dict:
    :param catalog_name:
    :param spark_session:
    """
    extracted_data = []
    for param in dq_rules_dict["tables"]:
        bron_tabel = param["table_name"]
        for rule in param["rules"]:
            rule_name = rule["rule_name"]
            parameters = rule.get("parameters", [])
            for parameter in parameters:
                if isinstance(parameter, dict) and "column" in parameter:
                    attribute_name = parameter["column"]
                    extracted_data.append(
                        {
                            "regelId": f"{bron_tabel}_{rule_name}_{attribute_name}",
                            "bronAttribuutId": f"{bron_tabel}_{attribute_name}",
                            "bronTabelId": bron_tabel,
                        }
                    )

    # TODO: create a PySpark dataframe directly, without use of pandas
    df_dqRegel = pd.DataFrame(extracted_data)
    spark_session.createDataFrame(df_dqRegel).write.mode("append").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog_name}.dataquality.regel")


def write_non_validation_tables(
    dq_rules_dict: DataQualityRulesDict,
    validation_settings_obj: ValidationSettings,
) -> None:
    create_brontabel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
    create_bronattribute(
        dq_rules_dict=dq_rules_dict,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
    create_dqRegel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )


def write_validation_table(
    validation_output: Any,
    validation_settings_obj: ValidationSettings,
    df: DataFrame,
    unique_identifier: str,
):
    for results in validation_output.values():
        result = results["validation_result"]
        extract_dq_validatie_data(
            validation_settings_obj.table_name,
            result,
            validation_settings_obj.catalog_name,
            validation_settings_obj.spark_session,
        )
        extract_dq_afwijking_data(
            validation_settings_obj.table_name,
            result,
            df,
            unique_identifier,
            validation_settings_obj.catalog_name,
            validation_settings_obj.spark_session,
        )
