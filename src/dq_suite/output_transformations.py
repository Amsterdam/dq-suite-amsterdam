from typing import Any, List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from .common import (
    DataQualityRulesDict,
    ValidationSettings,
    is_empty_dataframe,
    write_to_unity_catalog,
)
from .schemas.afwijking import SCHEMA as AFWIJKING_SCHEMA
from .schemas.bronattribuut import SCHEMA as BRONATTRIBUUT_SCHEMA
from .schemas.brontabel import SCHEMA as BRONTABEL_SCHEMA
from .schemas.regel import SCHEMA as REGEL_SCHEMA
from .schemas.validatie import SCHEMA as VALIDATIE_SCHEMA


def create_empty_dataframe(
    spark_session: SparkSession, schema: StructType
) -> DataFrame:
    return spark_session.sparkContext.parallelize([]).toDF(schema)


def list_of_dicts_to_df(
    list_of_dicts: List[dict], spark_session: SparkSession, schema: StructType
) -> DataFrame:
    if len(list_of_dicts) == 0:
        return create_empty_dataframe(
            spark_session=spark_session, schema=schema
        )
    return spark_session.createDataFrame(Row(**x) for x in list_of_dicts)


def extract_dq_validatie_data(
    df_name: str,
    dq_result: dict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param df_name: Name of the tables
    :param dq_result:  # TODO: add dataclass?
    :param catalog_name:
    :param spark_session:
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

    df_validatie = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=VALIDATIE_SCHEMA,
    )
    if not is_empty_dataframe(df=df_validatie):
        write_to_unity_catalog(
            df=df_validatie,
            catalog_name=catalog_name,
            table_name="validatie",
            schema=VALIDATIE_SCHEMA,
        )
    else:
        # TODO: implement (raise error?)
        pass


def extract_dq_afwijking_data(
    df_name: str,
    dq_result: dict,  # TODO: add dataclass?
    df: DataFrame,
    unique_identifier: str,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param df_name: Name of the tables
    :param dq_result:
    :param df: A DataFrame containing the invalid (deviated) result
    :param unique_identifier:
    :param catalog_name:
    :param spark_session:
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

    df_afwijking = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=AFWIJKING_SCHEMA,
    )
    if not is_empty_dataframe(df=df_afwijking):
        write_to_unity_catalog(
            df=df_afwijking,
            catalog_name=catalog_name,
            table_name="afwijking",
            schema=AFWIJKING_SCHEMA,
        )
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

    df_brontabel = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=BRONTABEL_SCHEMA,
    )
    write_to_unity_catalog(
        df=df_brontabel,
        catalog_name=catalog_name,
        table_name="brontabel",
        schema=BRONTABEL_SCHEMA,
    )


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

    df_bronattribuut = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=BRONATTRIBUUT_SCHEMA,
    )
    write_to_unity_catalog(
        df=df_bronattribuut,
        catalog_name=catalog_name,
        table_name="bronattribuut",
        schema=BRONATTRIBUUT_SCHEMA,
    )


def create_dq_regel(
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
                            "regelId": f"{bron_tabel}_{rule_name}_"
                            f"{attribute_name}",
                            "bronAttribuutId": f"{bron_tabel}_{attribute_name}",
                            "bronTabelId": bron_tabel,
                        }
                    )

    df_regel = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=REGEL_SCHEMA,
    )
    write_to_unity_catalog(
        df=df_regel,
        catalog_name=catalog_name,
        table_name="regel",
        schema=REGEL_SCHEMA,
    )


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
    create_dq_regel(
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
