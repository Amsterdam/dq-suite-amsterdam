from typing import Any, List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, xxhash64
from pyspark.sql.types import StructType

from .common import (
    DataQualityRulesDict,
    ValidationSettings,
    is_empty_dataframe,
    write_to_unity_catalog,
    merge_df_with_unity_table,
)
from .schemas.bronattribuut import SCHEMA as BRONATTRIBUUT_SCHEMA
from .schemas.brontabel import SCHEMA as BRONTABEL_SCHEMA
from .schemas.regel import SCHEMA as REGEL_SCHEMA
from .schemas.validatie import SCHEMA as VALIDATIE_SCHEMA
from .schemas.pre_validatie import SCHEMA as PRE_VALIDATIE_SCHEMA
from .schemas.afwijking import SCHEMA as AFWIJKING_SCHEMA
from .schemas.pre_afwijking import SCHEMA as PRE_AFWIJKING_SCHEMA


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
    return spark_session.createDataFrame((Row(**x) for x in list_of_dicts), schema=schema)


def construct_regel_id(
    df: str,
    output_columns_list: list,
) -> DataFrame:
    df_with_id = df.withColumn("regelId", xxhash64(col("regelNaam"), col("regelParameters"), col("bronTabelId")))
    return df_with_id.select(*output_columns_list)
    

def create_parameter_list_from_results(result: dict) -> list[dict]:
    parameters = result["expectation_config"]["kwargs"]
    parameters.pop("batch_id", None)
    return [parameters]


def extract_dq_validatie_data(
    table_name: str,
    dq_result: dict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param table_name: Name of the tables
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
        parameter_list = create_parameter_list_from_results(result=result)
        attribute = result["expectation_config"]["kwargs"].get("column")
        
        output = result["success"]
        output_text = "success" if output else "failure"
        extracted_data.append(
            {
                "aantalValideRecords": aantal_valide_records,
                "aantalReferentieRecords": element_count,
                "dqDatum": run_time,
                "dqResultaat": output_text,
                "regelNaam": expectation_type,
                "regelParameters": parameter_list,
                "bronTabelId": table_name,
            }
        )

    df_validatie = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=PRE_VALIDATIE_SCHEMA,
    )
    df_validatie_with_id_ordered = construct_regel_id(
        df=df_validatie,
        output_columns_list=['regelId','aantalValideRecords','aantalReferentieRecords','dqDatum','dqResultaat']
    )
    if not is_empty_dataframe(df=df_validatie_with_id_ordered):
        write_to_unity_catalog(
            df=df_validatie_with_id_ordered,
            catalog_name=catalog_name,
            table_name="validatie",
            schema=VALIDATIE_SCHEMA,
        )
    else:
        # TODO: implement (raise error?)
        pass


def extract_dq_afwijking_data(
    table_name: str,
    dq_result: dict,  # TODO: add dataclass?
    df: DataFrame,
    unique_identifier: str,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param table_name: Name of the table
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
        parameter_list = create_parameter_list_from_results(result=result)
        attribute = result["expectation_config"]["kwargs"].get("column")
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
                            "identifierVeldWaarde": id_value,
                            "afwijkendeAttribuutWaarde": value,
                            "dqDatum": run_time,
                            "regelNaam": expectation_type,
                            "regelParameters": parameter_list,
                            "bronTabelId": table_name,
                        }
                    )

    df_afwijking = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=PRE_AFWIJKING_SCHEMA,
    )
    df_afwijking_with_id_ordered = construct_regel_id(
        df=df_afwijking,
        output_columns_list=['regelId','identifierVeldWaarde','afwijkendeAttribuutWaarde','dqDatum']
    )
    if not is_empty_dataframe(df=df_afwijking):
        write_to_unity_catalog(
            df=df_afwijking_with_id_ordered,
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
    merge_dict = {
        "bronTabelId": "brontabel_df.bronTabelId",
        "uniekeSleutel": "brontabel_df.uniekeSleutel",
    }
    merge_df_with_unity_table(
        df=df_brontabel,
        catalog_name=catalog_name,
        table_name="brontabel",
        table_merge_id="bronTabelId",
        df_merge_id="bronTabelId",
        merge_dict=merge_dict,
        spark_session=spark_session,
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
    merge_dict = {
        "bronAttribuutId": "bronattribuut_df.bronAttribuutId",
        "attribuutNaam": "bronattribuut_df.attribuutNaam",
        "bronTabelId": "bronattribuut_df.bronTabelId",
    }
    merge_df_with_unity_table(
        df=df_bronattribuut,
        catalog_name=catalog_name,
        table_name="bronattribuut",
        table_merge_id="bronAttribuutId",
        df_merge_id="bronAttribuutId",
        merge_dict=merge_dict,
        spark_session=spark_session,
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
            extracted_data.append(
                {
                    "regelNaam": rule_name,
                    "regelParameters": parameters,
                    "bronTabelId": bron_tabel
                }
            )

    df_regel = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=REGEL_SCHEMA,
    )
    df_regel_with_id_ordered = construct_regel_id(
        df=df_regel,
        output_columns_list=['regelId','regelNaam','regelParameters','bronTabelId']
    )
    merge_dict = {
        "regelId": "regel_df.regelId",
        "regelNaam": "regel_df.regelNaam",
        "regelParameters": "regel_df.regelParameters",
        "bronTabelId": "regel_df.bronTabelId"
    }
    merge_df_with_unity_table(
        df=df_regel_with_id_ordered,
        catalog_name=catalog_name,
        table_name="regel",
        table_merge_id="regelId",
        df_merge_id="regelId",
        merge_dict=merge_dict,
        spark_session=spark_session,
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
