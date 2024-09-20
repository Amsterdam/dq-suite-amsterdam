import datetime
from typing import List

from great_expectations.checkpoint.checkpoint import CheckpointDescriptionDict
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
from .schemas.brondataset import SCHEMA as BRONDATASET_SCHEMA
from .schemas.brontabel import SCHEMA as BRONTABEL_SCHEMA
from .schemas.bronattribuut import SCHEMA as BRONATTRIBUUT_SCHEMA
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
    return spark_session.createDataFrame(
        (Row(**x) for x in list_of_dicts), schema=schema
    )


def construct_regel_id(
    df: DataFrame,
    output_columns_list: list[str],
) -> DataFrame:
    df_with_id = df.withColumn("regelId", xxhash64(col("regelNaam"), col("regelParameters"), col("bronTabelId")))
    return df_with_id.select(*output_columns_list)
    

def create_parameter_list_from_results(result: dict) -> list[dict]:
    parameters = result["kwargs"]
    parameters.pop("batch_id", None)
    return [parameters]


def get_target_attr_for_rule(result: dict) -> str:
    if "column" in result["expectation_config"]["kwargs"]:
        return result["expectation_config"]["kwargs"].get("column")
    else:
        return result["expectation_config"]["kwargs"].get("column_list")


def get_unique_deviating_values(deviating_attribute_value: list[str]) -> set[str]:
    unique_deviating_values = set()
    for waarde in deviating_attribute_value:
        if isinstance(waarde, dict):
            waarde = tuple(waarde.items()) #transform because a dict cannot be added to a set
        unique_deviating_values.add(waarde)
    return unique_deviating_values


def filter_df_based_on_deviating_values(
    value: str,
    attribute: str,
    df: DataFrame,
) -> DataFrame:
    if value is None:
        return df.filter(col(attribute).isNull())
    elif isinstance(attribute, list):
        # In case of compound keys, "attribute" is a list and "value" is a dict like tuple.
        # The indeces will match, and we take [1] for value, because the "key" is stored in [0].
        number_of_attrs = len(attribute)
        for i in range(number_of_attrs):
            df = df.filter(col(attribute[i]) == value[i][1])
        return df
    else:
        return df.filter(col(attribute) == value)


def get_grouped_ids_per_deviating_value(
    filtered_df: DataFrame,
    unique_identifier: list[str],
) -> list[str]:
    ids = (
        filtered_df.select(unique_identifier)
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    number_of_unique_ids = len(unique_identifier)
    return [ids[x:x+number_of_unique_ids] for x in range(0, len(ids), number_of_unique_ids)]


def extract_dq_validatie_data(
    table_name: str,
    dataset_name: str,
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
    tabel_id = f"{dataset_name}_{table_name}"
    dq_result = dq_result["validation_results"]

    # run_time = dq_result["meta"]["run_id"].run_time
    run_time = datetime.datetime(1900, 1, 1)
    # TODO: fix, find run_time in new GX API
    
    extracted_data = []
    for validation_result in dq_result:
        for expectation_result in validation_result["expectations"]:
            element_count = int(
                expectation_result["result"].get("element_count", 0)
            )
            unexpected_count = int(
                expectation_result["result"].get("unexpected_count", 0)
            )
            aantal_valide_records = element_count - unexpected_count
            expectation_type = expectation_result["expectation_type"]
            parameter_list = create_parameter_list_from_results(result=expectation_result)
            attribute = expectation_result["kwargs"].get("column")

            output = expectation_result["success"]
            output_text = "success" if output else "failure"
            extracted_data.append(
                {
                    "aantalValideRecords": aantal_valide_records,
                    "aantalReferentieRecords": element_count,
                    "dqDatum": run_time,
                    "dqResultaat": output_text,
                    "regelNaam": expectation_type,
                    "regelParameters": parameter_list,
                    "bronTabelId": tabel_id,
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
    dataset_name: str,
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
    tabel_id = f"{dataset_name}_{table_name}"
    dq_result = dq_result["validation_results"]

    # run_time = dq_result["meta"]["run_id"].run_time
    run_time = datetime.datetime(1900, 1, 1)
    # TODO: fix, find run_time in new GX API
    
    extracted_data = []
    if not isinstance(unique_identifier, list): unique_identifier = [unique_identifier]

    for validation_result in dq_result:
        for expectation_result in validation_result["expectations"]:
            expectation_type = expectation_result["expectation_type"]
            parameter_list = create_parameter_list_from_results(result=expectation_result)
            attribute = get_target_attr_for_rule(result=result)
            deviating_attribute_value = expectation_result["result"].get(
            "partial_unexpected_list", []
            )
            unique_deviating_values = get_unique_deviating_values(
                deviating_attribute_value
            )
            for value in unique_deviating_values:
                filtered_df = filter_df_based_on_deviating_values(
                    value=value,
                    attribute=attribute,
                    df=df
                )
                grouped_ids = get_grouped_ids_per_deviating_value(
                    filtered_df=filtered_df,
                    unique_identifier=unique_identifier
                )
                if isinstance(attribute, list): value = str(value)
                extracted_data.append(
                    {
                        "identifierVeldWaarde": grouped_ids,
                        "afwijkendeAttribuutWaarde": value,
                        "dqDatum": run_time,
                        "regelNaam": expectation_type,
                        "regelParameters": parameter_list,
                        "bronTabelId": tabel_id,
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


def create_brondataset(
    dq_rules_dict: DataQualityRulesDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    Function takes the dataset name and layer from the provided
    Data Quality rules to create a DataFrame containing this metadata.

    :param dq_rules_dict:
    :param catalog_name:
    :param spark_session:
    """
    name = dq_rules_dict["dataset"]["name"]
    layer = dq_rules_dict["dataset"]["layer"]
    extracted_data = [{"bronDatasetId": name, "medaillonLaag": layer}]

    df_brondataset = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=BRONDATASET_SCHEMA,
    )
    merge_dict = {
        "bronDatasetId": "brondataset_df.bronDatasetId",
        "medaillonLaag": "brondataset_df.medaillonLaag",
    }
    merge_df_with_unity_table(
        df=df_brondataset,
        catalog_name=catalog_name,
        table_name="brondataset",
        table_merge_id="bronDatasetId",
        df_merge_id="bronDatasetId",
        merge_dict=merge_dict,
        spark_session=spark_session,
    )


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
    dataset_name = dq_rules_dict["dataset"]["name"]
    for param in dq_rules_dict["tables"]:
        table_name = param["table_name"]
        tabel_id = f"{dataset_name}_{table_name}"
        unique_identifier = param["unique_identifier"]
        extracted_data.append(
            {"bronTabelId": tabel_id,
             "tabelNaam": table_name,
             "uniekeSleutel": unique_identifier}
        )

    df_brontabel = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=BRONTABEL_SCHEMA,
    )
    merge_dict = {
        "bronTabelId": "brontabel_df.bronTabelId",
        "tabelNaam": "brontabel_df.tabelNaam",
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
    dataset_name = dq_rules_dict["dataset"]["name"]
    used_ids = set()  # To keep track of used IDs
    for param in dq_rules_dict["tables"]:
        table_name = param["table_name"]
        tabel_id = f"{dataset_name}_{table_name}"
        for rule in param["rules"]:
            parameters = rule.get("parameters", [])
            for parameter in parameters:
                if isinstance(parameter, dict) and "column" in parameter:
                    attribute_name = parameter["column"]
                    # Create a unique ID
                    unique_id = f"{tabel_id}_{attribute_name}"
                    # Check if the ID is already used
                    if unique_id not in used_ids:
                        used_ids.add(unique_id)
                        extracted_data.append(
                            {
                                "bronAttribuutId": unique_id,
                                "attribuutNaam": attribute_name,
                                "bronTabelId": tabel_id,
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
    dataset_name = dq_rules_dict["dataset"]["name"]
    for table in dq_rules_dict["tables"]:
        table_name = table["table_name"]
        tabel_id = f"{dataset_name}_{table_name}"
        for rule in table["rules"]:
            rule_name = rule["rule_name"]
            parameters = rule.get("parameters", [])
            for param_set in parameters:
                column = param_set.get("column")
                extracted_data.append(
                    {
                        "regelNaam": rule_name,
                        "regelParameters": parameters,
                        "bronTabelId": tabel_id,
                        "attribuut": column
                    }
                )

    df_regel = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=REGEL_SCHEMA,
    )
    df_regel_with_id_ordered = construct_regel_id(
        df=df_regel,
        output_columns_list=['regelId','regelNaam','regelParameters','bronTabelId','attribuut']
    )
    merge_dict = {
        "regelId": "regel_df.regelId",
        "regelNaam": "regel_df.regelNaam",
        "regelParameters": "regel_df.regelParameters",
        "bronTabelId": "regel_df.bronTabelId",
        "attribuut": "regel_df.attribuut"
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
    create_brondataset(
        dq_rules_dict=dq_rules_dict,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
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
    validation_output: CheckpointDescriptionDict,
    validation_settings_obj: ValidationSettings,
    df: DataFrame,
    dataset_name: str,
    unique_identifier: str,
):
    extract_dq_validatie_data(
        validation_settings_obj.table_name,
        validation_output,
        validation_settings_obj.catalog_name,
        validation_settings_obj.spark_session,
    )
    extract_dq_afwijking_data(
        validation_settings_obj.table_name,
        validation_output,
        df,
        unique_identifier,
        validation_settings_obj.catalog_name,
        validation_settings_obj.spark_session,
    )
