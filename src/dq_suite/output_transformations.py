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
    merge_df_with_unity_table,
    write_to_unity_catalog,
)
from .schemas.afwijking import SCHEMA as AFWIJKING_SCHEMA
from .schemas.bronattribuut import SCHEMA as BRONATTRIBUUT_SCHEMA
from .schemas.brondataset import SCHEMA as BRONDATASET_SCHEMA
from .schemas.brontabel import SCHEMA as BRONTABEL_SCHEMA
from .schemas.pre_afwijking import SCHEMA as PRE_AFWIJKING_SCHEMA
from .schemas.pre_validatie import SCHEMA as PRE_VALIDATIE_SCHEMA
from .schemas.regel import SCHEMA as REGEL_SCHEMA
from .schemas.validatie import SCHEMA as VALIDATIE_SCHEMA


def snake_case_to_camel_case(snake_str):
    """
    Convert a snake_case string to a camelCase string. All DQ rules must be camelCase.
    Example: "my_column" -> "myColumn
    """
    return "".join(x.capitalize() for x in snake_str.lower().split("_"))


def convert_param_values_to_float(parameters):
    """
    Convert parameter values for keys that are in the float_list to float.
    """
    float_list = ["min_value", "max_value"]
    for k, v in parameters.items():
        if k in float_list:
            v = round(float(v), 1)
        parameters[k] = v


def create_empty_dataframe(
    spark_session: SparkSession, schema: StructType
) -> DataFrame:
    """
    Create an empty dataframe with the given schema.
    """
    return spark_session.sparkContext.parallelize([]).toDF(schema)


def list_of_dicts_to_df(
    list_of_dicts: List[dict], spark_session: SparkSession, schema: StructType
) -> DataFrame:
    """
    Create a dataframe from a list of dictionaries.
    """
    if not isinstance(list_of_dicts, list):
        raise TypeError("'list_of_dicts' should be of type 'list'")
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
    """
    Construct a regelId from the given dataframe. It is a combination of the regelNaam, regelParameters and bronTabelId.
    """
    if not isinstance(output_columns_list, list):
        raise TypeError("'output_columns_list' should be of type 'list'")
    df_with_id = df.withColumn(
        "regelId",
        xxhash64(
            col("regelNaam"), col("regelParameters"), col("bronTabelId")
        ).substr(2, 20),
    )
    return df_with_id.select(*output_columns_list)


def get_parameters_from_results(result: dict) -> list[dict]:
    """
    Get the parameters from the GX results.
    """
    parameters = result["kwargs"]
    parameters.pop("batch_id", None)
    return parameters


def get_target_attr_for_rule(result: dict) -> str:
    """
    Get the target attribute from the GX results. It will only return results for DQ rules applied to specific attributes.
    """
    if "column" in result["kwargs"]:
        return result["kwargs"].get("column")
    else:
        return result["kwargs"].get("column_list")


def get_unique_deviating_values(
    deviating_attribute_value: list[str],
) -> set[str]:
    """
    Get the unique deviating values from the GX results.
    """
    unique_deviating_values = set()
    for waarde in deviating_attribute_value:
        if isinstance(waarde, dict):
            waarde = tuple(
                waarde.items()
            )  # transform because a dict cannot be added to a set
        unique_deviating_values.add(waarde)
    return unique_deviating_values


def filter_df_based_on_deviating_values(
    value: str,
    attribute: str,
    df: DataFrame,
) -> DataFrame:
    """
    Filter the dataframe based on the deviating values. The output will contain only records that did not conform to the expectations set.
    """
    if value is None:
        return df.filter(col(attribute).isNull())
    elif isinstance(attribute, list):
        # In case of compound keys, "attribute" is a list and "value" is a dict
        # like tuple. The indeces will match, and we take [1] for value,
        # because the "key" is stored in [0].
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
    """
    Get the grouped ids per deviating value.
    """
    ids = (
        filtered_df.select(unique_identifier).rdd.flatMap(lambda x: x).collect()
    )
    number_of_unique_ids = len(unique_identifier)
    return [
        ids[x : x + number_of_unique_ids]
        for x in range(0, len(ids), number_of_unique_ids)
    ]


def extract_dataset_data(dq_rules_dict: dict) -> list[dict]:
    """
    Extract the dataset data from the dq_rules_dict.
    """
    name = dq_rules_dict["dataset"]["name"]
    layer = dq_rules_dict["dataset"]["layer"]
    return [{"bronDatasetId": name, "medaillonLaag": layer}]


def extract_table_data(dq_rules_dict: dict) -> list[dict]:
    """
    Extract the table data from the dq_rules_dict.
    """
    extracted_data = []
    dataset_name = dq_rules_dict["dataset"]["name"]
    for param in dq_rules_dict["tables"]:
        table_name = param["table_name"]
        tabel_id = f"{dataset_name}_{table_name}"
        unique_identifier = param["unique_identifier"]
        extracted_data.append(
            {
                "bronTabelId": tabel_id,
                "tabelNaam": table_name,
                "uniekeSleutel": unique_identifier,
            }
        )
    return extracted_data


def extract_attribute_data(dq_rules_dict: dict) -> list[dict]:
    """
    Extract the attribute data from the dq_rules_dict.
    """
    extracted_data = []
    dataset_name = dq_rules_dict["dataset"]["name"]
    used_ids = set()  # To keep track of used IDs
    for param in dq_rules_dict["tables"]:
        table_name = param["table_name"]
        tabel_id = f"{dataset_name}_{table_name}"
        for rule in param["rules"]:
            parameters = rule.get("parameters", [])
            if isinstance(parameters, dict) and "column" in parameters:
                attribute_name = parameters["column"]
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
    return extracted_data


def extract_regel_data(dq_rules_dict: dict) -> list[dict]:
    """
    Extract the regel data from the dq_rules_dict.
    """
    extracted_data = []
    dataset_name = dq_rules_dict["dataset"]["name"]
    for table in dq_rules_dict["tables"]:
        table_name = table["table_name"]
        tabel_id = f"{dataset_name}_{table_name}"
        for rule in table["rules"]:
            rule_name = rule["rule_name"]
            parameters = rule.get("parameters")
            convert_param_values_to_float(parameters)
            norm = rule.get("norm", None)
            column = parameters.get("column", None)
            extracted_data.append(
                {
                    "regelNaam": rule_name,
                    "regelParameters": parameters,
                    "norm": norm,
                    "bronTabelId": tabel_id,
                    "attribuut": column,
                }
            )
    return extracted_data


def extract_validatie_data(
    table_name: str,
    dataset_name: str,
    run_time: datetime,
    dq_result: CheckpointDescriptionDict,
) -> list[dict]:
    """
    Extract the validatie data from the dq_rules_dict.
    """
    # "validation_results" is typed List[Dict[str, Any]] in GX
    dq_result = dq_result["validation_results"]
    tabel_id = f"{dataset_name}_{table_name}"

    extracted_data = []
    for validation_result in dq_result:
        for expectation_result in validation_result["expectations"]:
            element_count = int(
                expectation_result["result"].get("element_count", 0)
            )
            unexpected_count = int(
                expectation_result["result"].get("unexpected_count", 0)
            )
            percentage_of_valid_records = float(
                int(
                    100
                    - expectation_result["result"].get("unexpected_percent", 0)
                )
                / 100
            )
            number_of_valid_records = element_count - unexpected_count
            expectation_type = expectation_result["expectation_type"]
            if "_" in expectation_type:
                expectation_type = snake_case_to_camel_case(expectation_type)
            parameter_list = get_parameters_from_results(
                result=expectation_result
            )
            expectation_result["kwargs"].get("column")

            output = expectation_result["success"]
            output_text = "success" if output else "failure"
            extracted_data.append(
                {
                    "aantalValideRecords": number_of_valid_records,
                    "aantalReferentieRecords": element_count,
                    "percentageValideRecords": percentage_of_valid_records,
                    "dqDatum": run_time,
                    "dqResultaat": output_text,
                    "regelNaam": expectation_type,
                    "regelParameters": parameter_list,
                    "bronTabelId": tabel_id,
                }
            )
    return extracted_data


def extract_afwijking_data(
    df: DataFrame,
    unique_identifier: str,
    table_name: str,
    dataset_name: str,
    run_time: datetime,
    dq_result: CheckpointDescriptionDict,
) -> list[dict]:
    """
    Extract the afwijking data from the dq_rules_dict.
    """
    # "validation_results" is typed List[Dict[str, Any]] in GX
    dq_result = dq_result["validation_results"]
    tabel_id = f"{dataset_name}_{table_name}"

    extracted_data = []
    if not isinstance(unique_identifier, list):
        unique_identifier = [unique_identifier]

    for validation_result in dq_result:
        for expectation_result in validation_result["expectations"]:
            expectation_type = expectation_result["expectation_type"]
            if "_" in expectation_type:
                expectation_type = snake_case_to_camel_case(expectation_type)
            parameter_list = get_parameters_from_results(
                result=expectation_result
            )
            attribute = get_target_attr_for_rule(result=expectation_result)
            deviating_attribute_value = expectation_result["result"].get(
                "partial_unexpected_list", []
            )
            unique_deviating_values = get_unique_deviating_values(
                deviating_attribute_value
            )
            for value in unique_deviating_values:
                filtered_df = filter_df_based_on_deviating_values(
                    value=value, attribute=attribute, df=df
                )
                grouped_ids = get_grouped_ids_per_deviating_value(
                    filtered_df=filtered_df, unique_identifier=unique_identifier
                )
                if isinstance(attribute, list):
                    value = str(value)
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
    return extracted_data


def create_dq_validatie(
    table_name: str,
    dataset_name: str,
    run_time: datetime,
    dq_result: CheckpointDescriptionDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param table_name: Name of the tables
    :param dataset_name:
    :param run_time:
    :param dq_result:  # TODO: add dataclass?
    :param catalog_name:
    :param spark_session:
    """
    extracted_data = extract_validatie_data(
        table_name=table_name,
        dataset_name=dataset_name,
        run_time=run_time,
        dq_result=dq_result,
    )
    df_validatie = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=PRE_VALIDATIE_SCHEMA,
    )
    df_validatie_with_id_ordered = construct_regel_id(
        df=df_validatie,
        output_columns_list=[
            "regelId",
            "aantalValideRecords",
            "aantalReferentieRecords",
            "percentageValideRecords",
            "dqDatum",
            "dqResultaat",
        ],
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


def create_dq_afwijking(
    table_name: str,
    dataset_name: str,
    dq_result: CheckpointDescriptionDict,
    df: DataFrame,
    unique_identifier: str,
    run_time: datetime,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param table_name: Name of the table
    :param dataset_name:
    :param dq_result:
    :param df: A DataFrame containing the invalid (deviated) result
    :param unique_identifier:
    :param run_time:
    :param catalog_name:
    :param spark_session:
    """
    extracted_data = extract_afwijking_data(
        df=df,
        unique_identifier=unique_identifier,
        table_name=table_name,
        dataset_name=dataset_name,
        run_time=run_time,
        dq_result=dq_result,
    )
    df_afwijking = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=PRE_AFWIJKING_SCHEMA,
    )
    df_afwijking_with_id_ordered = construct_regel_id(
        df=df_afwijking,
        output_columns_list=[
            "regelId",
            "identifierVeldWaarde",
            "afwijkendeAttribuutWaarde",
            "dqDatum",
        ],
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
    extracted_data = extract_dataset_data(dq_rules_dict=dq_rules_dict)

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
    extracted_data = extract_table_data(dq_rules_dict=dq_rules_dict)

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
    extracted_data = extract_attribute_data(dq_rules_dict=dq_rules_dict)

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
    extracted_data = extract_regel_data(dq_rules_dict=dq_rules_dict)

    df_regel = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=REGEL_SCHEMA,
    )
    df_regel_with_id_ordered = construct_regel_id(
        df=df_regel,
        output_columns_list=[
            "regelId",
            "regelNaam",
            "regelParameters",
            "norm",
            "bronTabelId",
            "attribuut",
        ],
    )
    merge_dict = {
        "regelId": "regel_df.regelId",
        "regelNaam": "regel_df.regelNaam",
        "regelParameters": "regel_df.regelParameters",
        "norm": "regel_df.norm",
        "bronTabelId": "regel_df.bronTabelId",
        "attribuut": "regel_df.attribuut",
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
    run_time: datetime,
):
    create_dq_validatie(
        table_name=validation_settings_obj.table_name,
        dataset_name=dataset_name,
        run_time=run_time,
        dq_result=validation_output,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
    create_dq_afwijking(
        table_name=validation_settings_obj.table_name,
        dataset_name=dataset_name,
        dq_result=validation_output,
        df=df,
        unique_identifier=unique_identifier,
        run_time=run_time,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
