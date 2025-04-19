import copy
import datetime
from typing import List, Dict, Any

from great_expectations.checkpoint.checkpoint import CheckpointDescriptionDict
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, xxhash64, lit
from pyspark.sql.types import StructType

from .common import (
    DataQualityRulesDict,
    ValidationSettings,
    is_empty_dataframe,
    merge_df_with_unity_table,
    write_to_unity_catalog, Rule,
)
from .schemas.afwijking import SCHEMA as AFWIJKING_SCHEMA
from .schemas.bronattribuut import SCHEMA as BRONATTRIBUUT_SCHEMA
from .schemas.brondataset import SCHEMA as BRONDATASET_SCHEMA
from .schemas.brontabel import SCHEMA as BRONTABEL_SCHEMA
from .schemas.pre_afwijking import SCHEMA as PRE_AFWIJKING_SCHEMA
from .schemas.pre_validatie import SCHEMA as PRE_VALIDATIE_SCHEMA
from .schemas.regel import SCHEMA as REGEL_SCHEMA
from .schemas.validatie import SCHEMA as VALIDATIE_SCHEMA


# def snake_case_to_camel_case(snake_str):
#     """
#     Convert a snake_case string to a camelCase string. All DQ rules must be camelCase.
#     Example: "my_column" -> "myColumn
#     """
#     return "".join(x.capitalize() for x in snake_str.lower().split("_"))


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


def add_regel_id_column(
    df: DataFrame,
) -> DataFrame:
    """
    Construct a regelId from the given dataframe by hashing the values of the
    regelNaam, regelParameters and bronTabelId columns.
    """
    if "regelNaam" not in df.columns:
        raise ValueError(f"Cannot compute hash: 'regelNaam' not found in "
                         f"columns: {df.columns}")
    if "regelParameters" not in df.columns:
        raise ValueError(f"Cannot compute hash: 'regelParameters' not found "
                         f"in columns: {df.columns}")
    if "bronTabelId" not in df.columns:
        raise ValueError(f"Cannot compute hash: 'bronTabelId' not found in "
                         f"columns: {df.columns}")

    df_with_id = df.withColumn(
        "regelId",
        xxhash64(
            col("regelNaam"), col("regelParameters"), col("bronTabelId")
        ).substr(2, 20),  # TODO/check: why characters 2-20?
    )
    return df_with_id


def get_parameters_from_results(result: dict) -> list[dict]:
    """
    Get the parameters from the GX results.
    """
    parameters = result["kwargs"]
    parameters.pop("batch_id", None)
    # TODO/check: why is batch_id removed? Shouldn't this be documented?
    return parameters


def get_target_attr_for_rule(result: dict) -> str:
    """
    Get the target attribute from the GX results. It will only return results
    for DQ rules applied to specific attributes.
    """
    if "column" in result["kwargs"]:
        return result["kwargs"].get("column")
    else:  # TODO/check: what if 'column_list' doesnt exist?
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
    Filter the dataframe based on the deviating values.
    The output will contain only records that did not conform to the
    expectations set.
    """
    if value is None:
        return df.filter(col(attribute).isNull())
    elif isinstance(attribute, list):
        # In case of compound keys, "attribute" is a list and "value" is a dict
        # like tuple. The indices will match, and we take [1] for value,
        # because the "key" is stored in [0].
        number_of_attrs = len(attribute)
        for i in range(number_of_attrs):
            df = df.filter(col(attribute[i]) == value[i][1])
        return df
    else:
        return df.filter(col(attribute) == lit(value))


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


def extract_brondataset_data(dq_rules_dict: DataQualityRulesDict) -> list[dict]:
    """
    Extract the dataset data from the dq_rules_dict.
    """
    name = dq_rules_dict["dataset"]["name"]
    layer = dq_rules_dict["dataset"]["layer"]
    return [{"bronDatasetId": name, "medaillonLaag": layer}]


def extract_brontabel_data(dq_rules_dict: DataQualityRulesDict) -> list[dict]:
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


def extract_bronattribuut_data(dq_rules_dict: DataQualityRulesDict) -> list[dict]:
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


def get_single_rule_dict(rule: Rule, table_id: str) -> dict:
    parameters = copy.deepcopy(rule["parameters"])

    # Round min/max values (if present) to a single decimal
    # TODO/check: why cast to float of min/max values?
    if "min_value" in parameters.keys():
        min_value = float(parameters["min_value"])
        parameters["min_value"] = round(min_value, 1)
    if "max_value" in parameters.keys():
        max_value = float(parameters["max_value"])
        parameters["max_value"] = round(max_value, 1)

    return {
            "regelNaam": rule["rule_name"],
            "regelParameters": parameters,  # TODO/check: this includes 'column'?
            "norm":  rule["norm"],
            "bronTabelId": table_id,
            "attribuut": parameters.get("column", None),
        }


def extract_regel_data(dq_rules_dict: DataQualityRulesDict) -> list[dict]:
    """
    Extract the regel data from the dq_rules_dict.
    """
    extracted_data = []
    dataset_name = dq_rules_dict["dataset"]["name"]
    for table in dq_rules_dict["tables"]:
        table_id = f"{dataset_name}_{table['table_name']}"
        for rule in table["rules"]:
            extracted_data.append(
                get_single_rule_dict(rule=rule, table_id=table_id)
            )
    return extracted_data


def extract_validatie_data(
    table_name: str,
    dataset_name: str,
    run_time: datetime,
    validation_output: CheckpointDescriptionDict,
) -> list[dict]:
    """
    Extract the validatie data from the dq_rules_dict.
    """
    validation_results: List[Dict[str, Any]] = validation_output["validation_results"]
    tabel_id = f"{dataset_name}_{table_name}"

    extracted_data = []
    for result in validation_results:
        for expectation_result in result["expectations"]:
            total_count = int(
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

            extracted_data.append(
                {
                    "aantalValideRecords": total_count - unexpected_count,
                    "aantalReferentieRecords": total_count,
                    "percentageValideRecords": percentage_of_valid_records,
                    "dqDatum": run_time,  # TODO/check: why is a 'datum' assigned a timestamp?
                    "dqResultaat": "success" if expectation_result["success"] else "failure",
                    "regelNaam": expectation_result["expectation_type"],
                    "regelParameters": get_parameters_from_results(result=expectation_result),
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
    validation_output: CheckpointDescriptionDict,
) -> list[dict]:
    """
    Extract the afwijking data from the dq_rules_dict.
    """
    validation_results: List[Dict[str, Any]] = validation_output["validation_results"]
    tabel_id = f"{dataset_name}_{table_name}"

    extracted_data = []
    if not isinstance(unique_identifier, list):
        unique_identifier = [unique_identifier]

    for result in validation_results:
        for expectation_result in result["expectations"]:
            expectation_type = expectation_result["expectation_type"]
            # TODO/check - no longer needed, this should be caught by input_validation
            # if "_" in expectation_type:
            #     expectation_type = snake_case_to_camel_case(expectation_type)
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
    validation_output: CheckpointDescriptionDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    """
    [insert explanation here]

    :param table_name: Name of the tables
    :param dataset_name:
    :param run_time:
    :param validation_output:  # TODO: add dataclass?
    :param catalog_name:
    :param spark_session:
    """
    extracted_data = extract_validatie_data(
        table_name=table_name,
        dataset_name=dataset_name,
        run_time=run_time,
        validation_output=validation_output,
    )
    df_validatie = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=PRE_VALIDATIE_SCHEMA,
    )
    df_validatie_with_id_ordered = add_regel_id_column(
        df=df_validatie,
    ).select("regelId", "aantalValideRecords", "aantalReferentieRecords",
             "percentageValideRecords", "dqDatum", "dqResultaat")
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
    validation_output: CheckpointDescriptionDict,
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
    :param validation_output:
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
        validation_output=validation_output,
    )
    df_afwijking = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=PRE_AFWIJKING_SCHEMA,
    )
    df_afwijking_with_id_ordered = add_regel_id_column(
        df=df_afwijking,
    ).select("regelId", "identifierVeldWaarde", "afwijkendeAttribuutWaarde",
             "dqDatum")
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


def create_metadata_dataframe(table_name: str, dq_rules_dict:
DataQualityRulesDict, spark_session: SparkSession) -> DataFrame:
    if table_name == "brondataset":
        extracted_data = extract_brondataset_data(dq_rules_dict=dq_rules_dict)
        schema = BRONDATASET_SCHEMA
    elif table_name == "brontabel":
        extracted_data = extract_brontabel_data(dq_rules_dict=dq_rules_dict)
        schema = BRONTABEL_SCHEMA
    elif table_name == "bronattribuut":
        extracted_data = extract_bronattribuut_data(dq_rules_dict=dq_rules_dict)
        schema = BRONATTRIBUUT_SCHEMA
    elif table_name == "regel":
        extracted_data = extract_regel_data(dq_rules_dict=dq_rules_dict)
        schema = REGEL_SCHEMA
    else:
        raise ValueError(f"Unknown metadata table name '{table_name}'")

    df = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=schema,
    )

    if table_name == "regel":
        return add_regel_id_column(
                df=df,
            ).select("regelId", *REGEL_SCHEMA.fieldNames())
    return df


def write_validation_metadata_tables(
    dq_rules_dict: DataQualityRulesDict,
    validation_settings_obj: ValidationSettings,
) -> None:
    metadata_table_names = ["brondataset", "brontabel", "bronattribuut",
                            "regel"]

    for table_name in metadata_table_names:
        df = create_metadata_dataframe(table_name=table_name,
                                       dq_rules_dict=dq_rules_dict,
                                       spark_session=validation_settings_obj.spark_session)

        merge_df_with_unity_table(
            df=df,
            catalog_name=validation_settings_obj.catalog_name,
            table_name=table_name,
            spark_session=validation_settings_obj.spark_session,
        )


def write_validation_tables(
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
        validation_output=validation_output,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
    create_dq_afwijking(
        table_name=validation_settings_obj.table_name,
        dataset_name=dataset_name,
        validation_output=validation_output,
        df=df,
        unique_identifier=unique_identifier,
        run_time=run_time,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
