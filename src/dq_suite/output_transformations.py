import copy
import datetime
from typing import Any, Dict, List
import humps

import humps
from great_expectations.checkpoint.checkpoint import (
    CheckpointDescriptionDict,
    CheckpointResult,
)
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, lit, xxhash64
from pyspark.sql.types import StructType

from .common import (
    DataQualityRulesDict,
    DatasetDict,
    Rule,
    RulesDict,
    ValidationSettings,
    enforce_column_order,
    is_empty_dataframe,
    merge_df_with_unity_table,
    write_to_unity_catalog,
)
from .schemas.afwijking import SCHEMA as AFWIJKING_SCHEMA
from .schemas.bronattribuut import SCHEMA as BRONATTRIBUUT_SCHEMA
from .schemas.brondataset import SCHEMA as BRONDATASET_SCHEMA
from .schemas.brontabel import SCHEMA as BRONTABEL_SCHEMA
from .schemas.regel import SCHEMA as REGEL_SCHEMA
from .schemas.regel_id_input import SCHEMA as REGEL_ID_INPUT_SCHEMA
from .schemas.validatie import SCHEMA as VALIDATIE_SCHEMA


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
        raise ValueError(
            f"Cannot compute hash: 'regelNaam' not found in "
            f"columns: {df.columns}"
        )
    if "regelParameters" not in df.columns:
        raise ValueError(
            f"Cannot compute hash: 'regelParameters' not found "
            f"in columns: {df.columns}"
        )
    if "bronTabelId" not in df.columns:
        raise ValueError(
            f"Cannot compute hash: 'bronTabelId' not found in "
            f"columns: {df.columns}"
        )

    df_with_id = df.withColumn(
        "regelId",
        xxhash64(
            col("regelNaam"), col("regelParameters"), col("bronTabelId")
        ).substr(
            2, 20
        ),  # We start from the 2nd hash value to avoid negative values. This increases performance in PowerBI
    )
    return df_with_id

def round_numeric_params(params: dict) -> dict:
    params = copy.deepcopy(params)
    for k in ("min_value", "max_value", "value"):
        if k in params and params[k] is not None:
            params[k] = round(float(params[k]), 1)
    return params

def get_parameters_from_results(result: dict) -> list[dict]:
    """
    Get the parameters from the GX results.
    """
    parameters = copy.deepcopy(result["kwargs"])
    if "batch_id" in parameters:
        del parameters[
            "batch_id"
        ]  # We don't need this value. It describes the data, but is not relevant for the rule description
    return parameters


def get_target_attr_for_rule(result: dict) -> str | None:
    """
    Get the target attribute from the GX results. It will only return results
    for DQ rules applied to specific attributes.
    """
    if "column" in result["kwargs"]:
        return result["kwargs"].get("column")
    elif "column_list" in result["kwargs"]:
        return result["kwargs"].get("column_list")
    else:
        # Some rules do not specify columns, but are scoped on table level
        return None


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
    deviating_value: str,
    attribute: str,
    df: DataFrame,
) -> DataFrame:
    """
    Filter the dataframe based on the deviating values.
    The output will contain only records that did not conform to the
    expectations set.

    # TODO: add documentation per parameter.
    """
    if deviating_value is None:
        return df.filter(col(attribute).isNull())
    elif isinstance(attribute, list):
        # In case of compound keys, "attribute" is a list and "deviating_value" is a dict
        # like tuple. The indices will match, and we take [1] for deviating_value,
        # because the "key" is stored in [0].
        number_of_attrs = len(attribute)
        for i in range(number_of_attrs):
            df = df.filter(col(attribute[i]) == deviating_value[i][1])
        return df
    else:
        return df.filter(col(attribute) == lit(deviating_value))


def get_grouped_ids_per_deviating_value(
    filtered_df: DataFrame,
    unique_identifier: list[str],
) -> list[str]:
    """
    Get the grouped ids per deviating value.
    """
    # TODO: add documentation. This function is very complex.
    ids = (
        filtered_df.select(unique_identifier).rdd.flatMap(lambda x: x).collect()
    )
    number_of_unique_ids = len(unique_identifier)
    return [
        ids[x : x + number_of_unique_ids]
        for x in range(0, len(ids), number_of_unique_ids)
    ]


def get_brondataset_data(dq_rules_dict: DataQualityRulesDict) -> list[dict]:
    """
    Get the dataset data from the dq_rules_dict.
    """
    dataset_dict: DatasetDict = dq_rules_dict["dataset"]
    return [
        {
            "bronDatasetId": dataset_dict["name"],
            "medaillonLaag": dataset_dict["layer"],
        }
    ]


def get_single_brontabel_dict(dataset_name: str, rules_dict: RulesDict) -> dict:
    table_name = rules_dict["table_name"]
    unique_identifier = rules_dict["unique_identifier"]
    table_id = f"{dataset_name}_{table_name}"
    return {
        "bronTabelId": table_id,
        "tabelNaam": table_name,
        "uniekeSleutel": unique_identifier,
    }


def get_brontabel_data(dq_rules_dict: DataQualityRulesDict) -> list[dict]:
    """
    Get the table data from the dq_rules_dict.
    """
    extracted_data = []
    dataset_name = dq_rules_dict["dataset"]["name"]
    for rules_dict in dq_rules_dict["tables"]:
        extracted_data.append(
            get_single_brontabel_dict(
                dataset_name=dataset_name, rules_dict=rules_dict
            )
        )
    return extracted_data


def get_single_bronattribuut_dict(rule: Rule, table_id: str) -> dict:
    parameters = copy.deepcopy(rule["parameters"])

    if isinstance(parameters, dict) and "column" in parameters:
        attribute_name = parameters["column"]
        unique_id = f"{table_id}_{attribute_name}"
        return {
            "bronAttribuutId": unique_id,
            "attribuutNaam": attribute_name,
            "bronTabelId": table_id,
        }
    return dict()


def get_bronattribuut_data(
    dq_rules_dict: DataQualityRulesDict,
) -> list[dict]:
    """
    Get the attribute data from the dq_rules_dict.
    """
    extracted_data = []
    dataset_name = dq_rules_dict["dataset"]["name"]
    bronattribuut_id_set = set()  # To keep track of used IDs
    for param in dq_rules_dict["tables"]:
        table_name = param["table_name"]
        table_id = f"{dataset_name}_{table_name}"
        for rule in param["rules"]:
            bronattribuut_dict = get_single_bronattribuut_dict(
                rule=rule, table_id=table_id
            )
            bronattribuut_id = bronattribuut_dict.get("bronAttribuutId", None)
            if (len(bronattribuut_dict) != 0) and (
                bronattribuut_id not in bronattribuut_id_set
            ):
                bronattribuut_id_set.add(bronattribuut_id)
                extracted_data.append(bronattribuut_dict)
    return extracted_data


def get_single_rule_dict(rule: Rule, table_id: str) -> dict:
    parameters = copy.deepcopy(rule["parameters"])

    # Round min/max values (if present) to a single decimal
    # GX does this in the background, so we need to match the behaviour to keep integrity between regelId in the tables.
    parameters = round_numeric_params(parameters)

    return {
        "regelNaam": humps.pascalize(rule["rule_name"]),
        "regelParameters": parameters,
        "norm": rule.get("norm", None),
        "bronTabelId": table_id,
        "attribuut": parameters.get("column", None),
        "severity": rule.get("severity", "ok"),
    }


def get_regel_data(dq_rules_dict: DataQualityRulesDict) -> list[dict]:
    """
    Get the regel data from the dq_rules_dict.
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

def _is_number(x):
    # Return True only for real numbers (int/float). Explicitly exclude boolean values.
    return isinstance(x, (int, float)) and not isinstance(x, bool)

def get_single_validation_result_dict(
    expectation_result: dict, run_time: datetime, table_id: str
) -> dict:
    expectation_type: str = expectation_result.get("expectation_type", "")
    result: dict = expectation_result.get("result", {}) or {}
    is_row_count_exp = expectation_type.startswith("expect_table_row_count_to_")

    # defaults
    total_count = None
    valid_records = None
    percentage_of_valid_records = None

    if not is_row_count_exp:
        # Non row-count expectations:
        # total_count comes from element_count (if numeric)
        element_count_value = result.get("element_count")
        if _is_number(element_count_value):
            total_count = int(element_count_value)

            # valid_records if unexpected_count is numeric
            unexpected_count_value = result.get("unexpected_count")
            if _is_number(unexpected_count_value):
                valid_records = max(total_count - int(unexpected_count_value), 0)

        # percentage_of_valid_records if unexpected_percent is numeric
        unexpected_percent_value = result.get("unexpected_percent")
        if _is_number(unexpected_percent_value):
            percentage_of_valid_records = int(100.0 - float(unexpected_percent_value)) / 100.0
    else:
        # Table row-count expectations:
        # total_count comes from observed_value (if numeric); other two metrics do not apply.
        observed_value = result.get("observed_value")
        if _is_number(observed_value):
            total_count = int(observed_value)

    validation_result = "success" if expectation_result["success"] else "failure"

    validation_parameters = round_numeric_params( 
        get_parameters_from_results(result=expectation_result)
    )

    return {
        "aantalValideRecords": valid_records,
        "aantalReferentieRecords": total_count,
        "percentageValideRecords": percentage_of_valid_records,
        # TODO/check: rename dqDatum, discuss all field names
        "dqDatum": run_time,
        "dqResultaat": validation_result,
        "regelNaam": humps.pascalize(expectation_type),
        "regelParameters": validation_parameters,
        "bronTabelId": table_id,
    }


def get_validatie_data(
    validation_settings_obj: ValidationSettings,
    run_time: datetime,
    validation_output: CheckpointDescriptionDict,
) -> list[dict]:
    """
    Get the validatie data from the dq_rules_dict.
    """
    validation_results: List[Dict[str, Any]] = validation_output[
        "validation_results"
    ]
    table_id = (
        f"{validation_settings_obj.dataset_name}_"
        f"{validation_settings_obj.table_name}"
    )

    extracted_data = []
    for result in validation_results:
        for expectation_result in result["expectations"]:
            extracted_data.append(
                get_single_validation_result_dict(
                    expectation_result=expectation_result,
                    run_time=run_time,
                    table_id=table_id,
                )
            )
    return extracted_data


def get_single_expectation_afwijking_data(
    expectation_result: Any,
    df: DataFrame,
    unique_identifier: list[str],
    run_time: datetime,
    table_id: str,
) -> list[dict]:
    extracted_data = []
    expectation_type = expectation_result["expectation_type"]
    parameter_list = round_numeric_params(
        get_parameters_from_results(result=expectation_result)
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
            deviating_value=value, attribute=attribute, df=df
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
                # TODO/check: rename dqDatum, discuss all field names
                "regelNaam": humps.pascalize(expectation_type),
                "regelParameters": parameter_list,
                "bronTabelId": table_id,
            }
        )

    return extracted_data


def get_afwijking_data(
    df: DataFrame,
    validation_settings_obj: ValidationSettings,
    run_time: datetime,
    validation_output: CheckpointDescriptionDict,
) -> list[dict]:
    """
    Get the afwijking data from the dq_rules_dict.
    """
    validation_results: List[Dict[str, Any]] = validation_output[
        "validation_results"
    ]
    table_id = (
        f"{validation_settings_obj.dataset_name}_"
        f"{validation_settings_obj.table_name}"
    )
    unique_identifier = validation_settings_obj.unique_identifier

    extracted_data = []
    if not isinstance(
        unique_identifier, list
    ):  # TODO/check: is this always a list[str]?
        unique_identifier = [unique_identifier]

    for result in validation_results:
        for expectation_result in result["expectations"]:
            extracted_data += get_single_expectation_afwijking_data(
                expectation_result=expectation_result,
                df=df,
                unique_identifier=unique_identifier,
                run_time=run_time,
                table_id=table_id,
            )
    return extracted_data


def create_metadata_dataframe(
    metadata_table_name: str,
    dq_rules_dict: DataQualityRulesDict,
    spark_session: SparkSession,
) -> DataFrame:
    if metadata_table_name == "brondataset":
        extracted_data = get_brondataset_data(dq_rules_dict=dq_rules_dict)
        schema = BRONDATASET_SCHEMA
    elif metadata_table_name == "brontabel":
        extracted_data = get_brontabel_data(dq_rules_dict=dq_rules_dict)
        schema = BRONTABEL_SCHEMA
    elif metadata_table_name == "bronattribuut":
        extracted_data = get_bronattribuut_data(dq_rules_dict=dq_rules_dict)
        schema = BRONATTRIBUUT_SCHEMA
    elif metadata_table_name == "regel":
        extracted_data = get_regel_data(dq_rules_dict=dq_rules_dict)
        schema = REGEL_SCHEMA
    else:
        raise ValueError(f"Unknown metadata table name '{metadata_table_name}'")
    df = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=spark_session,
        schema=schema,
    )

    if metadata_table_name == "regel":
        return add_regel_id_column(
            df=df,
        ).select("regelId", *REGEL_SCHEMA.fieldNames())
    return df


def write_validation_metadata_tables(
    dq_rules_dict: DataQualityRulesDict,
    validation_settings_obj: ValidationSettings,
) -> None:
    metadata_table_names = [
        "brondataset",
        "brontabel",
        "bronattribuut",
        "regel",
    ]

    for metadata_table_name in metadata_table_names:
        df = create_metadata_dataframe(
            metadata_table_name=metadata_table_name,
            dq_rules_dict=dq_rules_dict,
            spark_session=validation_settings_obj.spark_session,
        )

        merge_df_with_unity_table(
            df=df,
            catalog_name=validation_settings_obj.catalog_name,
            table_name=metadata_table_name,
            spark_session=validation_settings_obj.spark_session,
        )


def create_validation_result_dataframe(
    df: DataFrame,
    checkpoint_result: CheckpointResult,
    validation_table_name: str,
    validation_settings_obj: ValidationSettings,
) -> DataFrame:
    validation_output = checkpoint_result.describe_dict()
    run_time = checkpoint_result.run_id.run_time

    if validation_table_name == "validatie":
        extracted_data = get_validatie_data(
            validation_settings_obj=validation_settings_obj,
            run_time=run_time,
            validation_output=validation_output,
        )
        schema = VALIDATIE_SCHEMA
    elif validation_table_name == "afwijking":
        extracted_data = get_afwijking_data(
            df=df,
            validation_settings_obj=validation_settings_obj,
            run_time=run_time,
            validation_output=validation_output,
        )
        schema = AFWIJKING_SCHEMA
    else:
        raise ValueError(
            f"Unknown validation result table name '"
            f"{validation_table_name}'"
        )

    # StructType doesn't support .drop(), so use a workaround
    reduced_schema = StructType()
    for structfield in schema:
        if structfield.name == "regelId":
            continue
        reduced_schema = reduced_schema.add(
            structfield.name, structfield.dataType, structfield.nullable
        )
    for inputfield in REGEL_ID_INPUT_SCHEMA.fields:
        reduced_schema = reduced_schema.add(inputfield)
    df = list_of_dicts_to_df(
        list_of_dicts=extracted_data,
        spark_session=validation_settings_obj.spark_session,
        schema=reduced_schema,
    )  # Note: regelId is added below

    df = add_regel_id_column(df=df)
    return enforce_column_order(df=df, schema=schema)


def write_validation_result_tables(
    df: DataFrame,
    checkpoint_result: CheckpointResult,
    validation_settings_obj: ValidationSettings,
):
    validation_result_table_names = ["validatie", "afwijking"]

    for validation_table_name in validation_result_table_names:
        df_validation_result = create_validation_result_dataframe(
            df=df,
            checkpoint_result=checkpoint_result,
            validation_table_name=validation_table_name,
            validation_settings_obj=validation_settings_obj,
        )

        assert not is_empty_dataframe(
            df=df_validation_result
        ), f"No validation results to write for table '{validation_table_name}'."

        if validation_table_name == "validatie":
            schema = VALIDATIE_SCHEMA
        elif validation_table_name == "afwijking":
            schema = AFWIJKING_SCHEMA
        else:
            raise ValueError(
                f"Unknown validation result table name '{validation_table_name}'"
            )

        write_to_unity_catalog(
            df=df_validation_result,
            catalog_name=validation_settings_obj.catalog_name,
            table_name=validation_table_name,
            schema=schema,
        )


def get_highest_severity_from_validation_result(validation_result: dict, rules_dict: dict) -> str:
    """
    validation_result: dict containing ValidationResult["results"] (from checkpoint_result.run_results.values()[0])
    rules_dict: Dictionary of rules containing rule_name and severity under the 'rules' key

    Returns:
        The highest severity level ('fatal', 'error', 'warning', 'ok') 
    """

    rules_by_name = {
        rule["rule_name"]: rule["severity"]
        for rule in rules_dict.get("rules", [])
    }

    failed_severities = []

    severity_priority = {"fatal": 3, "error": 2, "warning": 1, "ok": 0}
    
    for result in validation_result.get("results", []):
        if result.get("success") is False:
            expectation_type = result["expectation_config"]["type"]
            rule_name = humps.pascalize(expectation_type)
            severity = rules_by_name.get(rule_name)
            if severity:
                failed_severities.append(severity)

    if not failed_severities:
        failed_severities.append("ok")

    highest_severity = max(failed_severities, key=lambda sev: severity_priority.get(sev, 0))
    return highest_severity