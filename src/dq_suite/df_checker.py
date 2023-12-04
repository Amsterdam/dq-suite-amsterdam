import pandas as pd
import itertools

# for df_dqValidatie
def extract_dq_validatie_data(check_name, dq_result):
    """
    Function takes a json dq_rules,and a string check_name and returns dataframe.
    
    :param df_dq_validatie: A df containing the valid result
    :type df: DataFrame
    :param dq_rules: A JSON string containing the Data Quality rules to be evaluated
    :type dq_rules: str
    :param check_name: Name of the run for reference purposes
    :type check_name: str
    :param output: A boolean containing is success true or false 
    :type output: boolean
    :return: A table df with the valid result DQ results, parsed from the extract_dq_validatie_data output
    :rtype: df.
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
        dq_regel_id = f"{check_name}_{expectation_type}_{attribute}"
        output=result["success"]
        output_text = "success" if output else "failure"
        extracted_data.append({
            "dqRegelId": dq_regel_id,
            "aantalValideRecords": aantal_valide_records,
            "aantalReferentieRecords": element_count,
            "dqDatum": run_time,
            "output": output_text,
        })
    # Create a DataFrame
    df_dq_validatie = pd.DataFrame(extracted_data)
    return df_dq_validatie


def extract_dq_afwijking_data(check_name, dq_result, unique_ids):
    """
    Function takes a json dq_rules and a string check_name and returns a DataFrame.

    :param df_dq_validatie: A DataFrame containing the invalid (deviated) result
    :type df: DataFrame
    :param dq_rules: A JSON string containing the Data Quality rules to be evaluated
    :type dq_rules: str
    :param check_name: Name of the run for reference purposes
    :type check_name: str
    : param unique_ids_cycle: a list containing ids in df
    :type unique_ids: list
    : param unique_ids_cycle: an integer to get unique_ids by one by
    :type unique_ids_cycle: int
    :return: A table df with the invalid result DQ results, parsed from the extract_dq_afwijking_data output
    :rtype: DataFrame
    """
    # Extracting information from the JSON
    run_time = dq_result["meta"]["run_id"].run_time  # Access run_time attribute
    # Extracted data
    extracted_data = []
    # Assign unique_ids sequentially to each row
    unique_ids_cycle = itertools.cycle(unique_ids)

    for result in dq_result["results"]:
        filter_veld_waarde = result["expectation_config"]["kwargs"].get("column")
        expectation_type = result["expectation_config"]["expectation_type"]
        attribute = result["expectation_config"]["kwargs"].get("column")
        dq_regel_id = f"{check_name}_{expectation_type}_{attribute}"
        afwijkende_attribuut_waarde = result.get("result", {}).get("partial_unexpected_list", [])
        for value in afwijkende_attribuut_waarde:
            extracted_data.append({
                "dqRegelId": dq_regel_id,
                "IdentifierVeldWaarde": next(unique_ids_cycle),
                "afwijkendeAttribuutWaarde": value,
                "dqDatum": run_time,
            })
    
    # Create a DataFrame
    df_dq_afwijking = pd.DataFrame(extracted_data)
    return df_dq_afwijking




