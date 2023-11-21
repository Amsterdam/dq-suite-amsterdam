import pandas as pd


def extract_dq_validatie_data(check_name, dq_result):
    """Function takes a json with the GX output and a string check_name and returns dataframe.
    
    :param df_dq_validatie: A df containing the valid result
    :type df: DataFrame
    :param dq_rules: A JSON string containing the Data Quality rules to be evaluated
    :type dq_rules: str
    :param check_name: Name of the run for reference purposes
    :type check_name: str
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
        extracted_data.append({
            "dqRegelId": dq_regel_id,
            "aantalValideRecords": aantal_valide_records,
            "aantalReferentieRecords": element_count,
            "dqDatum": run_time,
        })
    # Create a DataFrame
    df_dq_validatie = pd.DataFrame(extracted_data)
    return df_dq_validatie


def extract_dq_afwijking_data(check_name, dq_result):
    """
    Function takes a json dq_rules,and a string check_name and returns dataframe.
    
    :param df_dq_validatie: A df containing the invalid(deviated) result
    :type df: DataFrame
    :param dq_rules: A JSON string containing the Data Quality rules to be evaluated
    :type dq_rules: str
    :param check_name: Name of the run for reference purposes
    :type check_name: str
    :return: A table df with the invalid result DQ results, parsed from the extract_dq_afwijking_data output
    :rtype: df.
    """
    # Extracting information from the JSON
    run_time = dq_result["meta"]["run_id"].run_time  # Access run_time attribute
    # Extracted data
    extracted_data = []
    for result in dq_result["results"]:
        filter_veld_waarde = result["expectation_config"]["kwargs"].get("column")
        expectation_type = result["expectation_config"]["expectation_type"]
        attribute = result["expectation_config"]["kwargs"].get("column")
        dq_regel_id = f"{check_name}_{expectation_type}_{attribute}"
        afwijkende_attribuut_waarde = result.get("result", {}).get("partial_unexpected_list", [])
        for value in afwijkende_attribuut_waarde:
            extracted_data.append({
                "dqRegelId": dq_regel_id,
                "afwijkendeAttribuutWaarde": value,
                "dqDatum": run_time,
            })
    # Create a DataFrame
    df_dq_afwijking = pd.DataFrame(extracted_data)
    return df_dq_afwijking
