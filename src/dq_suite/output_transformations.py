import pandas as pd
from pyspark.sql.functions import col

# for df_dqValidatie
def extract_dq_validatie_data(check_name, dq_result):
    """
    Function takes a json with the GX output and a string check_name and returns dataframe.
    
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

def extract_dq_afwijking_data(check_name, dq_result, df, unique_identifier):
    """
    Function takes a json dq_rules and a string check_name and returns a DataFrame.

    :param df_dq_validatie: A DataFrame containing the invalid (deviated) result
    :type df: DataFrame
    :param check_name: Name of the run for reference purposes
    :type check_name: str
    : param unique_identifier: int comes from dq_rules
    :type unique_identifier: int
    :rtype: DataFrame
    """
    
    # Extracting information from the JSON
    run_time = dq_result["meta"]["run_id"].run_time  # Access run_time attribute
    # Extracted data for df
    extracted_data = []
    #for IdentifierVeldWaarde
    unique_identifier = unique_identifier
    # To store unique combinations of value and IDs
    unique_entries = set()  

    for result in dq_result["results"]:
        expectation_type = result["expectation_config"]["expectation_type"]
        attribute = result["expectation_config"]["kwargs"].get("column")
        dq_regel_id = f"{check_name}_{expectation_type}_{attribute}"
        afwijkende_attribuut_waarde = result["result"].get("partial_unexpected_list", [])
        for value in afwijkende_attribuut_waarde:
            if value == None: 
                filtered_df = df.filter(col(attribute).isNull())
                ids = filtered_df.select(unique_identifier).rdd.flatMap(lambda x: x).collect()
            else:
                    filtered_df = df.filter(col(attribute)==value)
                    ids = filtered_df.select(unique_identifier).rdd.flatMap(lambda x: x).collect()

            for id_value in ids:
                entry = (id_value)
                if entry not in unique_entries:  # Check for uniqueness before appending
                    unique_entries.add(entry)
                    extracted_data.append({
                                "dqRegelId": dq_regel_id,
                                "IdentifierVeldWaarde": id_value,
                                "afwijkendeAttribuutWaarde": value,
                                "dqDatum": run_time,
                            })
                    
    # Create a DataFrame
    df_dq_afwijking = pd.DataFrame(extracted_data)
    return df_dq_afwijking