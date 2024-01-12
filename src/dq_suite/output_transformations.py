import pandas as pd
from pyspark.sql.functions import col

# for df_dqValidatie
def extract_dq_validatie_data(check_name, dq_result):
    """
    Function takes a json with the GX output and a string check_name and returns a dataframe.
    :param dq_result: A dictionary containing the Data Quality results from GX
    :type dq_result: dict
    :param check_name: Name of the run for reference purposes
    :type check_name: str
    :return: A dataframe with the DQ results per rule
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
    Function takes a json with the GX output and a string check_name and returns dataframe.
    
    :param dq_result: A dictionary containing the Data Quality results from GX
    :type dq_result: dict
    :param check_name: Name of the run for reference purposes
    :type check_name: str
    :param df: A dataframe with the actual data
    :type: df
    :param unique_identifier: The column name of the id values of the actual data
    :type: str
    :return: A dataframe with all unexpected values and their id
    :rtype: df.
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
        dq_regel_id = f"{check_name}_{expectation_type}_{attribute}"
        afwijkende_attribuut_waarde = result["result"].get("partial_unexpected_list", [])
        for value in afwijkende_attribuut_waarde:
            if value == None: 
                filtered_df = df.filter(col(attribute).isNull())
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

def create_brontabel(dq_rules):
    """
    Function takes the table name and their unique identifier from the provided Data Quality rules 
    to create a DataFrame containing this metadata.

    :param name: str comes from dq_rules
    :type name: str
    :param unique_identifier: int comes from dq_rules
    :type unique_identifier: int
    :rtype: DataFrame
    :return: df_brontable
    :rtype: DataFrame
    """
    extracted_data = []
    for param in dq_rules["dataframe_parameters"]:
        name = param["table_name"]  
        unique_identifier = param["unique_identifier"]
        extracted_data.append({
            "name": name,
            "unique_identifier": unique_identifier
        })
    
    df_brontable = pd.DataFrame(extracted_data)
    return df_brontable

def create_bronattribute(dq_rules):
    """
    This function takes attributes/columns for each table specified in the Data Quality rules and creates a DataFrame containing these attribute details.

    :param BronTabel: str comes from dq_rules
    :type BronTabel: str
    :param attribute_name: str comes from dq_rules
    :type attribute_name: str
    :param unique_identifier: int comes from dq_rules
    :return: df_bronattribuut
    :rtype: DataFrame
    """
    extracted_data = []
    for param in dq_rules["dataframe_parameters"]:
        BronTabel = param["table_name"] 
        for rule in param["rules"]:
            parameters = rule.get("parameters", [])
            for parameter in parameters:
                if isinstance(parameter, dict) and "column" in parameter:
                    attribute_name = parameter["column"]
                    extracted_data.append({
                        "name": attribute_name,
                        "BronTabel": BronTabel,
                        "id": f"{BronTabel}_{attribute_name}"
                    })
    
    df_bronattribuut = pd.DataFrame(extracted_data)
    return df_bronattribuut

def create_dqRegel(dq_rules):
    """
    Function extracts information about Data Quality rules applied to each attribute/column for tables specified in the Data Quality rules and creates a DataFrame containing these rule details.

    :param BronTabel: str comes from dq_rules
    :type BronTabel: str
    :param rule_name: str comes from dq_rules
    :type rule_name: str
    :param attribute_name: str comes from dq_rules
    :type attribute_name: str
    :return: df_dqRegel
    :rtype: DataFrame
    """
    extracted_data = []
    for param in dq_rules["dataframe_parameters"]:
        BronTabel = param["table_name"] 
        for rule in param["rules"]:
            rule_name = rule["rule_name"]
            parameters = rule.get("parameters", [])
            for parameter in parameters:
                if isinstance(parameter, dict) and "column" in parameter:
                    attribute_name = parameter["column"]
                    extracted_data.append({
                        "id": f"{BronTabel}_{rule_name}_{attribute_name}",
                        "bronAttibuteId": f"{BronTabel}_{attribute_name}"   
                    })
    
    df_dqRegel = pd.DataFrame(extracted_data)
    return df_dqRegel
