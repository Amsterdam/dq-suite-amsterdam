# Export the schema from Unity Catalog to the Input Form
In order to output the schema from Unity Catalog, use the following commands (using the required schema name):

```
schema_output = dq_suite.schema_to_json_string('schema_name', spark)
print(schema_output)
```

Copy the string to the Input Form to quickly ingest the schema in Excel.


# Validate the schema of a table
It is possible to validate the schema of an entire table to a schema definition from Amsterdam Schema in one go. This is done by adding two fields to the "dq_rules" JSON when describing the table (See: https://github.com/Amsterdam/dq-suite-amsterdam/blob/main/dq_rules_example.json). 

You will need:
- validate_table_schema: the id field of the table from Amsterdam Schema
- validate_table_schema_url: the url of the table or dataset from Amsterdam Schema

The schema definition is converted into column level expectations (expect_column_values_to_be_of_type) on run time.