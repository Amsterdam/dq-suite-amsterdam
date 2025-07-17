# Useful functionalities
This page contains a collection of various useful functionalities provided by the `dq-suite` library. 

## Table of contents
- [Export the schema from Unity Catalog to the Input Form](#export-the-schema-from-unity-catalog-to-the-input-form)
- [Validate the schema of a table](#validate-the-schema-of-a-table)
- [Sending notifications to Slack](#sending-notifications-to-slack)
- [Sending notifications to MS Teams](#sending-notifications-to-ms-teams)
- [Add severity level to the Input Form](#Add-severity-level-to-the-input-form)

## Export the schema from Unity Catalog to the Input Form
In order to output the schema from Unity Catalog, use the following commands (using the required schema name):
```python
from dq_suite.other import export_schema_to_json_string

schema_output = export_schema_to_json_string(dataset='schema_name', 
                                             spark=spark, 
                                             *table='the_table')
print(schema_output)
```
Copy the string to the [Input Form](https://tamtam.amsterdam.nl/download/8205836-66696c65/7cc5ae47e92afc14e0117fa765890a488b9a5c14/DQ%20Input%20Form%20v0.11.xlsm) to quickly ingest the schema in Excel. The `table` parameter is optional, it gives more granular results.


## Validate the schema of a table
It is possible to validate the schema of an entire table to a schema definition from Amsterdam Schema in one go. This is done by adding two fields to the JSON file containing the data quality rules when describing the table. Click [here](https://github.com/Amsterdam/dq-suite-amsterdam/blob/main/dq_rules_example.json) for an example. You will need:
- `validate_table_schema`: the id field of the table from Amsterdam Schema
- `validate_table_schema_url`: the url of the table or dataset from Amsterdam Schema

The schema definition is converted into column level expectations (`ExpectColumnValuesToBeOfType`) on run time.


## Sending notifications to Slack
The `dq_suite.validation.run_validation` function takes a `slack_webhook` (string-typed) parameter, which is set to `None` by default. Furthermore, the `notify_on` (string-typed) parameter indicates when notifications will be sent: this could be set equal to `"all"`, `"success"` or `"failure"` (default). For more info on Slack webhooks, click [here](https://api.slack.com/messaging/webhooks). 

It is highly recommended to store a webhook in the key vault, and to have (at least) separate channels/webhooks for dev and prd. Eventually, one might want to add a channel/webhook per stakeholder for immediate notification. 

The current implementation uses a `CustomSlackNotificationAction` for sending notifications to Slack. This custom action was built on top of the [existing](https://docs.greatexpectations.io/docs/reference/api/checkpoint/slacknotificationaction_class/) `SlackNotificationAction`, and is an attempt at providing more detailed notifications, to simplify solving data issues. An (as of March '25) [open issue](https://github.com/Amsterdam/dq-suite-amsterdam/issues/95) aims to improve the way Slack messages are rendered/formatted, to further enhance their information content. 


## Sending notifications to MS Teams
The `dq_suite.validation.run_validation` function takes an `ms_teams_webhook` (string-typed) parameter, which is set to `None` by default. Furthermore, the `notify_on` (string-typed) parameter indicates when notifications will be sent: this could be set equal to `"all"`, `"success"` or `"failure"` (default). 
For more info on MS Teams webhooks, click [here](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook?tabs=newteams%2Cdotnet#create-an-incoming-webhook).

*Note*: this notification method is also supported [via Great Expectations](https://docs.greatexpectations.io/docs/reference/api/checkpoint/MicrosoftTeamsNotificationAction_class) by default, but (as of March '25) needs to be significantly improved to be on par with the contents of the Slack notifications. 


## Add severity level to the Input Form
We added a severity level to the `Input Form` to help with prioritizing failed validation rules based on their criticality.

The severity level is determined using the function:

`get_highest_severity_from_validation_result`
(imported `from src.dq_suite.output_transformations`)

This function extracts the highest severity level from the validation result by checking which failed rules have the most critical severity assigned.

Severity levels (from highest to lowest):

`fatal`: Fails the workflow. Workflow can't continue with this error.

`error`: Does not fail the workflow, but should be fixed A.S.A.P.

`warning`: Does not fail the workflow, but should be fixed eventually.

If no failed expectations match any defined severity, the function returns None.

This function is used within `dq_suite.validation.run_validation`, and its output is included in the final validation result for easier downstream processing and reporting.