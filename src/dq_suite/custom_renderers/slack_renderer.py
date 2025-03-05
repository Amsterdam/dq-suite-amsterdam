from typing import Literal, override

from great_expectations.checkpoint.actions import ActionContext, _should_notify
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.render.renderer.slack_renderer import SlackRenderer
# https://medium.com/@jojo-data/how-to-create-a-custom-module-in-great-expectations-efd0a6ed704a

from great_expectations.compatibility.pydantic import Field


from great_expectations.checkpoint import SlackNotificationAction


class CustomSlackRenderer(SlackRenderer):
    def _build_description_block(
            self,
            validation_result: ExpectationSuiteValidationResult,
            validation_result_urls: list[str],
    ) -> dict:
        status = "Failed :x:"
        if validation_result.success:
            status = "Success :tada:"

        validation_link = None
        summary_text = ""
        if validation_result_urls:
            if len(validation_result_urls) == 1:
                validation_link = validation_result_urls[0]
            else:
                title_hlink = "*Validation Results*"
                batch_validation_status_hlinks = "".join(
                    f"*<{validation_result_url} | {status}>*"
                    for validation_result_url in validation_result_urls
                )
                summary_text += f"""{title_hlink}
    {batch_validation_status_hlinks}
                """

        expectation_suite_name = validation_result.suite_name
        data_asset_name = validation_result.asset_name or "__no_data_asset_name__"
        summary_text += f"*Asset*: {data_asset_name}\\n"
        # Slack does not allow links to local files due to security risks
        # DataDocs links will be added in a block after this summary text when applicable
        if validation_link and "file://" not in validation_link:
            summary_text += (
                f"*Expectation Suite*: {expectation_suite_name}  <{validation_link}|View Results>"
            )
        else:
            summary_text += f"*Expectation Suite*: {expectation_suite_name}\\n"

        # Add sample of unexpected values
        if not validation_result.success:
            for result in validation_result.results:
                if not result.success:
                    expectation_info = result.expectation_config.meta
                    summary_text += (f"*Table*:"
                                     f" {expectation_info['table_name']} / "
                                     f"*Column*:"
                                     f" {expectation_info['column_name']} /  "
                                     f"*Expectation*:"
                                     f" {expectation_info['expectation_name']}\\n")

                    results = result.result
                    summary_text += (f"*Sample unexpected values*: "
                                     f"{results['partial_unexpected_list'][:3]}"
                                     f" / *Unexpected percentage*: "
                                     f"{results['unexpected_percent_total']}"
                                     )

        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": summary_text,
            },
        }


class CustomSlackNotificationAction(SlackNotificationAction):
    type: Literal["custom_slack"] = "custom_slack"
    renderer: CustomSlackRenderer = Field(default_factory=CustomSlackRenderer)

    @override
    def run(
            self, checkpoint_result: CheckpointResult,
            action_context: ActionContext | None = None
    ) -> dict:
        success = checkpoint_result.success or False
        checkpoint_name = checkpoint_result.checkpoint_config.name
        result = {"slack_notification_result": "none required"}

        if not _should_notify(success=success, notify_on=self.notify_on):
            return result

        checkpoint_text_blocks: list[dict] = []
        for (
                validation_result_suite_identifier,
                validation_result_suite,
        ) in checkpoint_result.run_results.items():
            validation_text_blocks = self._render_validation_result(
                result_identifier=validation_result_suite_identifier,
                result=validation_result_suite,
                action_context=action_context,
            )
            checkpoint_text_blocks.extend(validation_text_blocks)

        payload = self.renderer.concatenate_text_blocks(
            action_name=self.name,
            text_blocks=checkpoint_text_blocks,
            success=success,
            checkpoint_name=checkpoint_name,
            run_id=checkpoint_result.run_id,
        )

        return self._send_slack_notification(payload=payload)
