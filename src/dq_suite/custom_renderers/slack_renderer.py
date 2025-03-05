from typing import Literal

from great_expectations.checkpoint.actions import ActionContext, _should_notify
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.render.renderer.slack_renderer import SlackRenderer
# https://medium.com/@jojo-data/how-to-create-a-custom-module-in-great-expectations-efd0a6ed704a

from great_expectations.compatibility.pydantic import Field


from great_expectations.checkpoint import SlackNotificationAction


class CustomSlackNotificationAction(SlackNotificationAction):
    # @override
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

            # Add sample of unexpected values
            summary_text = validation_text_blocks[0]["text"]["text"]
            summary_text += "\n-----------------------\n"
            if not validation_result_suite.success:
                for result in validation_result_suite.results:
                    if not result.success:
                        expectation_info = result['expectation_config']['meta']
                        summary_text += (f"\n *Table*:"
                                         f" {expectation_info['table_name']} / "
                                         f"*Column*:"
                                         f" {expectation_info['column_name']} /  "
                                         f"*Expectation*:"
                                         f" {expectation_info['expectation_name']}\n")

                        results = result.result
                        summary_text += (f"\n *Sample unexpected values*: "
                                         f"{results['partial_unexpected_list'][:3]}"
                                         f" / *Unexpected percentage*: "
                                         f""
                                         f""
                                         f"{results['unexpected_percent_total']}\n"
                                         )

                        summary_text += "-----------------------\n"

            validation_text_blocks[0]["text"]["text"] = summary_text

            checkpoint_text_blocks.extend(validation_text_blocks)

        payload = self.renderer.concatenate_text_blocks(
            action_name=self.name,
            text_blocks=checkpoint_text_blocks,
            success=success,
            checkpoint_name=checkpoint_name,
            run_id=checkpoint_result.run_id,
        )

        return self._send_slack_notification(payload=payload)
