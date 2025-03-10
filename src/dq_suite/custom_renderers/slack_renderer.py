from great_expectations.checkpoint import SlackNotificationAction
from great_expectations.checkpoint.actions import ActionContext
from great_expectations.checkpoint.checkpoint import CheckpointResult

from typing import Literal

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.resource_identifiers import \
    ValidationResultIdentifier


# https://medium.com/@jojo-data/how-to-create-a-custom-module-in-great-expectations-efd0a6ed704a


class CustomSlackNotificationAction(SlackNotificationAction):
    @staticmethod
    def _should_notify(success: bool,
                       notify_on: Literal["all", "failure", "success"]) -> bool:
        return (
                notify_on == "all"
                or notify_on == "success"
                and success
                or notify_on == "failure"
                and not success
        )

    def _get_validation_text_blocks(self, validation_result_identifier:
    ValidationResultIdentifier, suite_validation_result:
    ExpectationSuiteValidationResult, action_context: ActionContext) -> list[dict]:

        validation_text_blocks = self._render_validation_result(
            result_identifier=validation_result_identifier,
            result=suite_validation_result,
            action_context=action_context,
        )

        # Add sample of unexpected values
        summary_text = validation_text_blocks[0]["text"]["text"]
        summary_text += "\n-----------------------\n"
        if not suite_validation_result.success:
            for result in suite_validation_result.results:
                if not result.success:
                    expectation_info = result["expectation_config"]["meta"]
                    results = result.result
                    summary_text += (
                        f"""
                        \n *Column*: {expectation_info['column_name']}\n 
*Expectation*: {expectation_info['expectation_name']}\n
*Sample unexpected values*:  {results['partial_unexpected_list'][:3]}\n
*Unexpected / total count*: {results['unexpected_count']} / {results['element_count']}\n
"-----------------------\n"
                        """
                    )

        validation_text_blocks[0]["text"]["text"] = summary_text

        return validation_text_blocks

    def run(
        self,
        checkpoint_result: CheckpointResult,
        action_context: ActionContext | None = None,
    ) -> dict:
        success = checkpoint_result.success or False
        result = {"slack_notification_result": "none required"}

        if not self._should_notify(success=success, notify_on=self.notify_on):
            return result

        checkpoint_text_blocks: list[dict] = []
        for (
            validation_result_identifier,
            suite_validation_result,
        ) in checkpoint_result.run_results.items():
            validation_text_blocks = self._get_validation_text_blocks(
                validation_result_identifier=validation_result_identifier,
                suite_validation_result=suite_validation_result,
                action_context=action_context
            )
            checkpoint_text_blocks.extend(validation_text_blocks)

        payload = self.renderer.concatenate_text_blocks(
            action_name=self.name,
            text_blocks=checkpoint_text_blocks,
            success=success,
            checkpoint_name=checkpoint_result.checkpoint_config.name,
            run_id=checkpoint_result.run_id,
        )

        return self._send_slack_notification(payload=payload)
