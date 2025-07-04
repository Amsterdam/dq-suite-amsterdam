from typing import Literal

from great_expectations.checkpoint import SlackNotificationAction
from great_expectations.checkpoint.actions import ActionContext
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.core import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)


class CustomSlackRenderer:
    # TODO: implement. Should at least allow for a custom header
    pass


class CustomSlackNotificationAction(SlackNotificationAction):
    @staticmethod
    def _should_notify(
        success: bool, notify_on: Literal["all", "failure", "success"]
    ) -> bool:
        return (
            notify_on == "all"
            or notify_on == "success"
            and success
            or notify_on == "failure"
            and not success
        )

    @staticmethod
    def _get_expectation_parameters_dict(
        result: ExpectationValidationResult,
    ) -> dict:
        expectation_parameters_dict = dict()
        for k, v in result["expectation_config"]["kwargs"].items():
            if k not in ["batch_id", "column"]:
                expectation_parameters_dict[k] = v
        return expectation_parameters_dict

    def _create_text_block_for_suite_validation_result(
        self, result: ExpectationValidationResult
    ) -> str:
        expectation_metadata = result["expectation_config"]["meta"]
        expectation_name = expectation_metadata["expectation_name"]
        results = result.result

        # Output for Set-type expectations is differently structured
        # TODO: refactor this output more neatly into a function
        if expectation_name == "ExpectTableColumnsToMatchSet":
            column_set = result["expectation_config"]["kwargs"]["column_set"]
            unexpected_values = results["details"]["mismatched"].get(
                "unexpected", None
            )  # Could be an empty collection/absent
            missing_values = results["details"]["mismatched"].get(
                "missing", None
            )  # Could be an empty collection/absent
            return f"""
    \n *Expectation*: `{expectation_name}`\n\n
    :information_source: Details:
    *Unexpected columns*:  ```{unexpected_values}```\n
    *Missing columns*:  ```{missing_values}```\n
    *Expected columns*: ```{column_set}```\n
    -----------------------\n
                """
        else:
            parameters = self._get_expectation_parameters_dict(result=result)
            partial_unexpected_list = results.get(
                "partial_unexpected_list", None
            )
            if partial_unexpected_list is not None:
                partial_unexpected_list = partial_unexpected_list[:3]
            return f"""
    \n *Column*: `{expectation_metadata['column_name']}`    *Expectation*: `{expectation_name}`\n\n
    :information_source: Details:
    *Sample unexpected values*:  ```{partial_unexpected_list}```\n
    *Unexpected / total count*: {results.get('unexpected_count', None)} / {results.get('element_count', 0)}\n
    *Expectation parameters*: ```{parameters}```\n
    -----------------------\n
                """

    def _get_validation_text_blocks(
        self,
        validation_result_identifier: ValidationResultIdentifier,
        suite_validation_result: ExpectationSuiteValidationResult,
        action_context: ActionContext,
    ) -> list[dict]:
        validation_text_blocks = self._render_validation_result(
            result_identifier=validation_result_identifier,
            result=suite_validation_result,
            action_context=action_context,
        )  # TODO: implement custom SlackRenderer-class (above)

        result_text_block = validation_text_blocks[0]["text"]["text"]
        result_text_block += "\n-----------------------"

        # Add sample of unexpected values + metadata to block
        if not suite_validation_result.success:  # Overall failure
            for result in suite_validation_result.results:
                if not result.success:  # Failure per expectation
                    result_text_block += (
                        self._create_text_block_for_suite_validation_result(
                            result=result
                        )
                    )

        validation_text_blocks[0]["text"]["text"] = result_text_block

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
                action_context=action_context,
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