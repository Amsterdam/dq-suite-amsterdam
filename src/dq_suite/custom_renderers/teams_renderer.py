from typing import TYPE_CHECKING

from great_expectations.core import RunIdentifier
from great_expectations.render.renderer.microsoft_teams_renderer import (
    MicrosoftTeamsRenderer,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        ValidationResultIdentifier,
    )


class CustomMSTeamsRenderer(MicrosoftTeamsRenderer):
    """Custom Teams renderer that sends an Adaptive Card payload."""

    def render(
        self,
        checkpoint_result: "CheckpointResult",
        data_docs_pages: dict["ValidationResultIdentifier", dict[str, str]] | None = None,
    ) -> dict:
        checkpoint_blocks: list[list[dict[str, str]]] = []
        for result_identifier, result in checkpoint_result.run_results.items():
            validation_blocks = self._render_validation_result(
                validation_result=result,
                validation_result_suite_identifier=result_identifier,
            )
            checkpoint_blocks.append(validation_blocks)

        data_docs_block = self._render_data_docs_links(data_docs_pages=data_docs_pages)
        return self._build_payload(
            checkpoint_result=checkpoint_result,
            checkpoint_blocks=checkpoint_blocks,
            data_docs_block=data_docs_block,
        )

    def _render_validation_result(
        self,
        validation_result: "ExpectationSuiteValidationResult",
        validation_result_suite_identifier: "ValidationResultIdentifier",
    ) -> list[dict[str, str]]:
        return [
            self._render_status(validation_result=validation_result),
            self._render_asset_name(validation_result=validation_result),
            self._render_suite_name(validation_result=validation_result),
            self._render_run_name(
                validation_result_suite_identifier=validation_result_suite_identifier
            ),
            self._render_batch_id(validation_result=validation_result),
            self._render_summary(validation_result=validation_result),
        ]

    def _build_payload(
        self,
        checkpoint_result: "CheckpointResult",
        checkpoint_blocks: list[list[dict[str, str]]],
        data_docs_block: list[dict[str, str]] | None,
    ) -> dict:
        checkpoint_name = checkpoint_result.checkpoint_config.name
        status = "Success !!!" if checkpoint_result.success else "Failure :("

        title_block = {
            "type": "TextBlock",
            "size": "Large",
            "weight": "Bolder",
            "text": f"Checkpoint Result: {checkpoint_name} ({status})",
            "wrap": True,
        }

        body: list[dict] = [
            title_block,
            {"type": "TextBlock", "text": "", "separator": True},
        ]

        for result_block in checkpoint_blocks:
            for block in result_block:
                body.append(block)
            body.append({"type": "TextBlock", "text": "", "separator": True})

        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": self.MICROSOFT_TEAMS_SCHEMA_URL,
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": body,
                        "actions": data_docs_block or [],
                    },
                }
            ],
        }

    def _render_run_name(
        self, validation_result_suite_identifier: "ValidationResultIdentifier"
    ) -> dict[str, str]:
        run_id = validation_result_suite_identifier.run_id
        run_name = run_id.run_name if isinstance(run_id, RunIdentifier) else run_id
        return self._render_validation_result_element(key="Run Name", value=run_name)