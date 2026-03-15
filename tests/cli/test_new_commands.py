"""Tests for new CLI commands: builtins, schema infer, export --format retention."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import click
from click.testing import CliRunner

from cli import cli_main


# ===========================================================================
#  Fixtures
# ===========================================================================


@pytest.fixture
def runner():
    return CliRunner(mix_stderr=False)


# ===========================================================================
#  builtins list
# ===========================================================================


class TestBuiltinsList:
    """Tests for `ledger builtins list`."""

    def test_builtins_list_text_format(self, runner):
        result = runner.invoke(cli_main, ["builtins", "list"])
        assert result.exit_code == 0
        output = result.output
        # Should list known builtins
        assert "immutable" in output
        assert "gdpr_erasable" in output
        assert "audit_field" in output
        assert "encrypted_at_rest" in output
        assert "pii_field" in output
        assert "primary_key" in output

    def test_builtins_list_json_format(self, runner):
        result = runner.invoke(cli_main, ["--format", "json", "builtins", "list"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, dict)
        assert "immutable" in data
        assert "pact_assertion_type" in data["immutable"]
        assert "arbiter_tier_behavior" in data["immutable"]
        assert "baton_masking_rule" in data["immutable"]
        assert "sentinel_severity" in data["immutable"]

    def test_builtins_list_yaml_format(self, runner):
        result = runner.invoke(cli_main, ["--format", "yaml", "builtins", "list"])
        assert result.exit_code == 0
        assert "immutable:" in result.output
        assert "pact_assertion_type:" in result.output

    def test_builtins_list_shows_propagation_rules(self, runner):
        result = runner.invoke(cli_main, ["--format", "json", "builtins", "list"])
        data = json.loads(result.output)
        # Verify specific propagation rules
        assert data["immutable"]["pact_assertion_type"] == "field_present"
        assert data["immutable"]["sentinel_severity"] == "critical"
        assert data["gdpr_erasable"]["baton_masking_rule"] == "full_mask"


# ===========================================================================
#  builtins show
# ===========================================================================


class TestBuiltinsShow:
    """Tests for `ledger builtins show <name>`."""

    def test_show_known_annotation(self, runner):
        result = runner.invoke(cli_main, ["builtins", "show", "immutable"])
        assert result.exit_code == 0
        assert "immutable" in result.output
        assert "field_present" in result.output

    def test_show_json_format(self, runner):
        result = runner.invoke(cli_main, ["--format", "json", "builtins", "show", "gdpr_erasable"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["annotation_name"] == "gdpr_erasable"
        assert data["pact_assertion_type"] == "field_present"
        assert data["arbiter_tier_behavior"] == "enforce_tier"
        assert data["baton_masking_rule"] == "full_mask"
        assert data["sentinel_severity"] == "high"

    def test_show_yaml_format(self, runner):
        result = runner.invoke(cli_main, ["--format", "yaml", "builtins", "show", "audit_field"])
        assert result.exit_code == 0
        assert "annotation_name: audit_field" in result.output

    def test_show_unknown_annotation_errors(self, runner):
        result = runner.invoke(cli_main, ["builtins", "show", "nonexistent_annotation"])
        assert result.exit_code != 0
        assert "Unknown annotation" in result.output or "nonexistent_annotation" in result.output

    def test_show_all_builtin_annotations(self, runner):
        """Each builtin should be individually showable."""
        builtins = [
            "immutable", "gdpr_erasable", "audit_field", "soft_delete_marker",
            "encrypted_at_rest", "not_null", "pii_field", "primary_key",
        ]
        for name in builtins:
            result = runner.invoke(cli_main, ["--format", "json", "builtins", "show", name])
            assert result.exit_code == 0, f"Failed for {name}: {result.output}"
            data = json.loads(result.output)
            assert data["annotation_name"] == name


# ===========================================================================
#  builtins stripe
# ===========================================================================


class TestBuiltinsStripe:
    """Tests for `ledger builtins stripe`."""

    def test_stripe_text_format(self, runner):
        result = runner.invoke(cli_main, ["builtins", "stripe"])
        assert result.exit_code == 0
        assert "stripe_card_number" in result.output
        assert "stripe_customer_email" in result.output
        assert "encrypted_at_rest" in result.output
        assert "FINANCIAL" in result.output
        assert "PII" in result.output

    def test_stripe_json_format(self, runner):
        result = runner.invoke(cli_main, ["--format", "json", "builtins", "stripe"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, dict)
        assert "stripe_card_number" in data
        assert "stripe_customer_email" in data
        assert data["stripe_card_number"]["classification"] == "FINANCIAL"
        assert "encrypted_at_rest" in data["stripe_card_number"]["annotations"]
        assert "tokenized" in data["stripe_card_number"]["annotations"]
        assert data["stripe_customer_email"]["classification"] == "PII"
        assert "pii_field" in data["stripe_customer_email"]["annotations"]
        assert "gdpr_erasable" in data["stripe_customer_email"]["annotations"]

    def test_stripe_yaml_format(self, runner):
        result = runner.invoke(cli_main, ["--format", "yaml", "builtins", "stripe"])
        assert result.exit_code == 0
        assert "stripe_card_number:" in result.output
        assert "stripe_customer_email:" in result.output

    def test_stripe_card_fields(self, runner):
        result = runner.invoke(cli_main, ["--format", "json", "builtins", "stripe"])
        data = json.loads(result.output)
        # Card number
        assert data["stripe_card_number"]["field_pattern"] == "*.card.number"
        assert data["stripe_card_number"]["propagation"]["sentinel_severity"] == "critical"
        # Card CVC
        assert data["stripe_card_cvc"]["field_pattern"] == "*.card.cvc"
        assert data["stripe_card_cvc"]["propagation"]["baton_masking_rule"] == "full_mask"

    def test_stripe_customer_fields(self, runner):
        result = runner.invoke(cli_main, ["--format", "json", "builtins", "stripe"])
        data = json.loads(result.output)
        # Customer name
        assert data["stripe_customer_name"]["classification"] == "PII"
        assert "gdpr_erasable" in data["stripe_customer_name"]["annotations"]
        # Customer phone
        assert data["stripe_customer_phone"]["classification"] == "PII"
        # Customer address
        assert data["stripe_customer_address"]["field_pattern"] == "*.customer.address.*"


# ===========================================================================
#  schema infer
# ===========================================================================


class TestSchemaInfer:
    """Tests for `ledger schema infer <backend_id>`."""

    def test_infer_requires_config(self, runner, tmp_path):
        """Should error when no config exists."""
        result = runner.invoke(cli_main, [
            "--config", str(tmp_path / "nonexistent.yaml"),
            "schema", "infer", "my_db",
        ])
        assert result.exit_code != 0

    def test_infer_backend_not_found(self, runner, tmp_path):
        """Should error when backend is not in config."""
        import yaml
        config_path = tmp_path / "ledger.yaml"
        config_path.write_text(yaml.dump({
            "project_name": "test",
            "schemas_dir": str(tmp_path / "schemas"),
            "changelog_path": str(tmp_path / "changelog"),
            "plans_dir": str(tmp_path / "plans"),
            "backends": [],
        }))
        result = runner.invoke(cli_main, [
            "--config", str(config_path),
            "schema", "infer", "nonexistent_backend",
        ])
        assert result.exit_code != 0

    @patch("cli.inference.infer_schema")
    @patch("cli.inference.schema_to_yaml")
    @patch("cli.config.load_config")
    def test_infer_outputs_yaml_to_stdout(self, mock_load, mock_to_yaml, mock_infer, runner):
        """Should output YAML draft to stdout."""
        mock_config = MagicMock()
        mock_backend = MagicMock()
        mock_backend.name = "test_db"
        mock_backend.backend_type = "postgres"
        mock_backend.base_url = "postgresql://localhost/test"
        mock_config.backends = [mock_backend]
        mock_load.return_value = mock_config

        mock_schema = MagicMock()
        mock_infer.return_value = mock_schema
        mock_to_yaml.return_value = "backend_id: test_db\ntables: []\n"

        result = runner.invoke(cli_main, ["schema", "infer", "test_db"])
        assert result.exit_code == 0
        assert "backend_id: test_db" in result.output

    @patch("cli.inference.infer_schema")
    @patch("cli.inference.schema_to_yaml")
    @patch("cli.config.load_config")
    def test_infer_writes_to_file(self, mock_load, mock_to_yaml, mock_infer, runner, tmp_path):
        """Should write output to file when --output is specified."""
        mock_config = MagicMock()
        mock_backend = MagicMock()
        mock_backend.name = "test_db"
        mock_backend.backend_type = "postgres"
        mock_backend.base_url = "postgresql://localhost/test"
        mock_config.backends = [mock_backend]
        mock_load.return_value = mock_config

        mock_schema = MagicMock()
        mock_infer.return_value = mock_schema
        mock_to_yaml.return_value = "backend_id: test_db\ntables: []\n"

        output_path = str(tmp_path / "draft.yaml")
        result = runner.invoke(cli_main, [
            "schema", "infer", "test_db", "--output", output_path,
        ])
        assert result.exit_code == 0
        assert os.path.isfile(output_path)
        with open(output_path) as f:
            content = f.read()
        assert "backend_id: test_db" in content

    @patch("cli.inference.infer_schema")
    @patch("cli.inference.schema_to_yaml")
    @patch("cli.config.load_config")
    def test_infer_passes_confidence_flag(self, mock_load, mock_to_yaml, mock_infer, runner):
        """Should pass --confidence through to inference."""
        mock_config = MagicMock()
        mock_backend = MagicMock()
        mock_backend.name = "test_db"
        mock_backend.backend_type = "postgres"
        mock_backend.base_url = "postgresql://localhost/test"
        mock_config.backends = [mock_backend]
        mock_load.return_value = mock_config

        mock_schema = MagicMock()
        mock_infer.return_value = mock_schema
        mock_to_yaml.return_value = "_confidence: draft\n"

        result = runner.invoke(cli_main, [
            "schema", "infer", "test_db", "--confidence",
        ])
        assert result.exit_code == 0
        # Verify confidence flag was passed to schema_to_yaml
        mock_to_yaml.assert_called_once_with(mock_schema, show_confidence=True)

    @patch("cli.config.load_config")
    def test_infer_handles_missing_dependency(self, mock_load, runner):
        """Should handle MissingDependencyError gracefully."""
        import inference

        mock_config = MagicMock()
        mock_backend = MagicMock()
        mock_backend.name = "test_db"
        mock_backend.backend_type = "postgres"
        mock_backend.base_url = "postgresql://localhost/test"
        mock_config.backends = [mock_backend]
        mock_load.return_value = mock_config

        with patch("cli.inference.infer_schema",
                   side_effect=inference.MissingDependencyError("psycopg2-binary", "postgres")):
            result = runner.invoke(cli_main, ["schema", "infer", "test_db"])
            assert result.exit_code != 0
            # The error message should mention the missing package
            assert "psycopg2" in (result.output + (result.stderr if hasattr(result, 'stderr') else ""))


# ===========================================================================
#  export --format retention
# ===========================================================================


class TestExportRetention:
    """Tests for `ledger export --format retention`."""

    @patch("cli.export.export_retention_from_config")
    @patch("cli.config.load_config")
    def test_retention_export_calls_correct_function(self, mock_load, mock_export, runner):
        """Should call export_retention_from_config, not export_contracts."""
        mock_config = MagicMock()
        mock_load.return_value = mock_config
        mock_export.return_value = {"retention_rules": []}

        result = runner.invoke(cli_main, ["export", "--format", "retention"])
        assert result.exit_code == 0
        mock_export.assert_called_once()

    @patch("cli.export.export_retention_from_config")
    @patch("cli.config.load_config")
    def test_retention_export_with_component_filter(self, mock_load, mock_export, runner):
        """Should pass component filter to retention export."""
        mock_config = MagicMock()
        mock_load.return_value = mock_config
        mock_export.return_value = {"retention_rules": []}

        result = runner.invoke(cli_main, [
            "export", "--format", "retention", "--component", "user_service",
        ])
        assert result.exit_code == 0
        mock_export.assert_called_once_with(mock_config, "user_service")

    @patch("cli.export.export_retention_from_config")
    @patch("cli.config.load_config")
    def test_retention_export_json_output(self, mock_load, mock_export, runner):
        """Should output valid JSON when --format json is combined."""
        mock_config = MagicMock()
        mock_load.return_value = mock_config
        mock_export.return_value = {
            "retention_rules": [
                {
                    "backend_id": "db1",
                    "table_name": "users",
                    "field_rules": [
                        {
                            "field_name": "email",
                            "annotation": "gdpr_erasable",
                            "retention_days": 90,
                        },
                    ],
                }
            ]
        }

        result = runner.invoke(cli_main, [
            "--format", "json",
            "export", "--format", "retention",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "retention_rules" in data

    def test_retention_is_valid_export_format_choice(self, runner):
        """The 'retention' option should be accepted by the --format choice."""
        result = runner.invoke(cli_main, ["export", "--format", "retention", "--help"])
        # Just verifying it doesn't fail with "invalid choice"
        # (--help always exits 0)
        assert result.exit_code == 0

    @patch("cli.export.export_contracts")
    @patch("cli.config.load_config")
    def test_pact_format_still_uses_export_contracts(self, mock_load, mock_export, runner):
        """Non-retention formats should still use the original export_contracts path."""
        mock_config = MagicMock()
        mock_load.return_value = mock_config
        mock_export.return_value = {"assertions": []}

        result = runner.invoke(cli_main, ["export", "--format", "pact"])
        assert result.exit_code == 0
        mock_export.assert_called_once()
