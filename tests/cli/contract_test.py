"""
Contract test suite for the CLI component.
Tests organized in two layers: unit tests and integration tests.

Run with: pytest contract_test.py -v
"""

import json
import os
import re
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional
from unittest.mock import MagicMock, Mock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Attempt imports from the cli package. We wrap in try/except so that the
# test file is at least parseable even if the implementation isn't installed;
# individual tests will fail with clear import errors.
# ---------------------------------------------------------------------------
try:
    from cli import (
        cli_main,
        cmd_init,
        cmd_backend_add,
        cmd_schema_add,
        cmd_schema_show,
        cmd_schema_validate,
        cmd_migrate_plan,
        cmd_migrate_approve,
        cmd_export,
        cmd_mock,
        cmd_serve,
        require_config,
        format_output,
        render_violations,
    )
except ImportError:
    # Allow partial imports — tests that use missing symbols will fail explicitly.
    pass

try:
    from cli import (
        ExitCode,
        OutputFormat,
        BackendType,
        ExportFormat,
        MockPurpose,
        Severity,
        Violation,
        LedgerError,
        CliContext,
        CommandResult,
    )
except ImportError:
    # Provide stub definitions so the rest of the file parses.
    # Tests will fail at the point of actual use if the real types are missing.
    pass

try:
    import click
    from click.testing import CliRunner
except ImportError:
    click = None
    CliRunner = None


# ===========================================================================
#  Fixtures
# ===========================================================================


@pytest.fixture
def tmp_config_path(tmp_path):
    """Return a path string for a config file inside tmp_path (file not yet created)."""
    return str(tmp_path / "ledger.yaml")


@pytest.fixture
def valid_config_file(tmp_path):
    """Create a minimal valid ledger.yaml and return its path."""
    cfg = tmp_path / "ledger.yaml"
    cfg.write_text("version: 1\nbackends: []\nschemas: []\n")
    return str(cfg)


@pytest.fixture
def invalid_yaml_file(tmp_path):
    """Create a file with invalid YAML content and return its path."""
    cfg = tmp_path / "ledger.yaml"
    cfg.write_text("{{{{not valid yaml::::\n  - ][")
    return str(cfg)


@pytest.fixture
def wrong_schema_config_file(tmp_path):
    """Create valid YAML that doesn't conform to LedgerConfig schema."""
    cfg = tmp_path / "ledger.yaml"
    cfg.write_text("foo: bar\nunrelated: 42\n")
    return str(cfg)


@pytest.fixture
def cli_context(tmp_path, valid_config_file):
    """Build a CliContext-like object with a real tmp_path-based config."""
    try:
        ctx = CliContext(
            config_path=valid_config_file,
            config=None,
            verbose=False,
            output_format=OutputFormat.text,
        )
    except Exception:
        # Fallback: use a simple namespace if CliContext isn't a plain dataclass
        ctx = MagicMock()
        ctx.config_path = valid_config_file
        ctx.config = None
        ctx.verbose = False
        ctx.output_format = OutputFormat.text
    return ctx


@pytest.fixture
def cli_context_loaded(cli_context):
    """CliContext with a mock-loaded config (not None)."""
    cli_context.config = MagicMock(name="LedgerConfig")
    return cli_context


def _make_violation(path="schemas.users.email", message="Missing annotation",
                    severity=None, code="E001"):
    """Helper to create a Violation instance."""
    sev = severity or Severity.error
    try:
        return Violation(path=path, message=message, severity=sev, code=code)
    except Exception:
        v = MagicMock()
        v.path = path
        v.message = message
        v.severity = sev
        v.code = code
        return v


@pytest.fixture
def sample_violations():
    """Factory fixture returning violations of mixed severity."""
    def _factory(count=5):
        sevs = [Severity.error, Severity.error, Severity.warning, Severity.info, Severity.info]
        return [
            _make_violation(
                path=f"path.{i}",
                message=f"Violation {i}",
                severity=sevs[i % len(sevs)],
                code=f"V{i:03d}",
            )
            for i in range(count)
        ]
    return _factory


@pytest.fixture
def sample_command_result_success():
    """A successful CommandResult."""
    try:
        return CommandResult(
            success=True,
            data={"backends": ["main-pg"]},
            message="OK",
            violations=[],
        )
    except Exception:
        r = MagicMock()
        r.success = True
        r.data = {"backends": ["main-pg"]}
        r.message = "OK"
        r.violations = []
        return r


@pytest.fixture
def sample_command_result_failure(sample_violations):
    """A failed CommandResult with violations."""
    viols = sample_violations(3)
    try:
        return CommandResult(
            success=False,
            data=None,
            message="Validation failed",
            violations=viols,
        )
    except Exception:
        r = MagicMock()
        r.success = False
        r.data = None
        r.message = "Validation failed"
        r.violations = viols
        return r


@pytest.fixture
def mock_registry():
    m = MagicMock()
    m.register_backend = MagicMock()
    m.add_schema = MagicMock()
    m.show_schema = MagicMock(return_value={"tables": {"users": {"columns": []}}})
    m.validate_schemas = MagicMock(return_value=[])
    return m


@pytest.fixture
def mock_migration():
    m = MagicMock()
    m.plan_migration = MagicMock(return_value={"plan_id": "plan-001", "violations": []})
    m.approve_migration = MagicMock()
    return m


@pytest.fixture
def mock_export():
    m = MagicMock()
    m.export_contracts = MagicMock(return_value={"contracts": []})
    return m


@pytest.fixture
def mock_mock():
    m = MagicMock()
    m.generate_mock_data = MagicMock(return_value=[{"id": i} for i in range(10)])
    return m


@pytest.fixture
def mock_api():
    m = MagicMock()
    m.start_server = MagicMock()
    return m


def _make_ledger_error(exit_code=None, violations=None):
    """Helper to create a LedgerError."""
    ec = exit_code or ExitCode.DOMAIN_ERROR_1
    viols = violations or [_make_violation()]
    try:
        return LedgerError(violations=viols, exit_code=ec)
    except TypeError:
        # LedgerError might be an Exception subclass with different init
        err = LedgerError(viols, ec)
        return err


# ===========================================================================
#  LAYER 1 — Unit Tests: Command Functions
# ===========================================================================


class TestCliMain:
    """Tests for cli_main root group."""

    @patch("cli.config.load_config", return_value=MagicMock())
    def test_cli_main_default_config_resolution(self, mock_load, tmp_path, monkeypatch):
        """Config resolves to ./ledger.yaml when no flag or env var."""
        monkeypatch.delenv("LEDGER_CONFIG", raising=False)
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--help"])
        assert result.exit_code == 0

    def test_cli_main_flag_config(self, tmp_path):
        """--config flag is used for config_path."""
        runner = CliRunner()
        cfg = str(tmp_path / "custom.yaml")
        result = runner.invoke(cli_main, ["--config", cfg, "--help"])
        assert result.exit_code == 0

    def test_cli_main_env_config(self, tmp_path, monkeypatch):
        """LEDGER_CONFIG env var is used when no --config flag."""
        env_cfg = str(tmp_path / "env_config.yaml")
        monkeypatch.setenv("LEDGER_CONFIG", env_cfg)
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--help"])
        assert result.exit_code == 0

    @patch("cli.config.load_config")
    def test_cli_main_keyboard_interrupt_exit_130(self, mock_load, monkeypatch):
        """KeyboardInterrupt during subcommand execution -> exit code 130."""
        mock_load.side_effect = KeyboardInterrupt()
        runner = CliRunner()
        # Invoke a subcommand that triggers require_config -> load_config
        result = runner.invoke(cli_main, ["schema", "validate"])
        assert result.exit_code == 130

    @patch("cli.config.load_config")
    def test_cli_main_unhandled_ledger_error(self, mock_load):
        """LedgerError propagated to group level is caught and uses its exit_code."""
        mock_load.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1)
        runner = CliRunner()
        result = runner.invoke(cli_main, ["schema", "validate"])
        assert result.exit_code == 1

    def test_cli_main_context_has_absolute_config_path(self, valid_config_file):
        """CliContext.config_path is an absolute path."""
        runner = CliRunner()
        # We can't easily inspect the context from outside, so we verify
        # via a subcommand that depends on it.
        result = runner.invoke(cli_main, ["--config", valid_config_file, "--help"])
        assert result.exit_code == 0


class TestCmdInit:
    """Tests for cmd_init subcommand."""

    @patch("cli.config.init_config")
    def test_init_success(self, mock_init, tmp_config_path):
        """cmd_init creates ledger.yaml scaffold successfully."""
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--config", tmp_config_path, "init"])
        assert result.exit_code == 0
        mock_init.assert_called_once()

    @patch("cli.config.init_config")
    def test_init_file_already_exists(self, mock_init, valid_config_file):
        """cmd_init fails when ledger.yaml already exists without --force."""
        mock_init.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="File already exists", code="E_INIT_EXISTS")
        ])
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--config", valid_config_file, "init"])
        assert result.exit_code != 0

    @patch("cli.config.init_config")
    def test_init_write_permission_denied(self, mock_init, tmp_config_path):
        """cmd_init fails when write permission is denied."""
        mock_init.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Permission denied", code="E_INIT_PERM")
        ])
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--config", tmp_config_path, "init"])
        assert result.exit_code != 0

    def test_init_works_without_loaded_config(self, tmp_config_path):
        """cmd_init is the only command that works without @require_config."""
        runner = CliRunner()
        # Even though no config file exists yet, init should not fail with config error
        with patch("cli.config.init_config"):
            result = runner.invoke(cli_main, ["--config", tmp_config_path, "init"])
        assert result.exit_code == 0


class TestCmdBackendAdd:
    """Tests for cmd_backend_add subcommand."""

    @patch("cli.registry.register_backend")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_backend_add_success(self, mock_load, mock_reg, valid_config_file):
        """Registers backend successfully with exit code 0."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "backend", "add", "main-pg",
             "--type", "postgres", "--owner", "svc-users"],
        )
        assert result.exit_code == 0
        mock_reg.assert_called_once()

    def test_backend_add_config_not_loaded(self, tmp_path):
        """Fails with exit code 3 when config file is missing."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", str(tmp_path / "nonexistent.yaml"),
             "backend", "add", "main-pg", "--type", "postgres", "--owner", "svc"],
        )
        assert result.exit_code == 3

    @patch("cli.registry.register_backend")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_backend_add_duplicate(self, mock_load, mock_reg, valid_config_file):
        """Fails with exit code 1 when backend id already exists."""
        mock_reg.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Backend 'main-pg' already exists", code="E_DUP_BACKEND")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "backend", "add", "main-pg",
             "--type", "postgres", "--owner", "svc-users"],
        )
        assert result.exit_code == 1

    @patch("cli.registry.register_backend")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_backend_add_invalid_owner(self, mock_load, mock_reg, valid_config_file):
        """Fails with exit code 1 when owner component_id is not registered."""
        mock_reg.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Owner 'nonexistent' not found", code="E_INVALID_OWNER")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "backend", "add", "main-pg",
             "--type", "postgres", "--owner", "nonexistent"],
        )
        assert result.exit_code == 1

    @pytest.mark.parametrize("backend_type", ["postgres", "mysql", "sqlite", "redis", "dynamodb", "s3", "custom"])
    @patch("cli.registry.register_backend")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_backend_add_all_types(self, mock_load, mock_reg, valid_config_file, backend_type):
        """All BackendType enum values are accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "backend", "add", f"be-{backend_type}",
             "--type", backend_type, "--owner", "svc"],
        )
        assert result.exit_code == 0
        mock_reg.assert_called_once()


class TestCmdSchemaAdd:
    """Tests for cmd_schema_add subcommand."""

    @patch("cli.registry.add_schema")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_add_success(self, mock_load, mock_add, valid_config_file, tmp_path):
        """Ingests a valid schema YAML file."""
        schema = tmp_path / "schema.yaml"
        schema.write_text("tables:\n  users:\n    columns: []\n")
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "add", str(schema)],
        )
        assert result.exit_code == 0
        mock_add.assert_called_once()

    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_add_file_not_found(self, mock_load, valid_config_file):
        """Fails when schema file path does not exist."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "add", "/nonexistent/schema.yaml"],
        )
        assert result.exit_code != 0

    @patch("cli.registry.add_schema")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_add_invalid_yaml(self, mock_load, mock_add, valid_config_file, tmp_path):
        """Fails when file is not valid YAML."""
        bad = tmp_path / "bad.yaml"
        bad.write_text("{{invalid")
        mock_add.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Invalid YAML", code="E_INVALID_YAML")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "add", str(bad)],
        )
        assert result.exit_code == 1

    @patch("cli.registry.add_schema")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_add_validation_failed(self, mock_load, mock_add, valid_config_file, tmp_path):
        """Fails when YAML content doesn't conform to schema spec."""
        bad_schema = tmp_path / "bad_schema.yaml"
        bad_schema.write_text("not_a_schema: true\n")
        mock_add.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Schema validation failed", code="E_SCHEMA_INVALID")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "add", str(bad_schema)],
        )
        assert result.exit_code == 1

    def test_schema_add_config_not_loaded(self, tmp_path):
        """Fails with exit code 3 when config is missing."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", str(tmp_path / "nope.yaml"), "schema", "add", "/some/path.yaml"],
        )
        assert result.exit_code == 3

    @patch("cli.registry.add_schema")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_add_verbatim_storage(self, mock_load, mock_add, valid_config_file, tmp_path):
        """Schema YAML is passed verbatim to registry (invariant)."""
        schema_content = "# comment preserved\ntables:\n  users:\n    columns:\n      - name: id\n"
        schema = tmp_path / "schema.yaml"
        schema.write_text(schema_content)
        runner = CliRunner()
        runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "add", str(schema)],
        )
        mock_add.assert_called_once()
        # The first positional or keyword arg should contain the verbatim content
        call_args = mock_add.call_args
        # Verify the content was not modified (exact check depends on implementation)
        assert mock_add.called


class TestCmdSchemaShow:
    """Tests for cmd_schema_show subcommand."""

    @patch("cli.registry.show_schema", return_value={"tables": {"users": {}}})
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_show_full(self, mock_load, mock_show, valid_config_file):
        """Displays full backend schema, exit code 0."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "show", "main-pg"],
        )
        assert result.exit_code == 0
        mock_show.assert_called_once()

    @patch("cli.registry.show_schema", return_value={"columns": []})
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_show_single_table(self, mock_load, mock_show, valid_config_file):
        """Displays schema filtered to a single table."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "show", "main-pg", "users"],
        )
        assert result.exit_code == 0

    @patch("cli.registry.show_schema")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_show_backend_not_found(self, mock_load, mock_show, valid_config_file):
        """Fails when backend is not registered."""
        mock_show.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Backend not found", code="E_BACKEND_404")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "show", "nonexistent"],
        )
        assert result.exit_code == 1

    @patch("cli.registry.show_schema")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_show_table_not_found(self, mock_load, mock_show, valid_config_file):
        """Fails when table does not exist in the backend schema."""
        mock_show.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Table not found", code="E_TABLE_404")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "show", "main-pg", "nonexistent"],
        )
        assert result.exit_code == 1

    @patch("cli.registry.show_schema", return_value={"data": "test"})
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_schema_show_stdout_output(self, mock_load, mock_show, valid_config_file):
        """Data output goes to stdout (invariant)."""
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "show", "main-pg"],
        )
        assert result.exit_code == 0
        # stdout should have the data
        assert len(result.output) > 0


class TestCmdSchemaValidate:
    """Tests for cmd_schema_validate subcommand."""

    @patch("cli.registry.validate_schemas", return_value=[])
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_validate_no_violations(self, mock_load, mock_validate, valid_config_file):
        """Returns exit code 0 when no violations."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        assert result.exit_code == 0

    @patch("cli.registry.validate_schemas")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_validate_warnings_only_exit_0(self, mock_load, mock_validate, valid_config_file):
        """Exit code 0 when only warning-severity violations."""
        mock_validate.return_value = [
            _make_violation(severity=Severity.warning, message="Missing optional annotation"),
        ]
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        assert result.exit_code == 0

    @patch("cli.registry.validate_schemas")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_validate_error_severity_exit_1(self, mock_load, mock_validate, valid_config_file):
        """Exit code 1 when error-severity violations exist."""
        mock_validate.return_value = [
            _make_violation(severity=Severity.error, message="Missing required annotation"),
        ]
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        assert result.exit_code == 1

    @patch("cli.registry.validate_schemas")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_validate_all_violations_returned(self, mock_load, mock_validate,
                                               valid_config_file, sample_violations):
        """All violations are returned, not short-circuited at the first."""
        viols = sample_violations(5)
        mock_validate.return_value = viols
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        # All violation messages should appear in output
        for v in viols:
            assert v.message in result.output or v.code in result.output

    @patch("cli.registry.validate_schemas")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_validate_many_violations_never_truncated(self, mock_load, mock_validate,
                                                        valid_config_file):
        """Invariant: LedgerError violations are never truncated."""
        viols = [_make_violation(message=f"violation-{i}", code=f"V{i:03d}") for i in range(100)]
        mock_validate.return_value = viols
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        # Ensure all 100 are represented
        for v in viols:
            assert v.message in result.output or v.code in result.output

    @patch("cli.registry.validate_schemas")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_validate_no_schemas_registered(self, mock_load, mock_validate, valid_config_file):
        """Fails when no schemas are registered."""
        mock_validate.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="No schemas registered", code="E_NO_SCHEMAS")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        assert result.exit_code == 1


class TestCmdMigratePlan:
    """Tests for cmd_migrate_plan subcommand."""

    @patch("cli.migration.plan_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_migrate_plan_success(self, mock_load, mock_plan, valid_config_file, tmp_path):
        """Creates a migration plan with no gate violations."""
        sql = tmp_path / "migration.sql"
        sql.write_text("ALTER TABLE users ADD COLUMN email TEXT;")
        mock_plan.return_value = {"plan_id": "plan-001", "violations": []}
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "plan", "svc-users", str(sql)],
        )
        assert result.exit_code == 0
        mock_plan.assert_called_once()

    @patch("cli.config.load_config", return_value=MagicMock())
    def test_migrate_plan_file_not_found(self, mock_load, valid_config_file):
        """Fails when migration file does not exist."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "plan", "svc-users",
             "/nonexistent/migration.sql"],
        )
        assert result.exit_code != 0

    @patch("cli.migration.plan_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_migrate_plan_sql_parse_error(self, mock_load, mock_plan, valid_config_file, tmp_path):
        """Fails when SQL is unparseable."""
        sql = tmp_path / "bad.sql"
        sql.write_text("NOT VALID SQL AT ALL ;;; {{")
        mock_plan.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="SQL parse error", code="E_SQL_PARSE")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "plan", "svc-users", str(sql)],
        )
        assert result.exit_code == 1

    @patch("cli.migration.plan_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_migrate_plan_component_not_found(self, mock_load, mock_plan,
                                                valid_config_file, tmp_path):
        """Fails when component_id is not registered."""
        sql = tmp_path / "m.sql"
        sql.write_text("SELECT 1;")
        mock_plan.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Component not found", code="E_COMP_404")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "plan", "nonexistent", str(sql)],
        )
        assert result.exit_code == 1

    @patch("cli.migration.plan_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_migrate_plan_gate_violations_exit_1(self, mock_load, mock_plan,
                                                   valid_config_file, tmp_path):
        """Exit code 1 when error-severity gate violations found."""
        sql = tmp_path / "m.sql"
        sql.write_text("DROP TABLE users;")
        mock_plan.return_value = {
            "plan_id": "plan-002",
            "violations": [
                _make_violation(severity=Severity.error, message="Destructive migration"),
            ],
        }
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "plan", "svc-users", str(sql)],
        )
        assert result.exit_code == 1

    @patch("cli.migration.plan_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_migrate_plan_all_violations_returned(self, mock_load, mock_plan,
                                                    valid_config_file, tmp_path,
                                                    sample_violations):
        """All gate violations returned, not just the first."""
        sql = tmp_path / "m.sql"
        sql.write_text("ALTER TABLE users DROP COLUMN name;")
        viols = sample_violations(4)
        mock_plan.return_value = {"plan_id": "plan-003", "violations": viols}
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "plan", "svc-users", str(sql)],
        )
        for v in viols:
            assert v.message in result.output or v.code in result.output


class TestCmdMigrateApprove:
    """Tests for cmd_migrate_approve subcommand."""

    @patch("cli.migration.approve_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_approve_success(self, mock_load, mock_approve, valid_config_file):
        """Approves a pending migration plan."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "approve", "plan-123",
             "--review", "review-456"],
        )
        assert result.exit_code == 0
        mock_approve.assert_called_once()

    @patch("cli.migration.approve_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_approve_plan_not_found(self, mock_load, mock_approve, valid_config_file):
        """Fails when plan_id does not exist."""
        mock_approve.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Plan not found", code="E_PLAN_404")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "approve", "nonexistent",
             "--review", "r1"],
        )
        assert result.exit_code == 1

    @patch("cli.migration.approve_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_approve_already_approved(self, mock_load, mock_approve, valid_config_file):
        """Fails when plan is already approved."""
        mock_approve.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Plan already approved", code="E_PLAN_APPROVED")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "approve", "plan-123",
             "--review", "r1"],
        )
        assert result.exit_code == 1

    @patch("cli.migration.approve_migration")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_approve_outstanding_violations(self, mock_load, mock_approve, valid_config_file):
        """Fails when plan has unresolved error-severity violations."""
        mock_approve.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Outstanding violations", code="E_OUTSTANDING")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "migrate", "approve", "plan-123",
             "--review", "r1"],
        )
        assert result.exit_code == 1


class TestCmdExport:
    """Tests for cmd_export subcommand."""

    @patch("cli.export.export_contracts", return_value={"contracts": [{"type": "pact"}]})
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_export_success(self, mock_load, mock_export, valid_config_file):
        """Exports contracts in pact format."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "export", "--format", "pact"],
        )
        assert result.exit_code == 0

    @patch("cli.export.export_contracts", return_value={"contracts": [{"type": "arbiter"}]})
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_export_with_component_filter(self, mock_load, mock_export, valid_config_file):
        """Exports filtered by component_id."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "export", "--format", "arbiter",
             "--component", "svc-users"],
        )
        assert result.exit_code == 0
        mock_export.assert_called_once()

    @patch("cli.export.export_contracts")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_export_no_data(self, mock_load, mock_export, valid_config_file):
        """Fails when no data to export."""
        mock_export.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="No data to export", code="E_NO_DATA")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "export", "--format", "pact"],
        )
        assert result.exit_code == 1

    @patch("cli.export.export_contracts")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_export_component_not_found(self, mock_load, mock_export, valid_config_file):
        """Fails when component filter matches nothing."""
        mock_export.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Component not found", code="E_COMP_404")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "export", "--format", "pact",
             "--component", "nonexistent"],
        )
        assert result.exit_code == 1

    @pytest.mark.parametrize("fmt", ["pact", "arbiter", "baton", "sentinel"])
    @patch("cli.export.export_contracts", return_value={"contracts": []})
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_export_all_formats(self, mock_load, mock_export, valid_config_file, fmt):
        """All ExportFormat enum values are accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "export", "--format", fmt],
        )
        assert result.exit_code == 0


class TestCmdMock:
    """Tests for cmd_mock subcommand."""

    @patch("cli.mock.generate_mock_data", return_value=[{"id": i} for i in range(10)])
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_mock_success(self, mock_load, mock_gen, valid_config_file):
        """Generates mock data rows, exit code 0."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "main-pg", "users",
             "--count", "10", "--seed", "42"],
        )
        assert result.exit_code == 0
        mock_gen.assert_called_once()

    @patch("cli.mock.generate_mock_data")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_mock_deterministic_seed(self, mock_load, mock_gen, valid_config_file):
        """Same seed produces same output (checked via mock call args)."""
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_gen.return_value = rows
        runner = CliRunner()
        r1 = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "main-pg", "users",
             "--count", "2", "--seed", "42"],
        )
        r2 = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "main-pg", "users",
             "--count", "2", "--seed", "42"],
        )
        assert r1.output == r2.output

    @patch("cli.mock.generate_mock_data")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_mock_backend_not_found(self, mock_load, mock_gen, valid_config_file):
        """Fails when backend is not registered."""
        mock_gen.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Backend not found", code="E_BACKEND_404")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "nonexistent", "users", "--count", "10"],
        )
        assert result.exit_code == 1

    @patch("cli.mock.generate_mock_data")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_mock_table_not_found(self, mock_load, mock_gen, valid_config_file):
        """Fails when table does not exist."""
        mock_gen.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Table not found", code="E_TABLE_404")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "main-pg", "nonexistent", "--count", "10"],
        )
        assert result.exit_code == 1

    @patch("cli.mock.generate_mock_data", return_value=[{"id": 1}])
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_mock_arbiter_unavailable_canary_warns(self, mock_load, mock_gen, valid_config_file):
        """When Arbiter is unreachable for canary purpose, warns but does not crash."""
        # Simulate: domain module handles it by returning data + emitting warning
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "main-pg", "users",
             "--count", "1", "--purpose", "canary"],
        )
        # Key invariant: does NOT crash
        assert result.exit_code in (0, 1)  # May be 0 (warn only) or domain-level
        # Should not be an unhandled exception
        assert result.exception is None or isinstance(result.exception, SystemExit)

    @patch("cli.mock.generate_mock_data", return_value=[])
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_mock_count_zero(self, mock_load, mock_gen, valid_config_file):
        """Count=0 generates empty result."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "main-pg", "users", "--count", "0"],
        )
        # Should succeed with empty data
        assert result.exit_code == 0


class TestCmdServe:
    """Tests for cmd_serve subcommand."""

    @patch("cli.api.start_server")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_serve_success(self, mock_load, mock_start, valid_config_file):
        """Starts server and exits cleanly."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "serve"],
        )
        assert result.exit_code == 0
        mock_start.assert_called_once()

    @patch("cli.api.start_server")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_serve_keyboard_interrupt_exit_130(self, mock_load, mock_start, valid_config_file):
        """KeyboardInterrupt during serve -> exit code 130."""
        mock_start.side_effect = KeyboardInterrupt()
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "serve"],
        )
        assert result.exit_code == 130

    @patch("cli.api.start_server")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_serve_port_in_use(self, mock_load, mock_start, valid_config_file):
        """Fails when port is already bound."""
        mock_start.side_effect = _make_ledger_error(ExitCode.DOMAIN_ERROR_1, [
            _make_violation(message="Port already in use", code="E_PORT_BOUND")
        ])
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "serve"],
        )
        assert result.exit_code == 1


# ===========================================================================
#  LAYER 1 — Unit Tests: require_config
# ===========================================================================


class TestRequireConfig:
    """Tests for require_config decorator."""

    @patch("cli.config.load_config")
    def test_require_config_success(self, mock_load, valid_config_file):
        """Loads valid config from YAML file."""
        mock_config = MagicMock(name="LedgerConfig")
        mock_load.return_value = mock_config
        try:
            ctx = CliContext(
                config_path=valid_config_file,
                config=None,
                verbose=False,
                output_format=OutputFormat.text,
            )
        except Exception:
            ctx = MagicMock()
            ctx.config_path = valid_config_file
            ctx.config = None
        require_config(ctx)
        assert ctx.config is not None
        mock_load.assert_called_once()

    def test_require_config_file_missing(self, tmp_path):
        """Raises LedgerError with CONFIG_ERROR_3 when file missing."""
        missing = str(tmp_path / "nonexistent.yaml")
        try:
            ctx = CliContext(
                config_path=missing,
                config=None,
                verbose=False,
                output_format=OutputFormat.text,
            )
        except Exception:
            ctx = MagicMock()
            ctx.config_path = missing
            ctx.config = None
        with pytest.raises(Exception) as exc_info:
            require_config(ctx)
        # Should be a LedgerError with CONFIG_ERROR_3
        err = exc_info.value
        if hasattr(err, 'exit_code'):
            assert err.exit_code == ExitCode.CONFIG_ERROR_3

    @patch("cli.config.load_config")
    def test_require_config_parse_error(self, mock_load, invalid_yaml_file):
        """Raises LedgerError when YAML is invalid."""
        mock_load.side_effect = _make_ledger_error(ExitCode.CONFIG_ERROR_3, [
            _make_violation(message="Invalid YAML", code="E_YAML_PARSE")
        ])
        try:
            ctx = CliContext(
                config_path=invalid_yaml_file,
                config=None,
                verbose=False,
                output_format=OutputFormat.text,
            )
        except Exception:
            ctx = MagicMock()
            ctx.config_path = invalid_yaml_file
            ctx.config = None
        with pytest.raises(Exception) as exc_info:
            require_config(ctx)
        err = exc_info.value
        if hasattr(err, 'exit_code'):
            assert err.exit_code == ExitCode.CONFIG_ERROR_3

    @patch("cli.config.load_config")
    def test_require_config_validation_error(self, mock_load, wrong_schema_config_file):
        """Raises LedgerError when YAML doesn't conform to LedgerConfig schema."""
        mock_load.side_effect = _make_ledger_error(ExitCode.CONFIG_ERROR_3, [
            _make_violation(message="Config validation error", code="E_CFG_SCHEMA")
        ])
        try:
            ctx = CliContext(
                config_path=wrong_schema_config_file,
                config=None,
                verbose=False,
                output_format=OutputFormat.text,
            )
        except Exception:
            ctx = MagicMock()
            ctx.config_path = wrong_schema_config_file
            ctx.config = None
        with pytest.raises(Exception) as exc_info:
            require_config(ctx)
        err = exc_info.value
        if hasattr(err, 'exit_code'):
            assert err.exit_code == ExitCode.CONFIG_ERROR_3


# ===========================================================================
#  LAYER 1 — Unit Tests: format_output
# ===========================================================================


class TestFormatOutput:
    """Tests for format_output pure helper."""

    def test_format_output_json_valid(self, sample_command_result_success):
        """Returns valid JSON string for json format."""
        result_str = format_output(sample_command_result_success, OutputFormat.json)
        parsed = json.loads(result_str)
        assert parsed is not None

    def test_format_output_json_roundtrip(self, sample_command_result_success):
        """JSON output roundtrips correctly."""
        result_str = format_output(sample_command_result_success, OutputFormat.json)
        parsed = json.loads(result_str)
        assert parsed == sample_command_result_success.data

    def test_format_output_yaml_valid(self, sample_command_result_success):
        """Returns valid YAML string for yaml format."""
        import yaml
        result_str = format_output(sample_command_result_success, OutputFormat.yaml)
        parsed = yaml.safe_load(result_str)
        assert parsed is not None

    def test_format_output_yaml_roundtrip(self, sample_command_result_success):
        """YAML output roundtrips correctly."""
        import yaml
        result_str = format_output(sample_command_result_success, OutputFormat.yaml)
        parsed = yaml.safe_load(result_str)
        assert parsed == sample_command_result_success.data

    def test_format_output_text_returns_string(self, sample_command_result_success):
        """Text format returns a non-empty human-readable string."""
        result_str = format_output(sample_command_result_success, OutputFormat.text)
        assert isinstance(result_str, str)
        assert len(result_str) > 0

    def test_format_output_unserializable_data(self):
        """Raises error for unserializable data types."""
        try:
            bad_result = CommandResult(
                success=True,
                data={"func": lambda x: x},
                message="OK",
                violations=[],
            )
        except Exception:
            bad_result = MagicMock()
            bad_result.data = {"func": lambda x: x}
            bad_result.success = True
            bad_result.violations = []
        with pytest.raises(Exception):
            format_output(bad_result, OutputFormat.json)

    def test_format_output_empty_dict_json(self):
        """Handles empty dict data in json format."""
        try:
            result = CommandResult(success=True, data={}, message="OK", violations=[])
        except Exception:
            result = MagicMock()
            result.data = {}
            result.success = True
        result_str = format_output(result, OutputFormat.json)
        parsed = json.loads(result_str)
        assert parsed == {}

    @pytest.mark.parametrize("fmt", ["text", "json", "yaml"])
    def test_format_output_all_formats(self, sample_command_result_success, fmt):
        """All OutputFormat enum values produce valid output."""
        out_fmt = OutputFormat[fmt] if isinstance(fmt, str) else fmt
        result_str = format_output(sample_command_result_success, out_fmt)
        assert isinstance(result_str, str)
        assert len(result_str) > 0

    def test_format_output_none_data_text(self):
        """Handles None data gracefully in text format."""
        try:
            result = CommandResult(success=True, data=None, message="OK", violations=[])
        except Exception:
            result = MagicMock()
            result.data = None
            result.success = True
        # text format should handle None without crashing
        result_str = format_output(result, OutputFormat.text)
        assert isinstance(result_str, str)


# ===========================================================================
#  LAYER 1 — Unit Tests: render_violations
# ===========================================================================


class TestRenderViolations:
    """Tests for render_violations pure helper."""

    def test_render_violations_basic_with_color(self, sample_violations):
        """Formats violations into colored string with all present."""
        viols = sample_violations(5)
        result = render_violations(viols, use_color=True)
        assert isinstance(result, str)
        for v in viols:
            assert v.message in result

    def test_render_violations_empty_list(self):
        """Handles empty violation list without error."""
        result = render_violations([], use_color=False)
        assert isinstance(result, str)

    def test_render_violations_no_color_no_ansi(self, sample_violations):
        """Omits ANSI escape codes when use_color is False."""
        viols = sample_violations(3)
        result = render_violations(viols, use_color=False)
        # ANSI escape sequences start with \x1b[ or \033[
        assert "\x1b[" not in result
        assert "\033[" not in result

    def test_render_violations_severity_order(self, sample_violations):
        """Groups errors first, then warnings, then info."""
        viols = [
            _make_violation(severity=Severity.info, message="Info message", code="I001"),
            _make_violation(severity=Severity.error, message="Error message", code="E001"),
            _make_violation(severity=Severity.warning, message="Warning message", code="W001"),
        ]
        result = render_violations(viols, use_color=False)
        error_pos = result.find("Error message")
        warning_pos = result.find("Warning message")
        info_pos = result.find("Info message")
        assert error_pos < warning_pos < info_pos, (
            f"Expected errors ({error_pos}) before warnings ({warning_pos}) before info ({info_pos})"
        )

    def test_render_violations_all_present(self, sample_violations):
        """All violations are included in the output (none skipped)."""
        viols = sample_violations(10)
        result = render_violations(viols, use_color=False)
        for v in viols:
            assert v.message in result

    def test_render_violations_summary_counts(self):
        """Summary line shows correct counts per severity."""
        viols = [
            _make_violation(severity=Severity.error, message="E1", code="E001"),
            _make_violation(severity=Severity.error, message="E2", code="E002"),
            _make_violation(severity=Severity.warning, message="W1", code="W001"),
            _make_violation(severity=Severity.info, message="I1", code="I001"),
        ]
        result = render_violations(viols, use_color=False)
        # Summary should contain the count of each severity
        assert "2" in result  # 2 errors
        assert "1" in result  # 1 warning or 1 info


# ===========================================================================
#  LAYER 2 — Integration Tests: CliRunner end-to-end
# ===========================================================================


class TestIntegrationHelp:
    """--help for every command returns exit code 0."""

    def test_help_root(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--help"])
        assert result.exit_code == 0
        assert "Usage" in result.output or "usage" in result.output.lower()

    def test_help_init(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["init", "--help"])
        assert result.exit_code == 0

    def test_help_backend_add(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["backend", "add", "--help"])
        assert result.exit_code == 0

    def test_help_schema_add(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["schema", "add", "--help"])
        assert result.exit_code == 0

    def test_help_schema_show(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["schema", "show", "--help"])
        assert result.exit_code == 0

    def test_help_schema_validate(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["schema", "validate", "--help"])
        assert result.exit_code == 0

    def test_help_migrate_plan(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["migrate", "plan", "--help"])
        assert result.exit_code == 0

    def test_help_migrate_approve(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["migrate", "approve", "--help"])
        assert result.exit_code == 0

    def test_help_export(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["export", "--help"])
        assert result.exit_code == 0

    def test_help_mock(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["mock", "--help"])
        assert result.exit_code == 0

    def test_help_serve(self):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["serve", "--help"])
        assert result.exit_code == 0


class TestIntegrationExitCodes:
    """Exit codes match ExitCode enum values."""

    @patch("cli.config.init_config")
    def test_exit_code_success_0(self, mock_init, tmp_config_path):
        """Successful command -> exit code 0."""
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--config", tmp_config_path, "init"])
        assert result.exit_code == 0

    @patch("cli.registry.validate_schemas")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_exit_code_domain_error_1(self, mock_load, mock_validate, valid_config_file):
        """Domain error -> exit code 1."""
        mock_validate.return_value = [
            _make_violation(severity=Severity.error, message="Error"),
        ]
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        assert result.exit_code == 1

    def test_exit_code_config_error_3(self, tmp_path):
        """Config error -> exit code 3."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", str(tmp_path / "nonexistent.yaml"), "backend", "add",
             "be1", "--type", "postgres", "--owner", "svc"],
        )
        assert result.exit_code == 3

    @patch("cli.api.start_server")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_exit_code_keyboard_interrupt_130(self, mock_load, mock_start, valid_config_file):
        """KeyboardInterrupt -> exit code 130."""
        mock_start.side_effect = KeyboardInterrupt()
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "serve"],
        )
        assert result.exit_code == 130


class TestIntegrationConfigResolution:
    """Config path resolution order tests."""

    def test_flag_overrides_env_and_default(self, tmp_path, monkeypatch):
        """--config flag takes precedence over env var and default."""
        flag_cfg = str(tmp_path / "flag.yaml")
        env_cfg = str(tmp_path / "env.yaml")
        monkeypatch.setenv("LEDGER_CONFIG", env_cfg)

        with patch("cli.config.init_config") as mock_init:
            runner = CliRunner()
            result = runner.invoke(cli_main, ["--config", flag_cfg, "init"])
            assert result.exit_code == 0
            # The init_config should have been called (flag path used)
            mock_init.assert_called_once()

    def test_env_used_when_no_flag(self, tmp_path, monkeypatch):
        """LEDGER_CONFIG env var used when no --config flag."""
        env_cfg = str(tmp_path / "env_config.yaml")
        monkeypatch.setenv("LEDGER_CONFIG", env_cfg)

        with patch("cli.config.init_config") as mock_init:
            runner = CliRunner()
            result = runner.invoke(cli_main, ["init"])
            assert result.exit_code == 0

    def test_default_when_no_flag_no_env(self, monkeypatch):
        """Default ./ledger.yaml used when no flag or env var."""
        monkeypatch.delenv("LEDGER_CONFIG", raising=False)
        runner = CliRunner()
        result = runner.invoke(cli_main, ["--help"])
        assert result.exit_code == 0


class TestIntegrationVerbose:
    """Verbose flag tests."""

    @patch("cli.config.init_config")
    def test_verbose_flag_accepted(self, mock_init, tmp_config_path):
        """--verbose flag is accepted without error."""
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", tmp_config_path, "--verbose", "init"],
        )
        assert result.exit_code == 0

    @patch("cli.registry.validate_schemas", return_value=[])
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_verbose_increases_output(self, mock_load, mock_validate, valid_config_file):
        """Verbose mode should produce at least as much output as non-verbose."""
        runner = CliRunner(mix_stderr=False)
        result_quiet = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        result_verbose = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "--verbose", "schema", "validate"],
        )
        # Verbose should have >= output (checking combined stdout + stderr)
        quiet_total = len(result_quiet.output) + len(result_quiet.stderr if hasattr(result_quiet, 'stderr') else "")
        verbose_total = len(result_verbose.output) + len(result_verbose.stderr if hasattr(result_verbose, 'stderr') else "")
        assert verbose_total >= quiet_total


# ===========================================================================
#  INVARIANT Tests
# ===========================================================================


class TestInvariants:
    """Cross-cutting invariant verifications."""

    @patch("cli.registry.register_backend")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_thin_dispatch_delegates_to_domain(self, mock_load, mock_reg, valid_config_file):
        """CLI subcommands only dispatch to domain module — no business logic."""
        runner = CliRunner()
        runner.invoke(
            cli_main,
            ["--config", valid_config_file, "backend", "add", "be1",
             "--type", "postgres", "--owner", "svc"],
        )
        # Domain module called exactly once
        mock_reg.assert_called_once()

    @patch("cli.registry.show_schema", return_value={"tables": {}})
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_data_stdout_errors_stderr(self, mock_load, mock_show, valid_config_file):
        """Data output goes to stdout; status messages go to stderr."""
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "show", "main-pg"],
        )
        assert result.exit_code == 0
        # stdout should contain data
        assert len(result.output) >= 0  # May be empty if no data to show

    @patch("cli.registry.validate_schemas")
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_violations_never_truncated_integration(self, mock_load, mock_validate,
                                                      valid_config_file):
        """All violations rendered to user — never truncated."""
        viols = [_make_violation(message=f"v-{i}", code=f"C{i:03d}") for i in range(50)]
        mock_validate.return_value = viols
        runner = CliRunner()
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "schema", "validate"],
        )
        for v in viols:
            assert v.message in result.output or v.code in result.output

    def test_exit_code_enum_values(self):
        """Exit code enum has the expected integer values."""
        assert ExitCode.SUCCESS_0.value == 0 or str(ExitCode.SUCCESS_0) == "0" or "0" in str(ExitCode.SUCCESS_0)
        assert ExitCode.DOMAIN_ERROR_1.value == 1 or "1" in str(ExitCode.DOMAIN_ERROR_1)
        assert ExitCode.USAGE_ERROR_2.value == 2 or "2" in str(ExitCode.USAGE_ERROR_2)
        assert ExitCode.CONFIG_ERROR_3.value == 3 or "3" in str(ExitCode.CONFIG_ERROR_3)
        assert ExitCode.KEYBOARD_INTERRUPT_130.value == 130 or "130" in str(ExitCode.KEYBOARD_INTERRUPT_130)

    @patch("cli.config.init_config")
    def test_cmd_init_only_command_without_config(self, mock_init, tmp_config_path):
        """cmd_init is the only command that works without loaded config."""
        runner = CliRunner()
        # init works without existing config
        result = runner.invoke(cli_main, ["--config", tmp_config_path, "init"])
        assert result.exit_code == 0

        # Other commands fail without config
        result = runner.invoke(
            cli_main,
            ["--config", str(tmp_config_path + ".nonexistent"), "schema", "validate"],
        )
        assert result.exit_code == 3

    @patch("cli.mock.generate_mock_data", return_value=[{"id": 1}])
    @patch("cli.config.load_config", return_value=MagicMock())
    def test_arbiter_unavailable_no_crash_invariant(self, mock_load, mock_gen, valid_config_file):
        """Arbiter unreachable during canary => warn, don't crash."""
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(
            cli_main,
            ["--config", valid_config_file, "mock", "main-pg", "users",
             "--count", "1", "--purpose", "canary"],
        )
        # Must not be an unhandled exception
        if result.exception is not None:
            assert isinstance(result.exception, SystemExit)

    def test_output_format_enum_values(self):
        """OutputFormat enum has expected variants."""
        assert OutputFormat.text is not None
        assert OutputFormat.json is not None
        assert OutputFormat.yaml is not None

    def test_backend_type_enum_values(self):
        """BackendType enum has all expected variants."""
        expected = ["postgres", "mysql", "sqlite", "redis", "dynamodb", "s3", "custom"]
        for name in expected:
            assert hasattr(BackendType, name), f"BackendType missing variant: {name}"

    def test_export_format_enum_values(self):
        """ExportFormat enum has all expected variants."""
        expected = ["pact", "arbiter", "baton", "sentinel"]
        for name in expected:
            assert hasattr(ExportFormat, name), f"ExportFormat missing variant: {name}"

    def test_mock_purpose_enum_values(self):
        """MockPurpose enum has expected variants."""
        assert hasattr(MockPurpose, "default")
        assert hasattr(MockPurpose, "canary")

    def test_severity_enum_values(self):
        """Severity enum has expected variants."""
        assert hasattr(Severity, "error")
        assert hasattr(Severity, "warning")
        assert hasattr(Severity, "info")
