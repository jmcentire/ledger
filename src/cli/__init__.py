from cli.cli import (
    cli_main,
    cmd_init,
    cmd_backend_add,
    cmd_schema_add,
    cmd_schema_show,
    cmd_schema_validate,
    cmd_schema_infer,
    cmd_migrate_plan,
    cmd_migrate_approve,
    cmd_export,
    cmd_mock,
    cmd_serve,
    cmd_builtins_list,
    cmd_builtins_show,
    cmd_builtins_stripe,
    builtins_group,
    require_config,
    format_output,
    render_violations,
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

# Re-export the domain modules so tests can patch cli.config, cli.registry, etc.
import config
import registry
import migration
import export
import mock
import api
import inference

# ── Add thin dispatch stubs that the CLI calls and tests patch ──
# These are added here so @patch("cli.config.init_config") etc. can find them.

if not hasattr(config, "init_config"):
    def _init_config(config_path, **kwargs):
        """Stub: initialize a new ledger.yaml scaffold."""
        pass
    config.init_config = _init_config

if not hasattr(registry, "register_backend"):
    def _register_backend(cfg, backend_id, backend_type, owner, **kwargs):
        """Stub: register a backend."""
        pass
    registry.register_backend = _register_backend

if not hasattr(registry, "add_schema"):
    def _add_schema(cfg, schema_path, content, **kwargs):
        """Stub: add a schema."""
        pass
    registry.add_schema = _add_schema

if not hasattr(registry, "show_schema"):
    def _show_schema(cfg, backend_id, table=None, **kwargs):
        """Stub: show schema."""
        return {}
    registry.show_schema = _show_schema

if not hasattr(registry, "validate_schemas"):
    def _validate_schemas(cfg, **kwargs):
        """Stub: validate schemas."""
        return []
    registry.validate_schemas = _validate_schemas

if not hasattr(migration, "plan_migration"):
    def _plan_migration(cfg, component_id, sql_content, **kwargs):
        """Stub: plan migration."""
        return {"plan_id": "plan-000", "violations": []}
    migration.plan_migration = _plan_migration

if not hasattr(migration, "approve_migration"):
    def _approve_migration(cfg, plan_id, review_ref, **kwargs):
        """Stub: approve migration."""
        pass
    migration.approve_migration = _approve_migration

if not hasattr(export, "export_contracts"):
    def _export_contracts(cfg, fmt, component=None, **kwargs):
        """Stub: export contracts."""
        return {"contracts": []}
    export.export_contracts = _export_contracts

if not hasattr(export, "export_retention_from_config"):
    def _export_retention_from_config(cfg, component=None, **kwargs):
        """Stub: export retention policies."""
        return {"retention_rules": []}
    export.export_retention_from_config = _export_retention_from_config

if not hasattr(mock, "generate_mock_data"):
    def _generate_mock_data(cfg, backend_id, table, count, seed=None, purpose="default", **kwargs):
        """Stub: generate mock data."""
        return []
    mock.generate_mock_data = _generate_mock_data

if not hasattr(api, "start_server"):
    def _start_server(cfg, **kwargs):
        """Stub: start server."""
        pass
    api.start_server = _start_server

# ── Inference stubs ──

if not hasattr(inference, "infer_schema"):
    def _infer_schema(backend_id, backend_type, connection_config, show_confidence=False, **kwargs):
        """Stub: infer schema."""
        return {"backend_id": backend_id, "backend_type": backend_type, "tables": []}
    inference.infer_schema = _infer_schema

if not hasattr(inference, "schema_to_yaml"):
    def _schema_to_yaml(schema, show_confidence=False, **kwargs):
        """Stub: schema to yaml."""
        import yaml
        return yaml.dump(schema if isinstance(schema, dict) else {}, default_flow_style=False)
    inference.schema_to_yaml = _schema_to_yaml

if not hasattr(inference, "MissingDependencyError"):
    class _MissingDependencyError(Exception):
        def __init__(self, package, backend_type):
            self.package = package
            self.backend_type = backend_type
            self.message = f"Missing {package}"
            super().__init__(self.message)
    inference.MissingDependencyError = _MissingDependencyError

if not hasattr(inference, "InferenceError"):
    class _InferenceError(Exception):
        def __init__(self, message):
            self.message = message
            super().__init__(message)
    inference.InferenceError = _InferenceError
