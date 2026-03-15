# === CLI Entry Point (cli) v1 ===
#  Dependencies: registry, migration, export, mock, config, api
# Click-based CLI providing all subcommands for Ledger: init, backend add, schema add/show/validate, migrate plan/approve, export, mock, and serve. Structured as a Click group hierarchy with nested subgroups (backend, schema, migrate) and top-level commands (init, export, mock, serve). Each subcommand is a thin dispatch layer that loads config, calls the appropriate domain module function via typed Protocols, and formats output for the terminal. Error handling catches LedgerError at the group level and surfaces all violations with severity-colored output to stderr and non-zero exit codes.

# Module invariants:
#   - Each CLI subcommand performs zero business logic — it only dispatches to the corresponding domain module function.
#   - Data output goes to stdout; status messages and errors go to stderr.
#   - Config resolution order is always: --config flag → LEDGER_CONFIG env var → ./ledger.yaml.
#   - LedgerError violations are never truncated — all violations are rendered to the user.
#   - Exit codes are deterministic: 0=success, 1=domain/validation error, 2=usage error (Click default), 3=config error, 130=KeyboardInterrupt.
#   - Schema YAML is stored verbatim — the CLI never normalizes, reformats, or reorders YAML content.
#   - The cmd_init function is the only command that operates without a loaded config.
#   - When Arbiter is unreachable during canary mock generation, the CLI warns on stderr and continues — it never crashes.
#   - All Click callbacks are annotated with return type None.
#   - Each CLI source file stays under 300 lines; the cli/ subpackage has one file per command group.

class ExitCode(Enum):
    """Process exit codes returned by CLI commands."""
    SUCCESS_0 = "SUCCESS_0"
    DOMAIN_ERROR_1 = "DOMAIN_ERROR_1"
    USAGE_ERROR_2 = "USAGE_ERROR_2"
    CONFIG_ERROR_3 = "CONFIG_ERROR_3"
    KEYBOARD_INTERRUPT_130 = "KEYBOARD_INTERRUPT_130"

class OutputFormat(Enum):
    """Supported output serialization formats for structured command results."""
    text = "text"
    json = "json"
    yaml = "yaml"

class BackendType(Enum):
    """Supported backend storage types that can be registered with Ledger."""
    postgres = "postgres"
    mysql = "mysql"
    sqlite = "sqlite"
    redis = "redis"
    dynamodb = "dynamodb"
    s3 = "s3"
    custom = "custom"

class ExportFormat(Enum):
    """Target contract formats for the export command."""
    pact = "pact"
    arbiter = "arbiter"
    baton = "baton"
    sentinel = "sentinel"

class MockPurpose(Enum):
    """Purpose classification for mock data generation."""
    default = "default"
    canary = "canary"

class Severity(Enum):
    """Severity level for a validation violation."""
    error = "error"
    warning = "warning"
    info = "info"

class Violation:
    """A single validation violation with location, message, and severity."""
    path: str                                # required, Dot-delimited path to the offending field or entity, e.g. 'backend.users.columns.email'.
    message: str                             # required, Human-readable description of the violation.
    severity: Severity                       # required, Severity classification of this violation.
    code: str                                # required, Machine-readable error code, e.g. 'SCHEMA_TYPE_MISMATCH'.

class LedgerError:
    """Domain error carrying one or more violations. Raised by domain modules, caught by the CLI group-level error handler."""
    violations: list                         # required, length(min=1), Non-empty list of Violation structs.
    exit_code: ExitCode                      # required, Exit code to use when surfacing this error to the terminal.

class CliContext:
    """Mutable context object threaded through Click commands via @click.pass_context. Implemented as a dataclass (not Pydantic) since it is internal and mutable. Carries resolved config path and lazy-loaded config."""
    config_path: str                         # required, Resolved absolute path to ledger.yaml. Resolution order: --config flag, LEDGER_CONFIG env var, ./ledger.yaml.
    config: any = None                       # optional, Lazy-loaded LedgerConfig from the config component. None until a command decorated with @require_config triggers loading.
    verbose: bool = false                    # optional, When true, print debug-level status messages to stderr.
    output_format: OutputFormat = text       # optional, Global output format override. Individual commands may support --output to set this.

class CommandResult:
    """Uniform result envelope returned by every thin-dispatch function to the CLI callback for formatting."""
    success: bool                            # required, Whether the command completed without domain errors.
    data: any = None                         # optional, Structured payload for stdout rendering. Type depends on the command.
    message: str = None                      # optional, Human-readable status message for stderr.
    violations: list = []                    # optional, Violations to render if success is false.

class RegistryProtocol:
    """typing.Protocol defining the interface the CLI expects from the registry domain module. Used for dependency injection and testing."""
    register_backend: str                    # required, Callable[[str, BackendType, str, any], CommandResult] — registers a backend by id, type, owner component_id, and config.
    add_schema: str                          # required, Callable[[str, any], CommandResult] — ingests a schema YAML file path with config.
    show_schema: str                         # required, Callable[[str, str | None, any], CommandResult] — shows schema for backend_id and optional table name.
    validate_schemas: str                    # required, Callable[[any], CommandResult] — validates all registered schemas against obligations.

class MigrationProtocol:
    """typing.Protocol defining the interface the CLI expects from the migration domain module."""
    plan_migration: str                      # required, Callable[[str, str, any], CommandResult] — creates a migration plan for component_id from migration_file path.
    approve_migration: str                   # required, Callable[[str, str, any], CommandResult] — approves a plan_id with a reviewer id.

class ExportProtocol:
    """typing.Protocol defining the interface the CLI expects from the export domain module."""
    export_contracts: str                    # required, Callable[[ExportFormat, str | None, any], CommandResult] — exports contracts in target format, optionally filtered by component_id.

class MockProtocol:
    """typing.Protocol defining the interface the CLI expects from the mock domain module."""
    generate_mock_data: str                  # required, Callable[[str, str, int, int | None, MockPurpose, any], CommandResult] — generates mock rows for backend_id.table with count, optional seed, purpose, and config.

class ApiProtocol:
    """typing.Protocol defining the interface the CLI expects from the api domain module."""
    start_server: str                        # required, Callable[[any], None] — starts the FastAPI+uvicorn HTTP server using the provided config.

def cli_main(
    config: str = None,
    verbose: bool = false,
    output: OutputFormat = text,
) -> None:
    """
    Root Click group entry point. Resolves config_path (--config flag → LEDGER_CONFIG env var → ./ledger.yaml), initializes CliContext, attaches it to click.Context, and sets up the top-level error handler that catches LedgerError and KeyboardInterrupt.

    Postconditions:
      - CliContext is attached to click.Context.obj
      - config_path in CliContext is an absolute resolved path string

    Errors:
      - keyboard_interrupt (SystemExit): User sends SIGINT during any subcommand execution
          exit_code: 130
      - unhandled_ledger_error (SystemExit): A domain module raises LedgerError that propagates to group level
          exit_code: 1

    Side effects: Writes status/error messages to stderr, Sets process exit code
    Idempotent: yes
    """
    ...

def cmd_init(
    ctx: CliContext,
) -> None:
    """
    Subcommand `ledger init`. Creates a new ledger.yaml scaffold at the resolved config_path. Does NOT require an existing config (only command that works without @require_config). Delegates to config.init_config.

    Postconditions:
      - A valid ledger.yaml file exists at ctx.config_path
      - Exit code is 0 on success

    Errors:
      - file_already_exists (LedgerError): ledger.yaml already exists at config_path and --force is not set
          exit_code: 1
          code: CONFIG_EXISTS
      - write_permission_denied (LedgerError): Process lacks write permission to the target directory
          exit_code: 3
          code: CONFIG_WRITE_ERROR

    Side effects: Writes ledger.yaml to disk
    Idempotent: no
    """
    ...

def cmd_backend_add(
    ctx: CliContext,
    backend_id: str,           # regex(^[a-z][a-z0-9_-]{0,62}$)
    backend_type: BackendType,
    owner: str,                # regex(^[a-z][a-z0-9_-]{0,62}$)
) -> None:
    """
    Subcommand `ledger backend add <id> --type <type> --owner <component_id>`. Registers a new backend data store. Requires config. Delegates to registry.register_backend.

    Preconditions:
      - ctx.config is not None (config loaded via @require_config)

    Postconditions:
      - Backend is registered in the config
      - Success message printed to stderr
      - Exit code is 0

    Errors:
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR
      - duplicate_backend (LedgerError): A backend with the same id already exists
          exit_code: 1
          code: BACKEND_DUPLICATE
      - invalid_owner (LedgerError): The owner component_id is not registered
          exit_code: 1
          code: OWNER_NOT_FOUND

    Side effects: none
    Idempotent: no
    """
    ...

def cmd_schema_add(
    ctx: CliContext,
    path: str,
) -> None:
    """
    Subcommand `ledger schema add <path>`. Ingests a schema YAML file verbatim (no normalization or reordering). Requires config. Delegates to registry.add_schema.

    Preconditions:
      - ctx.config is not None
      - path points to an existing readable file

    Postconditions:
      - Schema is stored verbatim in the registry
      - Exit code is 0 on success

    Errors:
      - file_not_found (LedgerError): The specified path does not exist
          exit_code: 1
          code: FILE_NOT_FOUND
      - invalid_yaml (LedgerError): The file is not valid YAML
          exit_code: 1
          code: YAML_PARSE_ERROR
      - schema_validation_failed (LedgerError): The YAML content does not conform to the Ledger schema spec
          exit_code: 1
          code: SCHEMA_INVALID
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: yes
    """
    ...

def cmd_schema_show(
    ctx: CliContext,
    backend_id: str,
    table: str = None,
) -> CommandResult:
    """
    Subcommand `ledger schema show <backend_id> [<table>]`. Displays the schema for a backend, optionally filtered to a single table. Structured data output to stdout; supports --output json|yaml|text. Requires config. Delegates to registry.show_schema.

    Preconditions:
      - ctx.config is not None

    Postconditions:
      - Structured data written to stdout in ctx.output_format
      - Exit code is 0 on success

    Errors:
      - backend_not_found (LedgerError): No backend registered with the given id
          exit_code: 1
          code: BACKEND_NOT_FOUND
      - table_not_found (LedgerError): The specified table does not exist in the backend schema
          exit_code: 1
          code: TABLE_NOT_FOUND
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: yes
    """
    ...

def cmd_schema_validate(
    ctx: CliContext,
) -> CommandResult:
    """
    Subcommand `ledger schema validate`. Validates all registered schemas against data obligations and annotation rules. Returns ALL violations, not just the first. Structured output to stdout; supports --output. Requires config. Delegates to registry.validate_schemas.

    Preconditions:
      - ctx.config is not None

    Postconditions:
      - All violations are returned (not short-circuited at the first)
      - Exit code is 0 if no error-severity violations, 1 otherwise
      - Structured violation list written to stdout

    Errors:
      - no_schemas_registered (LedgerError): No schemas have been added to the registry
          exit_code: 1
          code: NO_SCHEMAS
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: yes
    """
    ...

def cmd_migrate_plan(
    ctx: CliContext,
    component_id: str,         # regex(^[a-z][a-z0-9_-]{0,62}$)
    migration_file: str,
) -> CommandResult:
    """
    Subcommand `ledger migrate plan <component_id> <migration_file>`. Parses a SQL migration file, computes the schema diff, and runs migration gate checks. Returns full violation list with severity levels. Structured output. Requires config. Delegates to migration.plan_migration.

    Preconditions:
      - ctx.config is not None
      - migration_file points to a readable .sql file

    Postconditions:
      - A migration plan is created with a unique plan_id
      - All gate violations are returned with severity levels
      - Exit code is 0 if no error-severity gate violations, 1 otherwise

    Errors:
      - file_not_found (LedgerError): The migration file path does not exist
          exit_code: 1
          code: FILE_NOT_FOUND
      - sql_parse_error (LedgerError): The migration file contains unparseable SQL
          exit_code: 1
          code: SQL_PARSE_ERROR
      - component_not_found (LedgerError): The component_id is not registered
          exit_code: 1
          code: COMPONENT_NOT_FOUND
      - gate_violations (LedgerError): Migration gate checks produced error-severity violations
          exit_code: 1
          code: GATE_VIOLATIONS
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: yes
    """
    ...

def cmd_migrate_approve(
    ctx: CliContext,
    plan_id: str,
    review_id: str,
) -> None:
    """
    Subcommand `ledger migrate approve <plan_id> --review <id>`. Approves a previously created migration plan with a reviewer identifier. Requires config. Delegates to migration.approve_migration.

    Preconditions:
      - ctx.config is not None
      - A plan with plan_id exists and is in a pending state

    Postconditions:
      - Plan status is updated to approved
      - Approval record includes review_id and timestamp
      - Exit code is 0 on success

    Errors:
      - plan_not_found (LedgerError): No migration plan exists with the given plan_id
          exit_code: 1
          code: PLAN_NOT_FOUND
      - plan_already_approved (LedgerError): The plan has already been approved
          exit_code: 1
          code: PLAN_ALREADY_APPROVED
      - outstanding_violations (LedgerError): The plan has unresolved error-severity gate violations
          exit_code: 1
          code: UNRESOLVED_VIOLATIONS
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: yes
    """
    ...

def cmd_export(
    ctx: CliContext,
    format: ExportFormat,
    component_id: str = None,
) -> CommandResult:
    """
    Subcommand `ledger export --format <pact|arbiter|baton|sentinel> [--component <id>]`. Exports Ledger data as contracts in the target format. Structured data to stdout. Requires config. Delegates to export.export_contracts.

    Preconditions:
      - ctx.config is not None

    Postconditions:
      - Exported contract data written to stdout
      - Exit code is 0 on success

    Errors:
      - no_data_to_export (LedgerError): No schemas or backends are registered (or none match component filter)
          exit_code: 1
          code: NO_EXPORT_DATA
      - component_not_found (LedgerError): The specified component_id filter does not match any registered component
          exit_code: 1
          code: COMPONENT_NOT_FOUND
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: yes
    """
    ...

def cmd_mock(
    ctx: CliContext,
    backend_id: str,
    table: str,
    count: int,                # range(1..100000)
    seed: int = None,
    purpose: MockPurpose = default,
) -> CommandResult:
    """
    Subcommand `ledger mock <backend_id> <table> --count N [--seed S] [--purpose canary]`. Generates mock data rows for a table using faker for PII fields and stdlib random for everything else. Structured data to stdout. Requires config. Delegates to mock.generate_mock_data.

    Preconditions:
      - ctx.config is not None

    Postconditions:
      - Exactly `count` rows of mock data are generated
      - PII fields use faker; non-PII fields use stdlib random
      - If seed is provided, output is deterministic
      - Data written to stdout in ctx.output_format

    Errors:
      - backend_not_found (LedgerError): No backend registered with the given id
          exit_code: 1
          code: BACKEND_NOT_FOUND
      - table_not_found (LedgerError): The table does not exist in the backend schema
          exit_code: 1
          code: TABLE_NOT_FOUND
      - arbiter_unavailable (LedgerError): Purpose is canary but Arbiter service is unreachable
          exit_code: 1
          code: ARBITER_UNAVAILABLE
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: no
    """
    ...

def cmd_serve(
    ctx: CliContext,
) -> None:
    """
    Subcommand `ledger serve`. Starts the FastAPI+uvicorn HTTP server exposing the Ledger API. Requires config. Delegates to api.start_server. Blocks until interrupted.

    Preconditions:
      - ctx.config is not None

    Postconditions:
      - Server process is running and accepting HTTP requests
      - On KeyboardInterrupt, server shuts down cleanly with exit code 130

    Errors:
      - port_in_use (LedgerError): The configured HTTP port is already bound by another process
          exit_code: 1
          code: PORT_IN_USE
      - config_not_loaded (LedgerError): ledger.yaml not found or unparseable
          exit_code: 3
          code: CONFIG_LOAD_ERROR

    Side effects: none
    Idempotent: no
    """
    ...

def require_config(
    ctx: CliContext,
) -> None:
    """
    Decorator that ensures CliContext.config is loaded before the wrapped Click callback executes. Resolves config_path, calls config.load_config, and attaches the result to ctx.config. Raises LedgerError with exit_code CONFIG_ERROR_3 on failure.

    Preconditions:
      - ctx.config_path is a non-empty string

    Postconditions:
      - ctx.config is a valid LedgerConfig instance (not None)
      - Config was parsed from the YAML file at ctx.config_path

    Errors:
      - config_file_missing (LedgerError): No file exists at ctx.config_path
          exit_code: 3
          code: CONFIG_NOT_FOUND
      - config_parse_error (LedgerError): The file exists but contains invalid YAML
          exit_code: 3
          code: YAML_PARSE_ERROR
      - config_validation_error (LedgerError): YAML is valid but does not conform to LedgerConfig schema
          exit_code: 3
          code: CONFIG_INVALID

    Side effects: none
    Idempotent: yes
    """
    ...

def format_output(
    result: CommandResult,
    output_format: OutputFormat,
) -> str:
    """
    Pure helper that serializes a CommandResult's data payload into the requested OutputFormat string for writing to stdout. Handles text (human-readable tables/indented), json, and yaml formats.

    Preconditions:
      - result.data is not None when format is json or yaml

    Postconditions:
      - Returned string is valid JSON if output_format is json
      - Returned string is valid YAML if output_format is yaml
      - Returned string is human-readable if output_format is text

    Errors:
      - unserializable_data (LedgerError): result.data contains types that cannot be serialized to the target format
          exit_code: 1
          code: SERIALIZATION_ERROR

    Side effects: none
    Idempotent: yes
    """
    ...

def render_violations(
    violations: list,
    use_color: bool = true,
) -> str:
    """
    Pure helper that formats a list of Violations into a colored, human-readable string for stderr output. Groups violations by severity and includes counts.

    Postconditions:
      - All violations are included in the output (none skipped)
      - Violations are grouped by severity: errors first, then warnings, then info
      - Output includes a summary line with counts per severity

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['ExitCode', 'OutputFormat', 'BackendType', 'ExportFormat', 'MockPurpose', 'Severity', 'Violation', 'LedgerError', 'CliContext', 'CommandResult', 'RegistryProtocol', 'MigrationProtocol', 'ExportProtocol', 'MockProtocol', 'ApiProtocol', 'cli_main', 'SystemExit', 'cmd_init', 'cmd_backend_add', 'cmd_schema_add', 'cmd_schema_show', 'cmd_schema_validate', 'cmd_migrate_plan', 'cmd_migrate_approve', 'cmd_export', 'cmd_mock', 'cmd_serve', 'require_config', 'format_output', 'render_violations']
