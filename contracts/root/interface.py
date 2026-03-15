# === Root (root) v1 ===
#  Dependencies: api, cli, config, export, migration, mock, registry
# Ledger — Schema Registry and Data Obligation Manager. Composition root and public API facade for the distributed stack (Pact, Arbiter, Baton, Sentinel, Constrain). Defines shared cross-cutting types (enums, violation models, error hierarchy) in a leaf types module with zero internal imports, structural Protocol interfaces for each subsystem, and a bootstrap factory that loads config, constructs all subsystems, and returns a composed frozen Ledger application container. The root __init__.py re-exports __version__, shared types, protocols, and the create_ledger() factory with an explicit __all__. No business logic lives in this module — it is purely a composition and re-export layer. Import direction is strictly enforced: types.py and protocols.py import nothing from ledger.* subpackages; subpackages import only from ledger.types and ledger.protocols; subpackages never import from sibling subpackages at module level; root __init__.py imports downward only into subpackages.

# Module invariants:
#   - The root __init__.py contains NO business logic — it is purely a re-export facade with an explicit __all__
#   - types.py and protocols.py are leaf modules with zero imports from any ledger.* subpackage
#   - Subpackages (cli, registry, migration, propagation, api, mock, config, export) import shared types only from ledger.types and ledger.protocols — never from sibling subpackages at module level
#   - Root __init__.py imports downward only into subpackages — never circular
#   - All shared enums (Severity, BackendType, ExportFormat, PlanStatus, ClassificationTier) are defined exactly once in ledger.types and re-exported through root
#   - The canonical Violation model is defined exactly once in ledger.types — subpackages must not redefine it (though they may define domain-specific violation subtypes that extend it)
#   - LedgerError is the single base exception for all domain errors, defined in ledger.types
#   - All Protocol definitions live in ledger.protocols and are structural (typing.Protocol) — no ABC inheritance required
#   - create_ledger() is the sole entry point for constructing a fully wired Ledger application — no other bootstrap path exists
#   - Schema YAML content is stored verbatim throughout the entire stack — never normalized, reformatted, or reordered at any layer
#   - All validation across every subsystem returns ALL violations found — never short-circuits on the first error
#   - The propagation table is always data-driven (dict mapping), never code branches — custom annotations flow through without code changes
#   - __version__ is sourced from importlib.metadata.version('ledger') with '0.0.0-dev' fallback — never hardcoded
#   - Faker is used only for PII/FINANCIAL mock data generation; stdlib random is used for all other mock generation
#   - Missing Arbiter integration: warn and skip canary registration — never crash
#   - All timestamps throughout the system are UTC ISO 8601
#   - The Ledger container returned by create_ledger() is frozen/immutable after construction

class Severity(Enum):
    """Canonical severity levels for violations, gate results, and validation findings. Shared across all subpackages via ledger.types. Implemented as StrEnum in Python."""
    info = "info"
    warning = "warning"
    error = "error"
    critical = "critical"

class BackendType(Enum):
    """Supported backend storage types in the Ledger ecosystem. Shared canonical enum re-exported from ledger.types."""
    postgres = "postgres"
    mysql = "mysql"
    sqlite = "sqlite"
    redis = "redis"
    s3 = "s3"
    dynamodb = "dynamodb"
    kafka = "kafka"
    custom = "custom"

class ExportFormat(Enum):
    """Target contract formats for the export subsystem. Each variant names a downstream tool that consumes Ledger exports."""
    pact = "pact"
    arbiter = "arbiter"
    baton = "baton"
    sentinel = "sentinel"

class PlanStatus(Enum):
    """Lifecycle status of a migration plan. Transitions are strictly PENDING -> APPROVED or PENDING -> REJECTED; terminal states cannot transition further."""
    pending = "pending"
    approved = "approved"
    rejected = "rejected"

class ClassificationTier(Enum):
    """Fixed data classification tiers. Exactly five members; no runtime additions or removals permitted. Used to classify fields for governance, masking, and gate decisions."""
    PUBLIC = "PUBLIC"
    PII = "PII"
    FINANCIAL = "FINANCIAL"
    AUTH = "AUTH"
    COMPLIANCE = "COMPLIANCE"

class Violation:
    """Canonical violation model shared across all subpackages. Carries structured context for a single validation finding, gate result, or export issue. All validation functions collect ALL violations rather than short-circuiting on the first."""
    severity: Severity                       # required, Severity classification of this violation.
    message: str                             # required, Human-readable description of the violation.
    code: str                                # required, Machine-readable violation code, e.g. 'MISSING_PK', 'PII_NO_OBLIGATION', 'ANNOTATION_CONFLICT'.
    path: str = None                         # optional, Dot-delimited path to the offending field or entity, e.g. 'backend.users.columns.email'. Empty for global violations.
    context: dict = {}                       # optional, Additional structured key-value context for programmatic consumers. Keys and values are strings.

class LedgerError:
    """Base error type for all Ledger domain errors. Carries one or more structured Violation instances plus an exit code for CLI surfacing. All subpackage-specific errors inherit from this. Raised by domain modules, caught by CLI and API error handlers."""
    message: str                             # required, Top-level human-readable error summary.
    violations: ViolationList                # required, length(len(value) >= 1), Complete list of all violations. Never truncated to just the first.
    exit_code: int = 1                       # optional, Process exit code: 0=success, 1=domain error, 2=usage error, 3=config error, 130=keyboard interrupt.

ViolationList = list[Violation]
# A list of Violation instances. Used throughout the system to aggregate all findings.

class RegistryProtocol:
    """Structural typing.Protocol defining the interface the CLI and API expect from the registry subsystem. Concrete implementations in the registry subpackage satisfy this structurally. Methods: init(root) -> None, register_backend(root, metadata, actor) -> any, store_schema(root, backend_id, table, raw_yaml, actor) -> any, list_backends(root) -> list, list_schemas(root, backend_id) -> list, get_schema(root, backend_id, table) -> any, validate_all(root) -> any, read_changelog(root, backend_id, limit) -> list."""
    init: str                                # required, Callable[[Path], None] — initializes .ledger/ directory.
    register_backend: str                    # required, Callable[[Path, BackendMetadata, str], ChangelogEntry] — registers a backend.
    store_schema: str                        # required, Callable[[Path, str, str, bytes, str], ChangelogEntry] — stores a schema YAML verbatim.
    list_backends: str                       # required, Callable[[Path], list[BackendMetadata]] — lists all registered backends.
    list_schemas: str                        # required, Callable[[Path, str], list[SchemaRecord]] — lists schemas for a backend.
    get_schema: str                          # required, Callable[[Path, str, str], SchemaRecord | None] — gets a single schema.
    validate_all: str                        # required, Callable[[Path], ValidationResult] — validates all schemas, returns ALL violations.
    read_changelog: str                      # required, Callable[[Path, str | None, int], list[ChangelogEntry]] — reads changelog entries.

class MigrationProtocol:
    """Structural typing.Protocol defining the interface for the migration subsystem. Methods: parse_migration(sql, source_path) -> any, compute_diff(parsed, registry) -> any, evaluate_gates(diff, component_context) -> list, create_plan(diff, violations, registry, plans_dir) -> any, approve_plan(plan_id, reviewer, review_ref, rationale, plans_dir) -> any, load_plan(plan_id, plans_dir) -> any."""
    parse_migration: str                     # required, Callable[[str, str], ParsedMigration] — parses SQL migration file.
    compute_diff: str                        # required, Callable[[ParsedMigration, any], SchemaDiff] — computes enriched diff.
    evaluate_gates: str                      # required, Callable[[SchemaDiff, ComponentContext], list[GateViolation]] — runs gate checks.
    create_plan: str                         # required, Callable[[SchemaDiff, list, any, str], MigrationPlan] — creates and persists a plan.
    approve_plan: str                        # required, Callable[[str, str, str, str, str], MigrationPlan] — approves a pending plan.
    load_plan: str                           # required, Callable[[str, str], MigrationPlan] — loads a persisted plan.

class ExportProtocol:
    """Structural typing.Protocol defining the interface for the export subsystem. Methods: export_pact(component_id, propagation_table) -> any, export_arbiter(propagation_table, default_backend) -> any, export_baton(propagation_table) -> any, export_sentinel(propagation_table) -> any, yaml_dump(export_model) -> str."""
    export_pact: str                         # required, Callable[[str, list], ExportResultPact] — generates Pact contract assertions.
    export_arbiter: str                      # required, Callable[[list, str], ExportResultArbiter] — generates Arbiter classification rules.
    export_baton: str                        # required, Callable[[list], ExportResultBaton] — generates Baton egress nodes.
    export_sentinel: str                     # required, Callable[[list], ExportResultSentinel] — generates Sentinel severity mappings.
    yaml_dump: str                           # required, Callable[[any], str] — serializes export model to deterministic YAML.

class MockProtocol:
    """Structural typing.Protocol defining the interface for the mock data generator subsystem. Methods: generate_mock_records(request) -> any, resolve_seed(explicit, config) -> int."""
    generate_mock_records: str               # required, Callable[[MockGenerationRequest], MockGenerationResult] — generates mock data records.
    resolve_seed: str                        # required, Callable[[int | None, int | None], int] — resolves the seed for deterministic generation.

class ConfigProtocol:
    """Structural typing.Protocol defining the interface for the config subsystem. Methods: load_config(path) -> LedgerConfig, build_propagation_table(custom_annotations) -> dict, validate_annotation_set(annotations) -> list, get_builtin_propagation_table() -> dict, get_conflicts() -> any, get_requires() -> dict, parse_schema_file(path, propagation_table) -> SchemaFile."""
    load_config: str                         # required, Callable[[str], LedgerConfig] — loads and validates ledger.yaml.
    build_propagation_table: str             # required, Callable[[list], dict] — merges custom annotations with builtins.
    validate_annotation_set: str             # required, Callable[[list[str]], list[ConstraintViolation]] — validates annotation set against CONFLICTS/REQUIRES.
    get_builtin_propagation_table: str       # required, Callable[[], dict] — returns immutable builtin propagation table.
    get_conflicts: str                       # required, Callable[[], frozenset] — returns CONFLICTS constant.
    get_requires: str                        # required, Callable[[], dict] — returns REQUIRES constant.
    parse_schema_file: str                   # required, Callable[[str, dict], SchemaFile] — parses a single schema YAML file.

class ApiProtocol:
    """Structural typing.Protocol defining the interface for the HTTP API subsystem. Methods: create_app(config) -> any, serve_cli(port, host, config_path) -> None."""
    create_app: str                          # required, Callable[[LedgerConfig], FastAPI] — application factory for the HTTP server.
    serve_cli: str                           # required, Callable[[int, str, str], None] — starts uvicorn server, blocks until terminated.

class Ledger:
    """Frozen composed application container returned by create_ledger(). Holds references to all subsystem instances and the loaded config. This is the primary object consumers interact with after bootstrap. Satisfies all protocol interfaces by delegation to contained subsystem instances."""
    version: str                             # required, Ledger version string sourced from pyproject.toml via importlib.metadata.version('ledger'). Falls back to '0.0.0-dev' if not installed.
    config: any                              # required, Loaded and validated LedgerConfig instance from the config subsystem.
    registry: any                            # required, Registry subsystem instance satisfying RegistryProtocol.
    migration: any                           # required, Migration subsystem module/instance satisfying MigrationProtocol.
    export: any                              # required, Export subsystem module/instance satisfying ExportProtocol.
    mock: any                                # required, Mock data generator subsystem module/instance satisfying MockProtocol.
    api: any                                 # required, API subsystem module/instance satisfying ApiProtocol.

class BootstrapError:
    """Error raised when the create_ledger() bootstrap factory fails to construct the application. Inherits from LedgerError. Carries the config path and all violations encountered during bootstrap."""
    message: str                             # required, Human-readable summary of the bootstrap failure.
    violations: ViolationList                # required, All violations encountered during bootstrap (config parse errors, missing dirs, etc.).
    config_path: str                         # required, The config file path that was being loaded when the error occurred.
    exit_code: int = 3                       # optional, Exit code, defaults to 3 (config error).

class VersionInfo:
    """Structured version information for the Ledger package. Sourced from importlib.metadata at bootstrap time."""
    version: str                             # required, Semantic version string, e.g. '1.2.3'.
    python_version: str                      # required, Python interpreter version, e.g. '3.12.1'.
    pydantic_version: str                    # required, Pydantic library version, e.g. '2.5.0'.

class PublicExports:
    """Documentation type enumerating the exact names exported from ledger.__init__.__all__. This is not instantiated at runtime; it documents the public facade surface."""
    __version__: str                         # required, Package version string.
    Severity: str                            # required, Re-exported from ledger.types.
    BackendType: str                         # required, Re-exported from ledger.types.
    ExportFormat: str                        # required, Re-exported from ledger.types.
    PlanStatus: str                          # required, Re-exported from ledger.types.
    ClassificationTier: str                  # required, Re-exported from ledger.types.
    Violation: str                           # required, Re-exported from ledger.types.
    LedgerError: str                         # required, Re-exported from ledger.types.
    RegistryProtocol: str                    # required, Re-exported from ledger.protocols.
    MigrationProtocol: str                   # required, Re-exported from ledger.protocols.
    ExportProtocol: str                      # required, Re-exported from ledger.protocols.
    MockProtocol: str                        # required, Re-exported from ledger.protocols.
    ConfigProtocol: str                      # required, Re-exported from ledger.protocols.
    ApiProtocol: str                         # required, Re-exported from ledger.protocols.
    Ledger: str                              # required, Composed application container.
    create_ledger: str                       # required, Bootstrap factory function.
    get_version_info: str                    # required, Version introspection function.

def create_ledger(
    config_path: str = ledger.yaml, # length(len(value) >= 1)
) -> Ledger:
    """
    Bootstrap factory that constructs the complete Ledger application. Lifecycle: (1) resolve config_path to absolute path, (2) load and validate ledger.yaml via config.load_config, (3) initialize .ledger/ directory via registry.init if not already initialized, (4) construct subsystem references (registry, migration, export, mock, api modules), (5) resolve __version__ from importlib.metadata, (6) return a frozen Ledger container. This is the sole entry point for constructing a fully wired Ledger application instance. No schema data is loaded during bootstrap — registry loading is deferred to first access per the lazy loading requirement.

    Preconditions:
      - config_path, after resolution, points to an existing readable YAML file
      - The ledger.yaml content is valid YAML and conforms to the LedgerConfig schema
      - The schemas_dir referenced in config exists and is readable (not validated eagerly — deferred to first schema access)
      - Python 3.12+ runtime environment

    Postconditions:
      - Returned Ledger instance has all subsystem fields populated (non-None)
      - Ledger.config is a valid LedgerConfig with merged propagation table (builtins + custom annotations)
      - Ledger.version is a non-empty string (from importlib.metadata or '0.0.0-dev' fallback)
      - .ledger/ directory structure exists at the configured root (registry/, plans/, changelog.jsonl)
      - No schema YAML files have been loaded into memory — lazy loading is preserved
      - No HTTP server has been started
      - The propagation table is immutable (MappingProxyType)

    Errors:
      - config_not_found (BootstrapError): The resolved config_path does not point to an existing file
          exit_code: 3
          code: CONFIG_NOT_FOUND
      - config_parse_error (BootstrapError): The config file exists but contains invalid YAML (not parseable by yaml.safe_load)
          exit_code: 3
          code: YAML_PARSE_ERROR
      - config_validation_error (BootstrapError): YAML is valid but does not conform to LedgerConfig schema (missing required fields, type mismatches, constraint violations)
          exit_code: 3
          code: CONFIG_INVALID
      - annotation_collision (BootstrapError): A custom annotation name in ledger.yaml collides with a builtin annotation name
          exit_code: 3
          code: ANNOTATION_COLLISION
      - ledger_dir_corrupted (BootstrapError): .ledger/ directory exists but is in a partial/corrupted state (missing subdirs)
          exit_code: 3
          code: LEDGER_CORRUPTED
      - permission_denied (BootstrapError): Insufficient filesystem permissions to read config or create .ledger/ directory
          exit_code: 3
          code: PERMISSION_DENIED

    Side effects: none
    Idempotent: yes
    """
    ...

def get_version_info() -> VersionInfo:
    """
    Returns structured version information for the Ledger package, Python interpreter, and key dependencies. Reads from importlib.metadata (no file I/O). Pure introspection function.

    Postconditions:
      - Returned VersionInfo.version is a non-empty string
      - Returned VersionInfo.python_version matches the running interpreter version
      - Returned VersionInfo.pydantic_version is the installed Pydantic version string
      - If the ledger package is not installed (dev mode), version falls back to '0.0.0-dev'

    Side effects: none
    Idempotent: yes
    """
    ...

def get_version() -> str:
    """
    Returns the Ledger package version string. Convenience function that reads from importlib.metadata.version('ledger'). Falls back to '0.0.0-dev' if the package is not installed (e.g. during development). This value is re-exported as __version__ in ledger.__init__.

    Postconditions:
      - Returned string is non-empty
      - If package is installed, returned string matches the version in pyproject.toml
      - If package is not installed, returns '0.0.0-dev'

    Side effects: none
    Idempotent: yes
    """
    ...

def validate_import_graph(
    source_root: str,          # length(len(value) >= 1)
) -> ViolationList:
    """
    Development-time utility that validates the import direction rules are not violated. Scans the ledger package source files and checks: (1) types.py and protocols.py import nothing from ledger.* subpackages, (2) subpackages import only from ledger.types and ledger.protocols — never from sibling subpackages at module level, (3) root __init__.py imports only downward. Returns a list of violations. Intended for CI/test use, not production.

    Preconditions:
      - source_root points to an existing directory containing the ledger package source

    Postconditions:
      - Returned list is empty if no import rule violations are found
      - All violations are returned, not just the first
      - Each violation has code 'IMPORT_VIOLATION' and path set to the offending file and import line

    Errors:
      - source_root_not_found (LedgerError): source_root does not exist or is not a directory
          code: DIR_NOT_FOUND

    Side effects: none
    Idempotent: yes
    """
    ...

def resolve_config_path(
    explicit_path: str = None,
    env_var_name: str = LEDGER_CONFIG,
) -> str:
    """
    Pure helper that resolves the config file path using the standard resolution order: (1) explicit path parameter if non-None, (2) LEDGER_CONFIG environment variable if set and non-empty, (3) ./ledger.yaml default. Returns an absolute resolved path string. Does not validate that the file exists.

    Postconditions:
      - Returned string is an absolute filesystem path
      - If explicit_path is non-empty, returned path is the absolute form of explicit_path
      - If explicit_path is empty and env var is set, returned path is the absolute form of the env var value
      - If neither is available, returned path is the absolute form of './ledger.yaml'

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['Severity', 'BackendType', 'ExportFormat', 'PlanStatus', 'ClassificationTier', 'Violation', 'LedgerError', 'ViolationList', 'RegistryProtocol', 'MigrationProtocol', 'ExportProtocol', 'MockProtocol', 'ConfigProtocol', 'ApiProtocol', 'Ledger', 'BootstrapError', 'VersionInfo', 'PublicExports', 'create_ledger', 'get_version_info', 'get_version', 'validate_import_graph', 'resolve_config_path']
