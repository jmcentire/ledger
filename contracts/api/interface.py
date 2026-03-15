# === HTTP API Server (api) v2 ===
#  Dependencies: registry, migration, export, mock, config, propagation
# FastAPI application exposing all Ledger operations as REST endpoints. Provides backend registration (POST /backends), schema registration and query (POST /schemas, GET /schemas/{backend_id}, GET /schemas/{backend_id}/{table}), schema validation (POST /schemas/validate), migration planning and approval (POST /migrations/plan, POST /migrations/{plan_id}/approve), data export (GET /export/{format}), mock data generation (POST /mock/{backend_id}/{table}), annotation query (GET /annotations), and health check (GET /health). Returns JSON responses with appropriate HTTP status codes. Application factory pattern via create_app(config) -> FastAPI. Mounted on uvicorn single-worker, configurable port (default 7701). Lazy schema loading — schemas loaded on demand via Depends(), not at startup — to meet the 3-second startup constraint. Sync route handlers (def, not async def) since all I/O is file-based. Migration plans stored in-memory with UUID IDs and configurable TTL (default 1 hour). The `ledger serve` CLI command starts this server.

# Module invariants:
#   - All HTTP error responses use the ErrorResponse model with a complete list of violations — never just the first violation
#   - Schema YAML content is stored verbatim — never normalized, reformatted, or reordered on ingestion or retrieval
#   - POST creation endpoints return 201 with Location header; identical re-registration returns 200; conflicting re-registration returns 409
#   - Schema validation endpoints return ALL violations across all severity levels in a single response
#   - Migration gate failures return 200/201 with gate_result.passed=false — they are not HTTP errors
#   - Migration plans are stored in-memory only and do not survive server restarts
#   - Expired migration plans are cleaned up lazily on access — no background cleanup thread
#   - Health endpoint (GET /health) has zero heavy dependencies and responds without triggering schema loading
#   - Schema registry is lazily initialized on first request, not at app startup, to meet the 3-second startup constraint
#   - All sync route handlers use def (not async def) since all I/O is file-based
#   - Server runs with exactly 1 uvicorn worker
#   - PII-annotated columns in mock generation use faker; all other columns use stdlib random
#   - Missing Arbiter integration: warn and skip canary registration, never crash
#   - Invalid YAML: fail with clear parse error and file path in the violation message
#   - Plan IDs are UUID4 strings
#   - Export: CSV and YAML formats use PlainTextResponse; JSON uses JSONResponse

class HttpMethod(Enum):
    """HTTP methods used by API routes."""
    GET = "GET"
    POST = "POST"

class Severity(Enum):
    """Severity level for validation violations and migration gate results."""
    error = "error"
    warning = "warning"
    info = "info"

class ExportFormat(Enum):
    """Supported export formats, validated via StrEnum path parameter."""
    json = "json"
    csv = "csv"
    yaml = "yaml"

class PlanStatus(Enum):
    """Lifecycle status of a migration plan."""
    pending = "pending"
    approved = "approved"
    expired = "expired"

class Violation:
    """A single validation or gate violation with structured context."""
    field: str                               # required, Dot-path to the offending field, or empty string for global violations.
    message: str                             # required, Human-readable description of the violation.
    severity: Severity                       # required, Severity classification of this violation.
    code: str                                # required, Machine-readable violation code, e.g. 'MISSING_PK', 'PII_NO_OBLIGATION'.

class ErrorResponse:
    """Standard error response body returned for all error HTTP status codes. Contains full list of violations, never just the first."""
    error: str                               # required, Top-level error category, e.g. 'SchemaValidationError', 'ResourceNotFoundError'.
    detail: str                              # required, Human-readable summary of the error.
    violations: list                         # required, Complete list of all violations — all are returned, not just the first.

class HealthResponse:
    """Response body for GET /health. Zero heavy dependencies — no registry load."""
    status: str                              # required, Always 'ok' if the server is running.
    version: str                             # required, Ledger version string.
    port: int                                # required, Port the server is listening on.

class BackendRegistrationRequest:
    """Request body for POST /backends to register a new backend."""
    backend_id: str                          # required, regex(^[a-z][a-z0-9_-]{1,62}[a-z0-9]$), Unique identifier for the backend (e.g. 'pact', 'arbiter').
    display_name: str                        # required, length(1..256), Human-readable name for the backend.
    description: str = None                  # optional, Optional description of the backend's purpose.

class BackendRegistrationResponse:
    """Response body for successful backend registration (201 Created)."""
    backend_id: str                          # required, The registered backend's identifier.
    display_name: str                        # required, Human-readable name.
    created: bool                            # required, True if newly created, false if identical re-registration (200).

class SchemaRegistrationRequest:
    """Request body for POST /schemas. The yaml_content field carries the verbatim YAML text of the schema — it is stored as-is, never normalized, reformatted, or reordered."""
    backend_id: str                          # required, Backend this schema belongs to.
    table_name: str                          # required, regex(^[a-z][a-z0-9_.]{0,126}[a-z0-9]$), Fully qualified table name.
    yaml_content: str                        # required, length(1..1048576), Verbatim YAML text of the schema definition. Stored exactly as provided — no normalization, reformatting, or reordering on ingestion.
    version: str                             # required, regex(^\d+\.\d+\.\d+$), Semantic version of this schema (e.g. '1.0.0').

class SchemaRegistrationResponse:
    """Response body for successful schema registration."""
    backend_id: str                          # required, Backend the schema belongs to.
    table_name: str                          # required, Registered table name.
    version: str                             # required, Registered schema version.
    created: bool                            # required, True if newly created (201), false if identical re-registration (200).

class SchemaDetail:
    """Full schema detail returned by GET /schemas/{backend_id}/{table}."""
    backend_id: str                          # required, Owning backend.
    table_name: str                          # required, Table name.
    version: str                             # required, Current schema version.
    yaml_content: str                        # required, Verbatim YAML as originally registered.
    columns: list                            # required, Parsed column definitions.
    annotations: list                        # required, Data obligation annotations applied to this schema.

class ColumnInfo:
    """Parsed column metadata from a schema definition."""
    name: str                                # required, Column name.
    data_type: str                           # required, Column data type as declared in YAML.
    nullable: bool                           # required, Whether the column allows NULL values.
    primary_key: bool                        # required, Whether this column is part of the primary key.
    annotations: list = []                   # optional, Annotation tags on this column (e.g. 'pii:email', 'obligation:retain_30d').

class AnnotationEntry:
    """A single annotation entry with propagation metadata."""
    column: str                              # required, Column this annotation applies to.
    tag: str                                 # required, Annotation tag (e.g. 'pii:email').
    source: str                              # required, Where the annotation originated (e.g. 'schema_definition', 'propagation_rule').
    propagated: bool                         # required, True if this annotation was propagated from another schema.

class SchemaListResponse:
    """Response for GET /schemas/{backend_id} listing all schemas for a backend."""
    backend_id: str                          # required, The queried backend.
    schemas: list                            # required, List of schema summaries.

class SchemaSummary:
    """Summary of a registered schema without full YAML content."""
    table_name: str                          # required, Table name.
    version: str                             # required, Current version.
    column_count: int                        # required, Number of columns in the schema.
    annotation_count: int                    # required, Number of annotations on this schema.

class SchemaValidationRequest:
    """Request body for POST /schemas/validate — validates without registering."""
    yaml_content: str                        # required, length(1..1048576), Verbatim YAML text of the schema to validate.

class SchemaValidationResponse:
    """Response body for POST /schemas/validate. Returns all violations, not just the first."""
    valid: bool                              # required, True if schema has zero error-severity violations.
    violations: list                         # required, All violations found, across all severity levels.

class MigrationPlanRequest:
    """Request body for POST /migrations/plan."""
    backend_id: str                          # required, Backend the migration targets.
    table_name: str                          # required, Table being migrated.
    sql_content: str                         # required, length(1..5242880), Raw SQL migration script.

class MigrationDiff:
    """A single diff entry describing one change in a migration plan."""
    operation: str                           # required, Type of change: 'add_column', 'drop_column', 'alter_type', 'rename_column', 'add_index', 'drop_index', etc.
    target: str                              # required, Affected column or constraint name.
    details: dict                            # required, Operation-specific detail map (e.g. old_type, new_type for alter_type).

class GateResult:
    """Result of migration gate checks. Includes all violations with severity levels."""
    passed: bool                             # required, True if no error-severity violations — warnings/info do not block.
    violations: list                         # required, Full list of all gate violations across all severity levels.

class MigrationPlanResponse:
    """Response body for POST /migrations/plan (201 Created)."""
    plan_id: str                             # required, UUID identifier for the migration plan.
    backend_id: str                          # required, Target backend.
    table_name: str                          # required, Target table.
    status: PlanStatus                       # required, Initial status, always 'pending'.
    diffs: list                              # required, Parsed migration diffs.
    gate_result: GateResult                  # required, Gate check results including all violations.
    expires_at: str                          # required, ISO 8601 timestamp when this plan expires (default: creation + 1 hour).

class MigrationApproveResponse:
    """Response body for POST /migrations/{plan_id}/approve."""
    plan_id: str                             # required, The approved plan's UUID.
    status: PlanStatus                       # required, Updated status, 'approved' on success.
    approved_at: str                         # required, ISO 8601 timestamp of approval.

class ExportResponse:
    """Metadata wrapper for GET /export/{format}. Actual content returned via appropriate Response subclass (JSONResponse, PlainTextResponse for CSV/YAML)."""
    format: ExportFormat                     # required, The export format used.
    schema_count: int                        # required, Number of schemas included in the export.
    content: str                             # required, Serialized export content (JSON string, CSV text, or YAML text).

class MockGenerationRequest:
    """Request body for POST /mock/{backend_id}/{table}."""
    row_count: int                           # required, range(1..10000), Number of mock rows to generate.
    seed: int = None                         # optional, Random seed for reproducibility. If not provided, a random seed is used.

class MockGenerationResponse:
    """Response body for POST /mock/{backend_id}/{table}."""
    backend_id: str                          # required, Backend of the schema used.
    table_name: str                          # required, Table mock data was generated for.
    row_count: int                           # required, Actual number of rows generated.
    columns: list                            # required, Column names in order.
    rows: list                               # required, List of rows, each a list of values matching columns order.
    seed: int                                # required, Seed that was used for generation (for reproducibility).

class AnnotationsResponse:
    """Response body for GET /annotations."""
    annotations: list                        # required, All annotations across all backends and tables.
    total_count: int                         # required, Total number of annotations returned.

class LedgerConfig:
    """Configuration loaded from ledger.yaml. Lightweight parse at app creation, not at module import."""
    port: int                                # required, range(1..65535), Port for the HTTP server.
    schema_dir: str                          # required, Directory path where schema YAML files are stored.
    plan_ttl_seconds: int                    # required, range(60..86400), TTL for in-memory migration plans in seconds.
    arbiter_url: str = None                  # optional, URL for the Arbiter API. Empty string means Arbiter integration is disabled.

class InMemoryPlanStore:
    """In-memory storage for migration plans. Plans do not survive server restarts. Expired plans are cleaned up lazily on access."""
    plans: dict                              # required, Map from plan_id (UUID string) to stored plan data.
    ttl_seconds: int                         # required, TTL for plans, from LedgerConfig.plan_ttl_seconds.

def create_app(
    config: LedgerConfig,
) -> any:
    """
    Application factory: creates and configures a FastAPI instance with all routers mounted, exception handlers registered, and lazy-loading dependencies configured. This is the sole entry point for constructing the Ledger HTTP server. Config is parsed once (lightweight YAML read of ledger.yaml). No schema data is loaded — lazy loading via Depends() defers that to first request.

    Preconditions:
      - config is a valid LedgerConfig instance with all required fields populated
      - config.schema_dir points to an existing readable directory (not validated at create_app time — validated lazily on first schema access)

    Postconditions:
      - Returned FastAPI app has all 7 routers mounted: backends, schemas, migrations, export, mock, annotations, health
      - Exception handlers registered for SchemaValidationError, MigrationGateError, ResourceNotFoundError, ConflictError
      - No schema data has been loaded into memory — registry is lazy
      - App state contains config reference and uninitialized registry dependency

    Errors:
      - invalid_config (ValidationError): config fails Pydantic validation
      - yaml_parse_error (ValueError): ledger.yaml is malformed (only if config was loaded from file in this call path)
          detail: Clear parse error message with file path

    Side effects: none
    Idempotent: yes
    """
    ...

def handle_health() -> HealthResponse:
    """
    GET /health handler. Returns server status with zero heavy dependencies — no registry loading, no file I/O. Must respond instantly even if no schemas have been registered.

    Postconditions:
      - Response status is always 'ok' if the server is running
      - No schema data loaded as a side effect of this call

    Side effects: none
    Idempotent: yes
    """
    ...

def handle_register_backend(
    body: BackendRegistrationRequest,
) -> BackendRegistrationResponse:
    """
    POST /backends handler. Registers a new backend. Returns 201 with Location header on new creation, 200 if identical re-registration, 409 if conflicting re-registration (same backend_id, different display_name or description).

    Preconditions:
      - body passes Pydantic validation including backend_id regex and display_name length

    Postconditions:
      - If 201: backend is registered and retrievable; Location header set to /backends/{backend_id}
      - If 200: no state change (identical re-registration)
      - If 409: no state change (conflicting registration rejected)

    Errors:
      - conflict (ConflictError): backend_id already exists with different display_name or description
          http_status: 409
      - validation_error (ValidationError): Request body fails Pydantic validation
          http_status: 422

    Side effects: Mutates in-memory backend registry
    Idempotent: yes
    """
    ...

def handle_register_schema(
    body: SchemaRegistrationRequest,
) -> SchemaRegistrationResponse:
    """
    POST /schemas handler. Registers a schema for a backend. The yaml_content is stored verbatim — never normalized, reformatted, or reordered. Returns 201 with Location header on new creation, 200 if identical re-registration, 409 if version conflict.

    Preconditions:
      - body passes Pydantic validation
      - body.backend_id references a registered backend
      - body.yaml_content is valid YAML (parsed to verify, but stored verbatim)

    Postconditions:
      - If 201: schema is registered, yaml_content stored verbatim, Location header set to /schemas/{backend_id}/{table_name}
      - If 200: no state change (identical re-registration with same version and yaml_content)
      - If 409: no state change (same backend_id+table_name+version but different yaml_content)
      - Annotation propagation is triggered for the newly registered schema

    Errors:
      - backend_not_found (ResourceNotFoundError): body.backend_id does not reference a registered backend
          http_status: 404
      - conflict (ConflictError): Schema with same backend_id, table_name, version already exists with different content
          http_status: 409
      - invalid_yaml (SchemaValidationError): yaml_content is not valid YAML
          http_status: 422
      - schema_validation_failed (SchemaValidationError): Parsed schema fails structural validation
          http_status: 422
      - validation_error (ValidationError): Request body fails Pydantic field validation
          http_status: 422

    Side effects: Mutates in-memory schema registry, Reads schema YAML files from disk on lazy init, Triggers annotation propagation
    Idempotent: yes
    """
    ...

def handle_get_schemas_for_backend(
    backend_id: str,
) -> SchemaListResponse:
    """
    GET /schemas/{backend_id} handler. Returns list of all schemas registered under a backend. Triggers lazy loading of registry if not yet initialized.

    Preconditions:
      - backend_id is a non-empty string

    Postconditions:
      - Response contains all schemas for the given backend, possibly empty list if backend exists but has no schemas
      - Schema registry is initialized (lazy load triggered if first access)

    Errors:
      - backend_not_found (ResourceNotFoundError): backend_id does not reference a registered backend
          http_status: 404

    Side effects: May trigger lazy registry initialization
    Idempotent: yes
    """
    ...

def handle_get_schema_detail(
    backend_id: str,
    table_name: str,
) -> SchemaDetail:
    """
    GET /schemas/{backend_id}/{table} handler. Returns full schema detail including verbatim YAML, parsed columns, and annotations.

    Preconditions:
      - backend_id and table_name are non-empty strings

    Postconditions:
      - Response contains full schema detail with verbatim yaml_content as originally registered
      - Annotations include both direct and propagated entries

    Errors:
      - backend_not_found (ResourceNotFoundError): backend_id does not reference a registered backend
          http_status: 404
      - schema_not_found (ResourceNotFoundError): No schema registered for this backend_id + table_name combination
          http_status: 404

    Side effects: May trigger lazy registry initialization
    Idempotent: yes
    """
    ...

def handle_validate_schema(
    body: SchemaValidationRequest,
) -> SchemaValidationResponse:
    """
    POST /schemas/validate handler. Validates a schema YAML without registering it. Always returns 200 with validation results including all violations (not just the first). The valid field is true only if there are zero error-severity violations.

    Preconditions:
      - body passes Pydantic field validation (yaml_content is non-empty, within size limit)

    Postconditions:
      - Response.valid is true iff zero Violation entries have severity == 'error'
      - All violations are returned, not just the first
      - No schema data is persisted — validation only
      - No state mutation of any kind

    Errors:
      - validation_error (ValidationError): Request body fails Pydantic field validation
          http_status: 422

    Side effects: none
    Idempotent: yes
    """
    ...

def handle_create_migration_plan(
    body: MigrationPlanRequest,
) -> MigrationPlanResponse:
    """
    POST /migrations/plan handler. Parses a SQL migration script, computes diffs against the current registered schema, runs gate checks, and stores the plan in memory with a UUID and TTL. Returns 201 with the plan including all gate violations. Gate failure does NOT produce an error status — it returns 201 with gate_result.passed=false and the full violation list.

    Preconditions:
      - body passes Pydantic field validation
      - body.backend_id references a registered backend
      - body.table_name references a registered schema under that backend

    Postconditions:
      - A new plan with UUID plan_id is stored in InMemoryPlanStore with status 'pending'
      - Plan expires_at is set to now + config.plan_ttl_seconds
      - gate_result contains ALL violations with severity levels — never truncated
      - Expired plans encountered during this call are cleaned up (lazy eviction)

    Errors:
      - backend_not_found (ResourceNotFoundError): backend_id does not reference a registered backend
          http_status: 404
      - schema_not_found (ResourceNotFoundError): No schema registered for backend_id + table_name
          http_status: 404
      - sql_parse_error (SchemaValidationError): SQL content cannot be parsed into migration diffs
          http_status: 422
      - validation_error (ValidationError): Request body fails Pydantic field validation
          http_status: 422

    Side effects: Creates in-memory migration plan, Evicts expired plans
    Idempotent: no
    """
    ...

def handle_approve_migration_plan(
    plan_id: str,
) -> MigrationApproveResponse:
    """
    POST /migrations/{plan_id}/approve handler. Approves a pending migration plan. Only plans with status 'pending' and gate_result.passed=true can be approved.

    Preconditions:
      - plan_id is a valid UUID string

    Postconditions:
      - Plan status is updated to 'approved' in InMemoryPlanStore
      - approved_at timestamp is set to current UTC time in ISO 8601

    Errors:
      - plan_not_found (ResourceNotFoundError): No plan exists with the given plan_id, or plan has expired
          http_status: 404
      - plan_not_pending (ConflictError): Plan status is not 'pending' (already approved or expired)
          http_status: 409
      - gate_failed (MigrationGateError): Plan gate_result.passed is false — cannot approve a plan that failed gate checks
          http_status: 422

    Side effects: Mutates in-memory plan status
    Idempotent: yes
    """
    ...

def handle_export(
    format: ExportFormat,
) -> ExportResponse:
    """
    GET /export/{format} handler. Exports all registered schemas in the specified format. JSON format returns JSONResponse; CSV and YAML formats return PlainTextResponse with appropriate content type.

    Preconditions:
      - format is a valid ExportFormat variant (enforced by StrEnum path validation)

    Postconditions:
      - Response contains all currently registered schemas serialized in the requested format
      - JSON format: Content-Type application/json
      - CSV format: Content-Type text/csv via PlainTextResponse
      - YAML format: Content-Type text/yaml via PlainTextResponse
      - No state mutation of any kind

    Errors:
      - invalid_format (ValidationError): format is not a valid ExportFormat variant
          http_status: 422

    Side effects: May trigger lazy registry initialization
    Idempotent: yes
    """
    ...

def handle_generate_mock(
    backend_id: str,
    table_name: str,
    body: MockGenerationRequest,
) -> MockGenerationResponse:
    """
    POST /mock/{backend_id}/{table} handler. Generates mock data rows based on a registered schema's column definitions. Uses faker for PII-annotated columns and stdlib random for all others, per project standards.

    Preconditions:
      - body passes Pydantic validation (row_count 1-10000)
      - backend_id references a registered backend
      - table_name references a registered schema under that backend

    Postconditions:
      - Response contains exactly body.row_count rows
      - Each row has values matching the schema's column order
      - PII-annotated columns use faker for realistic mock data
      - Non-PII columns use stdlib random
      - If seed is provided, output is deterministically reproducible
      - No state mutation of any kind

    Errors:
      - backend_not_found (ResourceNotFoundError): backend_id does not reference a registered backend
          http_status: 404
      - schema_not_found (ResourceNotFoundError): No schema registered for backend_id + table_name
          http_status: 404
      - validation_error (ValidationError): Request body fails Pydantic field validation (row_count out of range)
          http_status: 422

    Side effects: May trigger lazy registry initialization
    Idempotent: yes
    """
    ...

def handle_get_annotations() -> AnnotationsResponse:
    """
    GET /annotations handler. Returns all data obligation annotations across all backends and tables, including both directly declared and propagated annotations.

    Postconditions:
      - Response contains all annotations from all registered schemas
      - Each annotation indicates whether it was propagated or directly declared
      - total_count matches len(annotations)

    Side effects: May trigger lazy registry initialization
    Idempotent: yes
    """
    ...

def serve_cli(
    port: int = 7701,
    host: str = 127.0.0.1,
    config_path: str = ledger.yaml,
) -> None:
    """
    Click command handler for `ledger serve`. Loads config from ledger.yaml, creates the FastAPI app via create_app(), and starts uvicorn with workers=1. This is the CLI entry point, not an HTTP handler.

    Preconditions:
      - config_path points to a readable YAML file
      - port is a valid port number (1-65535)

    Postconditions:
      - uvicorn server is running on host:port with workers=1
      - Server blocks until interrupted (Ctrl+C / SIGINT)

    Errors:
      - config_not_found (FileNotFoundError): config_path does not exist or is not readable
          detail: File path included in error message
      - invalid_yaml (ValueError): ledger.yaml is malformed YAML
          detail: Clear parse error with file path
      - port_in_use (OSError): The specified port is already bound by another process
          detail: Address already in use

    Side effects: Reads ledger.yaml config file, Starts HTTP server on specified port, Blocks until terminated
    Idempotent: no
    """
    ...

def get_registry() -> any:
    """
    FastAPI dependency (Depends) that provides the schema registry. Lazy-initialized on first call — does not load schemas until a request actually needs them. Subsequent calls return the cached instance. This is the key mechanism for meeting the 3-second startup constraint.

    Preconditions:
      - create_app has been called and app state contains config

    Postconditions:
      - Returns initialized schema registry instance
      - On first call: registry is created and schema directory is scanned
      - On subsequent calls: cached registry returned with no additional I/O

    Errors:
      - schema_dir_not_found (ResourceNotFoundError): config.schema_dir does not exist or is not readable
          http_status: 500
          detail: Schema directory not found
      - schema_dir_unreadable (OSError): Filesystem permission error reading schema_dir
          detail: Permission denied

    Side effects: Reads schema directory on first invocation
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['HttpMethod', 'Severity', 'ExportFormat', 'PlanStatus', 'Violation', 'ErrorResponse', 'HealthResponse', 'BackendRegistrationRequest', 'BackendRegistrationResponse', 'SchemaRegistrationRequest', 'SchemaRegistrationResponse', 'SchemaDetail', 'ColumnInfo', 'AnnotationEntry', 'SchemaListResponse', 'SchemaSummary', 'SchemaValidationRequest', 'SchemaValidationResponse', 'MigrationPlanRequest', 'MigrationDiff', 'GateResult', 'MigrationPlanResponse', 'MigrationApproveResponse', 'ExportResponse', 'MockGenerationRequest', 'MockGenerationResponse', 'AnnotationsResponse', 'LedgerConfig', 'InMemoryPlanStore', 'create_app', 'ValidationError', 'handle_health', 'handle_register_backend', 'ConflictError', 'handle_register_schema', 'ResourceNotFoundError', 'SchemaValidationError', 'handle_get_schemas_for_backend', 'handle_get_schema_detail', 'handle_validate_schema', 'handle_create_migration_plan', 'handle_approve_migration_plan', 'MigrationGateError', 'handle_export', 'handle_generate_mock', 'handle_get_annotations', 'serve_cli', 'get_registry']
