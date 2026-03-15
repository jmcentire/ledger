# === Registry & Schema Store (registry) v2 ===
# Manages the .ledger/ directory structure (init), backend registration, and schema storage. `init` creates .ledger/registry/, .ledger/plans/, and .ledger/changelog.jsonl. Backend registration stores backend metadata (id, type, owner component) as flat YAML under .ledger/registry/, enforcing unique backend IDs and that no two components own the same backend. Schema store accepts schema YAML files and stores them verbatim (byte-for-byte) under .ledger/registry/<backend_id>/. Provides read APIs: list all schemas, get schemas by backend_id, get schema by backend_id+table. Schema validation checks all registered schemas for annotation conflict pairs, REQUIRES satisfaction (warnings vs errors), and backend ownership exclusivity — returns ALL violations. Appends entries to the append-only changelog (JSONL) for every backend registration and schema addition with timestamp, actor, change type, and affected backend/table/field.

# Module invariants:
#   - Schema YAML is stored verbatim — never normalized, reformatted, or reordered on ingestion
#   - Backend IDs are unique across the entire registry; enforced both by scan-on-write and filesystem path uniqueness
#   - No two components may own the same backend (ownership exclusivity)
#   - The changelog at .ledger/changelog.jsonl is append-only; entries are never modified or deleted
#   - Changelog sequence numbers are monotonically increasing starting at 1 with no gaps
#   - All timestamps are UTC ISO 8601
#   - validate_all returns ALL violations across all schemas — it never short-circuits on the first error
#   - ValidationResult.valid is True if and only if there are zero error-severity violations; warnings do not invalidate
#   - init is idempotent: calling it on an already-initialized .ledger/ with all paths intact is a no-op
#   - All public functions require .ledger/ to be initialized and raise LedgerNotInitializedError otherwise
#   - backend_id must match ^[a-z][a-z0-9_-]{1,62}[a-z0-9]$ (3-64 chars, lowercase, starts with letter, ends with alphanumeric)

class BackendType(Enum):
    """Supported backend types in the Ledger ecosystem."""
    postgres = "postgres"
    mysql = "mysql"
    sqlite = "sqlite"
    redis = "redis"
    s3 = "s3"
    dynamodb = "dynamodb"
    kafka = "kafka"
    custom = "custom"

class ViolationSeverity(Enum):
    """Severity level for schema validation violations."""
    error = "error"
    warning = "warning"

class ChangeType(Enum):
    """Types of changes recorded in the append-only changelog."""
    backend_registered = "backend_registered"
    schema_added = "schema_added"

Path = primitive  # Filesystem path (pathlib.Path). Represents an absolute or relative directory/file path.

datetime = primitive  # UTC datetime (datetime.datetime with tzinfo=UTC). All timestamps are ISO 8601 UTC.

class BackendMetadata:
    """Frozen Pydantic v2 model representing registered backend metadata. Stored as flat YAML under .ledger/registry/<backend_id>.yaml."""
    backend_id: str                          # required, regex(^[a-z][a-z0-9_-]{1,62}[a-z0-9]$), length(3..64), Unique identifier for the backend. Must match pattern ^[a-z][a-z0-9_-]{1,62}[a-z0-9]$ (3-64 chars, lowercase alphanumeric with hyphens/underscores, must start with letter and end with alphanumeric).
    backend_type: BackendType                # required, The type of the backend system (e.g. postgres, redis, s3).
    owner_component: str                     # required, length(1..128), The component that owns this backend. No two components may own the same backend.
    registered_at: datetime                  # required, UTC timestamp of when this backend was registered. ISO 8601 format.

class SchemaRecord:
    """Pydantic v2 model holding a stored schema. raw_content preserves verbatim bytes; parsed_content is the YAML-parsed dict for query use."""
    backend_id: str                          # required, The backend this schema belongs to.
    table_name: str                          # required, length(1..256), The table/entity name within the backend.
    raw_content: bytes                       # required, Verbatim byte-for-byte content of the schema YAML file as ingested. Never normalized, reformatted, or reordered.
    parsed_content: dict                     # required, YAML-parsed dictionary representation of raw_content, cached for query and validation use.
    stored_at: datetime                      # required, UTC timestamp of when this schema was stored.

class Violation:
    """Frozen Pydantic v2 model representing a single schema validation violation. Validation always collects ALL violations rather than short-circuiting on the first."""
    severity: ViolationSeverity              # required, Whether this violation is a blocking error or an advisory warning.
    rule: str                                # required, Machine-readable rule identifier (e.g. 'annotation_conflict', 'requires_unsatisfied', 'ownership_exclusive').
    backend_id: str                          # required, The backend where the violation was detected.
    table: str = None                        # optional, Table name if the violation is table-scoped. Empty string if backend-level.
    field: str = None                        # optional, Field name if the violation is field-scoped. Empty string if table-level or backend-level.
    message: str                             # required, Human-readable description of the violation.

class ValidationResult:
    """Pydantic v2 model containing the full set of violations from validate_all(). The 'valid' property is True only when there are zero error-severity violations (warnings are acceptable)."""
    violations: list[Violation]              # required, Complete list of all detected violations across all registered schemas.
    valid: bool                              # required, Computed property: True if no violations have severity='error'. Warnings alone do not invalidate.

class ChangelogEntry:
    """Frozen Pydantic v2 model for a single line in the append-only JSONL changelog (.ledger/changelog.jsonl). Each entry records a mutation to the registry."""
    timestamp: datetime                      # required, UTC timestamp (ISO 8601) of when the change occurred.
    sequence: int                            # required, range(1..), Monotonically increasing sequence number for changelog ordering. Starts at 1.
    actor: str                               # required, length(1..256), Identity of the user or system that performed the change (e.g. CLI user, CI pipeline).
    change_type: ChangeType                  # required, The type of change: 'backend_registered' or 'schema_added'.
    backend_id: str                          # required, The backend affected by this change.
    table: str = None                        # optional, Table name if the change is schema-scoped. Empty string for backend-level changes.
    field: str = None                        # optional, Field name if the change is field-scoped. Empty string otherwise.
    detail: str = None                       # optional, Optional human-readable detail about the change.

class LedgerError:
    """Base exception for all Ledger registry errors. All custom exceptions inherit from this."""
    message: str                             # required, Human-readable error message.

class LedgerNotInitializedError:
    """Raised when an operation requires an initialized .ledger/ directory but none exists. Inherits from LedgerError."""
    message: str                             # required, Error message including the expected path.
    root: str                                # required, The root path where .ledger/ was expected.

class LedgerCorruptedError:
    """Raised when .ledger/ exists but is in a partial or corrupted state (e.g. missing required subdirectories). Inherits from LedgerError."""
    message: str                             # required, Error message describing the corruption.
    missing_paths: list[str]                 # required, List of expected paths that are missing or corrupted.

class DuplicateBackendError:
    """Raised when attempting to register a backend with an ID that already exists. Inherits from LedgerError."""
    message: str                             # required
    backend_id: str                          # required, The duplicate backend_id.

class OwnershipConflictError:
    """Raised when a backend registration would violate ownership exclusivity (no two components may own the same backend). Inherits from LedgerError."""
    message: str                             # required
    backend_id: str                          # required, The contested backend_id.
    existing_owner: str                      # required, The component that currently owns this backend.
    attempted_owner: str                     # required, The component that attempted to claim ownership.

class BackendNotFoundError:
    """Raised when referencing a backend_id that has not been registered. Inherits from LedgerError."""
    message: str                             # required
    backend_id: str                          # required, The backend_id that was not found.

class SchemaParseError:
    """Raised when schema YAML bytes cannot be parsed. Includes the file path and parse error details. Inherits from LedgerError."""
    message: str                             # required
    backend_id: str                          # required, The backend the schema was being stored for.
    table: str                               # required, The table name of the schema.
    parse_error: str                         # required, The underlying YAML parse error message.

def init(
    root: Path,
) -> None:
    """
    Initializes the .ledger/ directory structure at the given root path. Creates .ledger/registry/, .ledger/plans/, and .ledger/changelog.jsonl. Idempotent: if all required paths already exist, returns without error. Raises LedgerCorruptedError if .ledger/ exists but is in a partial or inconsistent state (e.g. some subdirectories missing or changelog is not a file).

    Preconditions:
      - root must be an existing directory with write permissions
      - If .ledger/ exists at root, it must either be fully intact (all subdirs + changelog present) or not exist at all — partial state triggers LedgerCorruptedError

    Postconditions:
      - .ledger/ directory exists at root
      - .ledger/registry/ directory exists
      - .ledger/plans/ directory exists
      - .ledger/changelog.jsonl file exists (created empty if new)

    Errors:
      - corrupted_state (LedgerCorruptedError): .ledger/ directory exists but is missing required subdirectories or changelog file
          missing_paths: List of paths that should exist but do not
      - permission_denied (LedgerError): root directory is not writable
          message: Cannot write to root directory: <root>

    Side effects: Creates directories .ledger/registry/ and .ledger/plans/, Creates empty file .ledger/changelog.jsonl if it does not exist
    Idempotent: yes
    """
    ...

def register_backend(
    root: Path,
    metadata: BackendMetadata,
    actor: str,                # length(1..256)
) -> ChangelogEntry:
    """
    Registers a new backend by writing its metadata as flat YAML to .ledger/registry/<backend_id>.yaml. Enforces unique backend_id (no duplicate registrations) and ownership exclusivity (no two components may own the same backend). On success, appends a 'backend_registered' entry to the changelog and returns it.

    Preconditions:
      - .ledger/ must be initialized at root (init has been called)
      - metadata.backend_id must not already be registered
      - No other backend may already be owned by a different component with the same backend_id

    Postconditions:
      - File .ledger/registry/<backend_id>.yaml exists with the metadata serialized as YAML
      - A 'backend_registered' ChangelogEntry has been appended to .ledger/changelog.jsonl
      - Returned ChangelogEntry has change_type='backend_registered', matching backend_id, and monotonically increasing sequence number

    Errors:
      - not_initialized (LedgerNotInitializedError): .ledger/ directory does not exist at root
          root: The root path where .ledger/ was expected
      - duplicate_backend (DuplicateBackendError): A backend with the same backend_id is already registered
          backend_id: The duplicate backend_id
      - ownership_conflict (OwnershipConflictError): The backend_id is already owned by a different component
          backend_id: The contested backend_id
          existing_owner: Current owner component
          attempted_owner: Attempting owner component

    Side effects: Writes YAML file to .ledger/registry/<backend_id>.yaml, Appends JSONL entry to .ledger/changelog.jsonl
    Idempotent: no
    """
    ...

def store_schema(
    root: Path,
    backend_id: str,           # regex(^[a-z][a-z0-9_-]{1,62}[a-z0-9]$)
    table: str,                # length(1..256)
    raw_yaml: bytes,
    actor: str,                # length(1..256)
) -> ChangelogEntry:
    """
    Stores a schema YAML file verbatim (byte-for-byte) under .ledger/registry/<backend_id>/<table>.yaml. Validates that the raw bytes are parseable as YAML before storing. The backend must already be registered. On success, appends a 'schema_added' entry to the changelog and returns it.

    Preconditions:
      - .ledger/ must be initialized at root
      - backend_id must reference an already-registered backend
      - raw_yaml must be valid YAML (parseable by pyyaml)

    Postconditions:
      - File .ledger/registry/<backend_id>/<table>.yaml exists with exactly the bytes from raw_yaml
      - A 'schema_added' ChangelogEntry has been appended to .ledger/changelog.jsonl
      - Returned ChangelogEntry has change_type='schema_added', matching backend_id and table

    Errors:
      - not_initialized (LedgerNotInitializedError): .ledger/ directory does not exist at root
          root: The root path where .ledger/ was expected
      - backend_not_found (BackendNotFoundError): No backend with the given backend_id has been registered
          backend_id: The backend_id that was not found
      - schema_parse_error (SchemaParseError): raw_yaml bytes cannot be parsed as valid YAML
          backend_id: The target backend_id
          table: The target table name
          parse_error: The underlying YAML parse error message

    Side effects: Writes raw bytes to .ledger/registry/<backend_id>/<table>.yaml, Creates directory .ledger/registry/<backend_id>/ if not exists, Appends JSONL entry to .ledger/changelog.jsonl
    Idempotent: no
    """
    ...

def list_backends(
    root: Path,
) -> list[BackendMetadata]:
    """
    Returns a list of all registered backends, sorted by backend_id alphabetically. Reads all .yaml files directly under .ledger/registry/ and deserializes them as BackendMetadata.

    Preconditions:
      - .ledger/ must be initialized at root

    Postconditions:
      - Returned list is sorted by backend_id in ascending lexicographic order
      - Each BackendMetadata in the list corresponds to a .yaml file in .ledger/registry/
      - No side effects on the filesystem

    Errors:
      - not_initialized (LedgerNotInitializedError): .ledger/ directory does not exist at root
          root: The root path where .ledger/ was expected

    Side effects: none
    Idempotent: yes
    """
    ...

def list_schemas(
    root: Path,
    backend_id: str,           # regex(^[a-z][a-z0-9_-]{1,62}[a-z0-9]$)
) -> list[SchemaRecord]:
    """
    Returns a list of all schema records for the given backend_id, sorted by table_name alphabetically. Reads all .yaml files under .ledger/registry/<backend_id>/.

    Preconditions:
      - .ledger/ must be initialized at root
      - backend_id must reference a registered backend

    Postconditions:
      - Returned list is sorted by table_name in ascending lexicographic order
      - Each SchemaRecord contains both raw_content (verbatim bytes) and parsed_content (dict)
      - No side effects on the filesystem

    Errors:
      - not_initialized (LedgerNotInitializedError): .ledger/ directory does not exist at root
          root: The root path where .ledger/ was expected
      - backend_not_found (BackendNotFoundError): No backend with the given backend_id has been registered
          backend_id: The backend_id that was not found

    Side effects: none
    Idempotent: yes
    """
    ...

def get_schema(
    root: Path,
    backend_id: str,           # regex(^[a-z][a-z0-9_-]{1,62}[a-z0-9]$)
    table: str,                # length(1..256)
) -> SchemaRecord | None:
    """
    Returns a single SchemaRecord for the given backend_id and table name, or None if no schema exists for that combination. Does not raise an error if the schema is not found — returns None instead.

    Preconditions:
      - .ledger/ must be initialized at root

    Postconditions:
      - If the schema file .ledger/registry/<backend_id>/<table>.yaml exists, returns a SchemaRecord with verbatim raw_content
      - If the schema file does not exist, returns None
      - No side effects on the filesystem

    Errors:
      - not_initialized (LedgerNotInitializedError): .ledger/ directory does not exist at root
          root: The root path where .ledger/ was expected

    Side effects: none
    Idempotent: yes
    """
    ...

def validate_all(
    root: Path,
) -> ValidationResult:
    """
    Scans all registered schemas across all backends and returns ALL validation violations. Checks: (1) annotation conflict pairs — certain annotations are mutually exclusive, (2) REQUIRES satisfaction — annotations that require other annotations or conditions produce warnings or errors, (3) backend ownership exclusivity — no two components may own the same backend. Returns a ValidationResult with the complete list of violations and a computed 'valid' flag that is True only if there are zero error-severity violations.

    Preconditions:
      - .ledger/ must be initialized at root

    Postconditions:
      - Returned ValidationResult.violations contains every detected violation — validation never short-circuits
      - ValidationResult.valid is True if and only if no violation has severity='error'
      - No side effects on the filesystem — validation is read-only

    Errors:
      - not_initialized (LedgerNotInitializedError): .ledger/ directory does not exist at root
          root: The root path where .ledger/ was expected

    Side effects: none
    Idempotent: yes
    """
    ...

def read_changelog(
    root: Path,
    backend_id: str = None,
    limit: int = 0,            # range(0..)
) -> list[ChangelogEntry]:
    """
    Reads entries from the append-only JSONL changelog at .ledger/changelog.jsonl. Supports optional filtering by backend_id and an optional limit on the number of entries returned (most recent first when limited). Performs lazy line-by-line reading to avoid loading the entire file into memory.

    Preconditions:
      - .ledger/ must be initialized at root
      - .ledger/changelog.jsonl must exist and be a valid JSONL file

    Postconditions:
      - Returned list contains at most 'limit' entries if limit > 0
      - If backend_id filter is provided, all returned entries have matching backend_id
      - Entries are ordered by sequence number ascending
      - No side effects on the filesystem

    Errors:
      - not_initialized (LedgerNotInitializedError): .ledger/ directory does not exist at root
          root: The root path where .ledger/ was expected

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['BackendType', 'ViolationSeverity', 'ChangeType', 'BackendMetadata', 'SchemaRecord', 'Violation', 'ValidationResult', 'ChangelogEntry', 'LedgerError', 'LedgerNotInitializedError', 'LedgerCorruptedError', 'DuplicateBackendError', 'OwnershipConflictError', 'BackendNotFoundError', 'SchemaParseError', 'init', 'register_backend', 'store_schema', 'list_backends', 'list_schemas', 'get_schema', 'validate_all', 'read_changelog']
