# === Configuration & Data Models (config) v1 ===
# Loads and validates ledger.yaml configuration. Defines all shared Pydantic models used across the system: SchemaFile, Field, Backend, Annotation, ClassificationTier (fixed enum: PUBLIC/PII/FINANCIAL/AUTH/COMPLIANCE), MigrationGate, MigrationPlan, ChangelogEntry, PropagationRule. Provides the annotation propagation table — a data-driven dict mapping each built-in annotation to its propagation targets (Pact assertion type, Arbiter tier behavior, Baton masking rule, Sentinel severity). Custom annotations from ledger.yaml extend this table at load time. Also defines the CONFLICTS pairs (immutable+gdpr_erasable, audit_field+gdpr_erasable, soft_delete_marker+immutable) and REQUIRES rules as data. Provides file-locking utility (fcntl-based) for changelog and plans directory writes.

# Module invariants:
#   - ClassificationTier enum is fixed at exactly five members: PUBLIC, PII, FINANCIAL, AUTH, COMPLIANCE. No runtime additions or removals are permitted.
#   - CONFLICTS is an immutable frozenset of exactly three frozenset pairs: {immutable, gdpr_erasable}, {audit_field, gdpr_erasable}, {soft_delete_marker, immutable}. It is never modified after module load.
#   - REQUIRES is an immutable dict mapping annotation names to frozensets of required co-annotations. It is never modified after module load.
#   - The builtin propagation table is immutable after module load. Custom annotations may extend the table at config load time but may never overwrite or shadow a builtin annotation name.
#   - The public propagation table returned by build_propagation_table is a MappingProxyType and cannot be mutated by callers.
#   - Schema YAML content stored in SchemaFile.raw_yaml is the exact byte-for-byte string read from disk — never round-tripped through pyyaml serialization, reformatted, normalized, or reordered.
#   - All validation functions return complete lists of all violations found, never short-circuiting on the first error.
#   - All Pydantic models that represent domain data use ConfigDict(frozen=True) for immutability.
#   - LedgerConfig is the sole composed return type of load_config and contains the merged propagation table, validated schemas, and resolved configuration.
#   - file_lock operates only on Unix platforms with fcntl support; a clear PlatformError is raised on unsupported platforms.

class ClassificationTier(Enum):
    """Fixed enumeration of data classification tiers. Implemented as @final StrEnum in Python. No runtime extension permitted."""
    PUBLIC = "PUBLIC"
    PII = "PII"
    FINANCIAL = "FINANCIAL"
    AUTH = "AUTH"
    COMPLIANCE = "COMPLIANCE"

class PropagationRule:
    """Defines how a single annotation propagates to each downstream system. Frozen Pydantic model."""
    annotation_name: str                     # required, length(min=1), The canonical annotation name this rule applies to.
    pact_assertion_type: str                 # required, length(min=1), The Pact assertion type generated for this annotation (e.g. 'field_present', 'type_match', 'not_null').
    arbiter_tier_behavior: str               # required, length(min=1), How Arbiter should treat fields with this annotation (e.g. 'enforce_tier', 'audit_only', 'block_downgrade').
    baton_masking_rule: str                  # required, length(min=1), The Baton masking rule to apply (e.g. 'full_mask', 'partial_mask', 'no_mask', 'hash').
    sentinel_severity: str                   # required, regex(^(critical|high|medium|low|info)$), The Sentinel alert severity for violations of this annotation (e.g. 'critical', 'high', 'medium', 'low', 'info').

class Annotation:
    """A single annotation applied to a field, referencing a known annotation name and optional parameters. Frozen Pydantic model."""
    name: str                                # required, length(min=1), The canonical annotation name (must exist in the propagation table).
    params: dict = {}                        # optional, Optional key-value parameters for the annotation.

class Field:
    """A single field within a schema, including its type, classification tier, and annotations. Frozen Pydantic model."""
    name: str                                # required, regex(^[a-z][a-z0-9_]*$), The field name as it appears in the schema.
    field_type: str                          # required, length(min=1), The field's data type (e.g. 'string', 'integer', 'boolean', 'timestamp', 'uuid').
    classification: ClassificationTier       # required, The data classification tier assigned to this field.
    nullable: bool = false                   # optional, Whether this field permits null values.
    annotations: AnnotationList = []         # optional, List of annotations applied to this field. Validated against CONFLICTS and REQUIRES rules.

AnnotationList = list[Annotation]
# A list of Annotation instances applied to a field.

class SchemaFile:
    """Represents a parsed schema YAML file. Contains both the structured model and the raw YAML string. Frozen Pydantic model."""
    name: str                                # required, regex(^[a-z][a-z0-9_]*$), The schema name (e.g. 'users', 'orders').
    version: int                             # required, range(min=1), Schema version number, monotonically increasing.
    fields: FieldList                        # required, length(min=1), Ordered list of fields in this schema.
    raw_yaml: str                            # required, The exact raw YAML content read from disk, preserved verbatim. Never round-tripped through pyyaml.
    source_path: str                         # required, The filesystem path from which this schema was loaded.

FieldList = list[Field]
# Ordered list of Field instances within a schema.

class Backend:
    """Configuration for an external backend system (Pact, Arbiter, Baton, Sentinel, Constrain). Frozen Pydantic model."""
    name: str                                # required, regex(^(pact|arbiter|baton|sentinel|constrain)$), The backend system name.
    enabled: bool = true                     # optional, Whether this backend integration is active.
    base_url: str = ""                       # optional, regex(^(https?://.*|)$), Base URL for the backend API. Empty string if not configured.
    timeout_ms: int = 5000                   # optional, range(min=100,max=60000), HTTP request timeout in milliseconds.

class MigrationGate:
    """A single migration gate check result including severity. Frozen Pydantic model."""
    rule_name: str                           # required, length(min=1), The name of the gate rule that was evaluated.
    passed: bool                             # required, Whether the gate check passed.
    severity: str                            # required, regex(^(error|warning|info)$), The severity level of a violation.
    message: str                             # required, Human-readable description of the gate result.
    field_name: str = ""                     # optional, The field name this gate applies to, if field-specific.
    schema_name: str = ""                    # optional, The schema name this gate applies to, if schema-specific.

MigrationGateList = list[MigrationGate]
# List of MigrationGate results from a migration gate evaluation.

class MigrationPlan:
    """A complete migration plan comprising SQL diff analysis and gate results. Frozen Pydantic model."""
    plan_id: str                             # required, length(min=1), Unique identifier for this migration plan.
    schema_name: str                         # required, The schema this migration plan targets.
    from_version: int                        # required, range(min=1), The source schema version.
    to_version: int                          # required, range(min=1), The target schema version.
    gates: MigrationGateList                 # required, All gate check results for this migration.
    approved: bool                           # required, Whether all error-severity gates passed.
    created_at: str                          # required, regex(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$), ISO 8601 UTC timestamp of plan creation.

class ChangelogEntry:
    """A single entry in the schema changelog recording a change event. Frozen Pydantic model."""
    entry_id: str                            # required, length(min=1), Unique identifier for this changelog entry.
    schema_name: str                         # required, The schema that was changed.
    version: int                             # required, range(min=1), The schema version after this change.
    change_type: str                         # required, regex(^(create|update|delete|migrate)$), The type of change.
    timestamp: str                           # required, regex(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$), ISO 8601 UTC timestamp of the change.
    description: str = ""                    # optional, Human-readable description of the change.
    migration_plan_id: str = ""              # optional, Associated migration plan ID, if this change was part of a migration.

class ConstraintViolation:
    """A single constraint violation found during annotation validation. Contains the violation type, the involved annotations, and a human-readable message."""
    violation_type: str                      # required, regex(^(conflict|missing_required)$), The type of violation: 'conflict' or 'missing_required'.
    annotations: StringList                  # required, The annotation names involved in this violation.
    message: str                             # required, Human-readable description of the violation.

ConstraintViolationList = list[ConstraintViolation]
# List of all constraint violations found during validation.

StringList = list[str]
# A list of strings.

class CustomAnnotationDef:
    """Definition of a custom annotation as specified in ledger.yaml. Used to extend the builtin propagation table."""
    name: str                                # required, length(min=1), regex(^[a-z][a-z0-9_]*$), The custom annotation name. Must not collide with any builtin annotation name.
    pact_assertion_type: str                 # required, Pact assertion type for this custom annotation.
    arbiter_tier_behavior: str               # required, Arbiter tier behavior for this custom annotation.
    baton_masking_rule: str                  # required, Baton masking rule for this custom annotation.
    sentinel_severity: str                   # required, regex(^(critical|high|medium|low|info)$), Sentinel severity for this custom annotation.

CustomAnnotationDefList = list[CustomAnnotationDef]
# List of custom annotation definitions from ledger.yaml.

BackendList = list[Backend]
# List of Backend configurations.

SchemaFileList = list[SchemaFile]
# List of SchemaFile instances.

class LedgerConfig:
    """The top-level composed configuration object returned by load_config. Contains all resolved configuration, schemas, backends, and the merged propagation table. Frozen Pydantic model."""
    project_name: str                        # required, length(min=1), The project name from ledger.yaml.
    schemas_dir: str                         # required, Path to the directory containing schema YAML files.
    changelog_path: str                      # required, Path to the changelog file.
    plans_dir: str                           # required, Path to the migration plans directory.
    backends: BackendList                    # required, Configured backend integrations.
    custom_annotations: CustomAnnotationDefList = [] # optional, Custom annotation definitions from ledger.yaml.
    propagation_table: dict                  # required, The merged propagation table (builtin + custom). At runtime this is a MappingProxyType[str, PropagationRule] but serialized as dict for Pydantic compatibility.

class PropagationRuleDict:
    """Type alias documentation: at runtime the propagation table is MappingProxyType[str, PropagationRule]. This struct is not instantiated directly but documents the mapping shape."""
    key: str                                 # required, Annotation name (the dict key).
    value: PropagationRule                   # required, The propagation rule for that annotation.

class LedgerValidationError:
    """Error model aggregating all validation violations encountered during config loading or schema parsing. Contains file path context and the full list of violations."""
    file_path: str                           # required, The filesystem path of the file that failed validation.
    violations: StringList                   # required, length(min=1), All validation error messages, never truncated to just the first.
    source_exception: str = ""               # optional, String representation of the underlying exception, if any.

class ConflictsPairs:
    """Documentation type for the CONFLICTS constant. At runtime this is frozenset[frozenset[str]] with exactly three pairs."""
    pair_1: StringList                       # required, ['immutable', 'gdpr_erasable']
    pair_2: StringList                       # required, ['audit_field', 'gdpr_erasable']
    pair_3: StringList                       # required, ['soft_delete_marker', 'immutable']

class FileLockHandle:
    """Opaque handle returned when entering the file_lock context manager. Holds the lock file descriptor for cleanup."""
    lock_path: str                           # required, Path to the .lock sidecar file.
    fd: int                                  # required, File descriptor of the open lock file.
    exclusive: bool                          # required, Whether this is an exclusive (write) or shared (read) lock.

def load_config(
    path: str,                 # length(min=1)
) -> LedgerConfig:
    """
    Reads and validates ledger.yaml from the given path. Parses via yaml.safe_load, validates into Pydantic models, merges custom annotations into the propagation table with collision detection, preserves raw YAML for all schema files, and returns a composed LedgerConfig. On validation failure, raises LedgerValidationError with all violations and file path context. Never crashes on a single error — always aggregates.

    Preconditions:
      - path points to an existing, readable file
      - File contents are valid YAML (parseable by yaml.safe_load)

    Postconditions:
      - Returned LedgerConfig.propagation_table contains all builtin annotations plus any valid custom annotations from ledger.yaml
      - All SchemaFile.raw_yaml fields contain the exact bytes read from disk, not round-tripped YAML
      - No builtin annotation name has been overwritten by a custom annotation
      - All CONFLICTS and REQUIRES constraints have been validated for every field's annotation set across all loaded schemas

    Errors:
      - file_not_found (FileNotFoundError): The file at path does not exist
          path: The path that was not found
      - permission_denied (PermissionError): The file at path is not readable
          path: The path that could not be read
      - invalid_yaml (LedgerValidationError): The file content is not valid YAML
          file_path: Path to the invalid file
          violations: YAML parse error details
      - validation_errors (LedgerValidationError): The YAML content fails Pydantic model validation (missing required fields, type mismatches, constraint violations)
          file_path: Path to the invalid file
          violations: All validation error messages
      - annotation_collision (LedgerValidationError): A custom annotation name collides with a builtin annotation name
          file_path: Path to ledger.yaml
          violations: List of colliding annotation names
      - constraint_violations (LedgerValidationError): One or more fields have annotation sets that violate CONFLICTS or REQUIRES rules
          file_path: Path to the schema file with violations
          violations: All constraint violation messages

    Side effects: Reads ledger.yaml from disk, Reads schema YAML files referenced in configuration from disk
    Idempotent: yes
    """
    ...

def build_propagation_table(
    custom_annotations: CustomAnnotationDefList,
) -> dict:
    """
    Merges custom annotation definitions with the builtin propagation table. Detects and rejects name collisions between custom and builtin annotations. Returns an immutable MappingProxyType[str, PropagationRule] containing all annotations.

    Preconditions:
      - All custom annotations have non-empty, valid snake_case names
      - All custom annotations have valid sentinel_severity values

    Postconditions:
      - Returned dict (MappingProxyType at runtime) contains all builtin annotations unchanged
      - Returned dict contains all provided custom annotations that do not collide with builtins
      - Returned dict is immutable (MappingProxyType) and cannot be modified by callers
      - No builtin annotation entry has been modified or removed

    Errors:
      - name_collision (ValueError): One or more custom annotation names match a builtin annotation name
          collisions: List of colliding annotation names
      - duplicate_custom_names (ValueError): Two or more custom annotations share the same name
          duplicates: List of duplicate custom annotation names

    Side effects: none
    Idempotent: yes
    """
    ...

def validate_annotation_set(
    annotations: StringList,
) -> ConstraintViolationList:
    """
    Validates a set of annotation names against the CONFLICTS and REQUIRES constraint data. Returns a complete list of all violations found — never short-circuits on the first error. An empty return list indicates no violations.

    Preconditions:
      - All annotation names in the set are non-empty strings

    Postconditions:
      - Every CONFLICTS pair where both members are present in the input set produces exactly one 'conflict' violation
      - Every REQUIRES rule where the source annotation is present but one or more required annotations are missing produces exactly one 'missing_required' violation per missing annotation
      - The returned list is exhaustive — all violations are reported, not just the first

    Errors:
      - empty_annotation_name (ValueError): One or more annotation names in the input set are empty strings
          message: Annotation names must be non-empty

    Side effects: none
    Idempotent: yes
    """
    ...

def get_builtin_propagation_table() -> dict:
    """
    Returns a read-only copy of the builtin propagation table (without custom annotations). Useful for introspection and testing. The returned value is a MappingProxyType[str, PropagationRule].

    Postconditions:
      - Returned dict is a MappingProxyType and cannot be mutated
      - Contains exactly the builtin annotation set, no custom annotations

    Side effects: none
    Idempotent: yes
    """
    ...

def get_conflicts() -> any:
    """
    Returns the CONFLICTS constant: a frozenset of frozenset[str] pairs representing mutually exclusive annotation combinations. Exactly three pairs: {immutable, gdpr_erasable}, {audit_field, gdpr_erasable}, {soft_delete_marker, immutable}.

    Postconditions:
      - Returned value is a frozenset containing exactly 3 frozenset[str] pairs
      - Returned value is identical to the module-level CONFLICTS constant

    Side effects: none
    Idempotent: yes
    """
    ...

def get_requires() -> dict:
    """
    Returns the REQUIRES constant: a dict mapping annotation names to frozensets of annotation names that must co-occur. Immutable at module level.

    Postconditions:
      - Returned value is the module-level REQUIRES dict
      - Keys are annotation names, values are frozenset[str] of required co-annotations

    Side effects: none
    Idempotent: yes
    """
    ...

def file_lock(
    path: str,                 # length(min=1)
    exclusive: bool = true,
    blocking: bool = true,
) -> FileLockHandle:
    """
    Context manager that acquires an advisory file lock (fcntl.flock) on a .lock sidecar file adjacent to the given path. Supports exclusive (write) and shared (read) locks. Creates the .lock file if it does not exist. On non-Unix platforms, raises PlatformError. Used for coordinating concurrent writes to changelog and migration plans directories.

    Preconditions:
      - Running on a Unix platform with fcntl support
      - The parent directory of path exists and is writable (for .lock sidecar creation)

    Postconditions:
      - On context entry: the .lock sidecar file exists and is locked (exclusive or shared) per the requested mode
      - On context exit: the lock is released and the file descriptor is closed
      - The original file at path is never modified by the locking mechanism itself

    Errors:
      - platform_unsupported (PlatformError): Running on a non-Unix platform (e.g. Windows) where fcntl is not available
          message: fcntl-based file locking is only supported on Unix platforms
      - lock_contention (BlockingIOError): blocking=false and the lock is already held by another process
          path: The lock file path that could not be acquired
      - permission_denied (PermissionError): Cannot create or open the .lock sidecar file due to filesystem permissions
          path: The lock file path
      - parent_dir_missing (FileNotFoundError): The parent directory of path does not exist
          path: The parent directory path

    Side effects: Creates .lock sidecar file if it does not exist, Acquires advisory fcntl.flock lock on the sidecar file
    Idempotent: no
    """
    ...

def parse_schema_file(
    path: str,                 # length(min=1)
    propagation_table: dict,
) -> SchemaFile:
    """
    Parses a single schema YAML file into a SchemaFile model. Reads the file, preserves raw YAML verbatim, parses via yaml.safe_load, validates into Field/Annotation models, and runs CONFLICTS/REQUIRES validation on every field's annotation set. Returns all violations on failure, not just the first.

    Preconditions:
      - path points to an existing, readable YAML file
      - propagation_table contains all valid annotation names (builtin + custom)

    Postconditions:
      - Returned SchemaFile.raw_yaml is the exact string content read from disk
      - All field annotation names exist in the propagation_table
      - All field annotation sets pass CONFLICTS and REQUIRES validation
      - SchemaFile.source_path equals the input path

    Errors:
      - file_not_found (FileNotFoundError): The schema file does not exist
          path: The path that was not found
      - invalid_yaml (LedgerValidationError): The file content is not valid YAML
          file_path: Path to the invalid file
          violations: YAML parse error details
      - schema_validation_errors (LedgerValidationError): The parsed YAML fails SchemaFile/Field/Annotation Pydantic validation
          file_path: Path to the invalid file
          violations: All validation error messages
      - unknown_annotation (LedgerValidationError): A field references an annotation name not present in the propagation_table
          file_path: Path to the schema file
          violations: List of unknown annotation names with field context
      - constraint_violations (LedgerValidationError): One or more fields have annotation sets violating CONFLICTS or REQUIRES
          file_path: Path to the schema file
          violations: All constraint violation messages with field context

    Side effects: Reads schema YAML file from disk
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['ClassificationTier', 'PropagationRule', 'Annotation', 'Field', 'AnnotationList', 'SchemaFile', 'FieldList', 'Backend', 'MigrationGate', 'MigrationGateList', 'MigrationPlan', 'ChangelogEntry', 'ConstraintViolation', 'ConstraintViolationList', 'StringList', 'CustomAnnotationDef', 'CustomAnnotationDefList', 'BackendList', 'SchemaFileList', 'LedgerConfig', 'PropagationRuleDict', 'LedgerValidationError', 'ConflictsPairs', 'FileLockHandle', 'load_config', 'build_propagation_table', 'validate_annotation_set', 'get_builtin_propagation_table', 'get_conflicts', 'get_requires', 'file_lock', 'PlatformError', 'BlockingIOError', 'parse_schema_file']
