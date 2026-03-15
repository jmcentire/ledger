# === Migration Parser & Planner (migration) v1 ===
#  Dependencies: registry
# Parses SQL migration files (PostgreSQL/ANSI SQL) using regex-based extraction to identify ADD COLUMN, DROP COLUMN, and ALTER COLUMN operations from ALTER TABLE statements. Computes schema diffs against the current registry state and applies gate logic to produce migration plans. Gate decisions are ordered AUTO_PROCEED < HUMAN_GATE < BLOCKED. Plans are persisted as JSON in .ledger/plans/ with UUID-based plan IDs. Supports human approval workflow for HUMAN_GATE plans with required review reference and rationale. Blast radius computed via single-hop FK expansion from registry annotations.

# Module invariants:
#   - Gate decisions are totally ordered: AUTO_PROCEED < HUMAN_GATE < BLOCKED. The overall gate for a plan is always the highest-severity gate across all violations.
#   - Plan status transitions are strictly: PENDING -> APPROVED or PENDING -> REJECTED. No other transitions are valid. APPROVED and REJECTED are terminal states.
#   - Blast radius includes all directly affected tables from the parsed migration plus single-hop FK-referenced tables from registry annotations. Transitive FK chasing is never performed.
#   - SQL parsing uses regex extraction only — no full SQL parser. Only ALTER TABLE with ADD COLUMN, DROP COLUMN, and ALTER COLUMN operations are recognized.
#   - Schema YAML and SQL source content are never normalized, reformatted, or reordered during parsing.
#   - All parse and validation functions return complete lists of warnings/violations, never short-circuiting on the first error.
#   - Plan files are written atomically to .ledger/plans/ to prevent partial writes on crash.
#   - Plan IDs are UUID v4, guaranteed unique per plan.
#   - Gate rules are defined as a data table (dict mapping condition to decision), not as code branches.

class OperationType(Enum):
    """Discriminator for the type of column operation extracted from SQL."""
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    ALTER_COLUMN = "ALTER_COLUMN"

class GateDecision(Enum):
    """Ordered severity enum for migration gate outcomes. AUTO_PROCEED < HUMAN_GATE < BLOCKED."""
    AUTO_PROCEED = "AUTO_PROCEED"
    HUMAN_GATE = "HUMAN_GATE"
    BLOCKED = "BLOCKED"

class PlanStatus(Enum):
    """Lifecycle status of a migration plan."""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

class ViolationSeverity(Enum):
    """Severity level for gate violations, aligned with GateDecision."""
    INFO = "INFO"
    WARN = "WARN"
    BLOCK = "BLOCK"

class ColumnConstraint:
    """A single constraint parsed from a column definition in SQL (e.g., NOT NULL, DEFAULT, UNIQUE, PRIMARY KEY, REFERENCES)."""
    constraint_type: str                     # required, The type of constraint, e.g. 'NOT_NULL', 'DEFAULT', 'UNIQUE', 'PRIMARY_KEY', 'REFERENCES', 'CHECK'.
    value: str = None                        # optional, Optional constraint value, e.g. the default expression or referenced table.column for REFERENCES.

class ColumnOperation:
    """A single column-level operation extracted from an ALTER TABLE SQL statement. Discriminated by op_type field."""
    op_type: OperationType                   # required, Discriminator: ADD_COLUMN, DROP_COLUMN, or ALTER_COLUMN.
    table_name: str                          # required, length(min=1), Fully qualified or unqualified table name from the ALTER TABLE statement.
    column_name: str                         # required, length(min=1), Name of the column being added, dropped, or altered.
    new_type: str = None                     # optional, The SQL type for ADD_COLUMN or the new type for ALTER_COLUMN. Empty for DROP_COLUMN.
    old_type: str = None                     # optional, The previous SQL type for ALTER_COLUMN (populated from registry context during diff). Empty for ADD/DROP.
    constraints: list = []                   # optional, Parsed constraints for the column (NOT NULL, DEFAULT, etc.). Empty list if none.

class ParseWarning:
    """A non-fatal warning emitted during SQL parsing, e.g., unrecognized statement or ambiguous syntax."""
    line_number: int                         # required, 1-based line number in the source SQL where the warning occurred.
    message: str                             # required, Human-readable description of the parse warning.
    raw_statement: str = None                # optional, The raw SQL statement fragment that triggered the warning.

class ParsedMigration:
    """Result of parsing a SQL migration file. Contains all extracted column operations, source metadata, and any parse warnings."""
    operations: list                         # required, Ordered list of column operations extracted from the SQL file.
    source_path: str                         # required, Filesystem path of the source SQL migration file.
    source_hash: str                         # required, SHA-256 hex digest of the raw SQL content for integrity tracking.
    statement_count: int                     # required, Total number of semicolon-delimited statements found in the file (including non-ALTER statements).
    warnings: list = []                      # optional, Non-fatal parse warnings. Empty if parsing was clean.

class FieldAnnotation:
    """Registry-sourced annotation context for a specific field, used during diff enrichment and gate evaluation."""
    classification_tier: str                 # required, Data classification tier: PUBLIC, INTERNAL, PII, RESTRICTED.
    is_audit_field: bool                     # required, Whether this field is marked as an audit field in the registry.
    is_immutable: bool                       # required, Whether this field is marked as immutable in the registry.
    is_encrypted: bool                       # required, Whether this field is marked as requiring encryption in the registry.

class DiffEntry:
    """A single operation enriched with registry context. Combines the parsed SQL operation with annotation metadata from the schema registry."""
    operation: ColumnOperation               # required, The parsed column operation from SQL.
    annotation: FieldAnnotation = None       # optional, Registry-sourced annotation for this field. None if the field is new (ADD) or not found in registry.
    is_new_field: bool                       # required, True if this is an ADD_COLUMN for a field not currently in the registry.
    is_field_removal: bool                   # required, True if this is a DROP_COLUMN for a field that exists in the registry.

class SchemaDiff:
    """Complete diff between a parsed migration and the current registry state, enriched with annotation context."""
    entries: list                            # required, Ordered list of enriched diff entries.
    affected_tables: list                    # required, Deduplicated list of table names directly affected by the migration operations.
    source_path: str                         # required, Source migration file path, propagated from ParsedMigration.
    source_hash: str                         # required, Source file hash, propagated from ParsedMigration.

class GateViolation:
    """A single gate rule violation detected during migration gate evaluation."""
    rule_id: str                             # required, Unique identifier for the gate rule that was violated (e.g., 'audit_field_drop', 'immutable_modify', 'encryption_removal').
    severity: GateDecision                   # required, The gate decision severity for this violation: BLOCKED, HUMAN_GATE, or AUTO_PROCEED.
    table_name: str                          # required, The table containing the violating field.
    column_name: str                         # required, The column that triggered the violation.
    message: str                             # required, Human-readable explanation of why this violation was raised.
    context: dict = {}                       # optional, Additional structured context (e.g., current tier, requested tier, annotation details).

class GateRuleEntry:
    """A single row in the gate rule data table mapping a condition type and field annotation pattern to a gate decision."""
    condition_type: str                      # required, The condition identifier, e.g., 'audit_field_drop', 'immutable_modify', 'encryption_removal', 'tier_mismatch', 'public_only_change'.
    decision: GateDecision                   # required, The gate decision to apply when this condition is matched.
    description: str                         # required, Human-readable description of the rule for audit and display purposes.

class ComponentContext:
    """Context about the component requesting the migration, used to evaluate tier-based gate rules."""
    component_id: str                        # required, The component_id of the component whose migration is being evaluated.
    declared_data_access_tiers: list         # required, The list of data classification tiers this component has declared in its data_access (e.g., ['PUBLIC', 'INTERNAL']).

class MigrationPlan:
    """A complete migration plan: diff, violations, overall gate decision, blast radius, and lifecycle status. Persisted as JSON in .ledger/plans/<plan_id>.json."""
    plan_id: str                             # required, regex(^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$), UUID v4 string uniquely identifying this plan.
    schema_version: int                      # required, Version counter for the plan schema format, for forward-compatible deserialization.
    diff: SchemaDiff                         # required, The enriched schema diff this plan was computed from.
    violations: list                         # required, All gate violations detected. May be empty for AUTO_PROCEED plans.
    overall_gate: GateDecision               # required, The highest-severity gate decision across all violations. AUTO_PROCEED if no violations.
    blast_radius: list                       # required, Set of table names in the blast radius: directly affected tables plus single-hop FK-referenced tables.
    status: PlanStatus                       # required, Current lifecycle status of the plan.
    created_at: str                          # required, ISO 8601 timestamp of plan creation.
    updated_at: str                          # required, ISO 8601 timestamp of last status update.
    source_path: str                         # required, Original migration SQL file path.
    source_hash: str                         # required, SHA-256 hash of the original SQL file for integrity verification.

class ApprovalRecord:
    """Records human approval of a HUMAN_GATE migration plan, including the required review reference and rationale."""
    plan_id: str                             # required, regex(^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$), UUID of the plan being approved.
    reviewer: str                            # required, length(min=1), Identity of the reviewer approving the plan.
    review_reference: str                    # required, length(min=1), External review reference (e.g., ticket ID, PR URL) passed via --review flag.
    rationale: str                           # required, length(min=1), Human-written rationale for why this migration is approved despite gate violations.
    timestamp: str                           # required, ISO 8601 timestamp of approval.
    new_status: PlanStatus                   # required, The status the plan transitions to (APPROVED).

class MigrationParseError:
    """Raised when SQL parsing fails catastrophically (e.g., file not found, encoding error, completely unparseable content). Contains structured context."""
    source_path: str                         # required, Path to the SQL file that failed to parse.
    message: str                             # required, Human-readable error description.
    line_number: int = None                  # optional, Line number where the fatal error occurred, if applicable.
    raw_content: str = None                  # optional, The raw content fragment that caused the failure.

class PlanNotFoundError:
    """Raised when a plan_id does not correspond to any persisted plan in .ledger/plans/."""
    plan_id: str                             # required, The UUID that was not found.
    search_path: str                         # required, The filesystem path that was searched.

class InvalidPlanTransitionError:
    """Raised when an attempted plan status transition is not valid (e.g., approving an already-approved plan or approving a BLOCKED plan)."""
    plan_id: str                             # required, The UUID of the plan with the invalid transition.
    current_status: PlanStatus               # required, The current status of the plan.
    requested_status: PlanStatus             # required, The status that was requested.
    message: str                             # required, Explanation of why the transition is invalid.

class PlanPersistenceError:
    """Raised when atomic file write for plan persistence fails (disk full, permission denied, etc.)."""
    plan_id: str                             # required, The UUID of the plan that failed to persist.
    target_path: str                         # required, The filesystem path where the write was attempted.
    message: str                             # required, Human-readable error description.

def parse_migration(
    sql: str,
    source_path: str,
) -> ParsedMigration:
    """
    Parses a SQL migration file using regex-based extraction targeting PostgreSQL/ANSI SQL. Strips comments (single-line -- and block /* */), splits on semicolons, and extracts ADD COLUMN, DROP COLUMN, and ALTER COLUMN operations from ALTER TABLE statements. Returns all operations and warnings; never short-circuits on the first warning.

    Preconditions:
      - sql is a non-empty string
      - source_path is a non-empty string representing a valid filesystem path

    Postconditions:
      - Returned ParsedMigration.source_path equals the input source_path
      - Returned ParsedMigration.source_hash is the SHA-256 hex digest of the input sql string
      - Returned ParsedMigration.operations contains only ADD_COLUMN, DROP_COLUMN, or ALTER_COLUMN operations
      - All parse warnings are collected in ParsedMigration.warnings — parsing does not stop at first warning
      - Statement count reflects total semicolon-delimited statements including non-ALTER statements

    Errors:
      - empty_sql (MigrationParseError): sql is empty or contains only whitespace/comments
          message: SQL content is empty or contains only comments
      - encoding_error (MigrationParseError): sql contains invalid characters that prevent regex extraction
          message: SQL content contains invalid characters

    Side effects: none
    Idempotent: yes
    """
    ...

def compute_diff(
    parsed: ParsedMigration,
    registry: any,
) -> SchemaDiff:
    """
    Computes a schema diff between a parsed migration and the current registry state. Each parsed operation is enriched with field annotations from the registry (classification tier, audit/immutable/encrypted flags). Produces the list of affected tables from the migration operations.

    Preconditions:
      - parsed is a valid ParsedMigration with at least one operation
      - registry is a valid SchemaRegistry instance with accessible schema data

    Postconditions:
      - Returned SchemaDiff.entries has same length as parsed.operations
      - Each DiffEntry.operation corresponds to the same-index operation in parsed.operations
      - DiffEntry.annotation is populated from registry for existing fields, None for new fields
      - DiffEntry.is_new_field is True iff op_type is ADD_COLUMN and field not in registry
      - DiffEntry.is_field_removal is True iff op_type is DROP_COLUMN and field exists in registry
      - SchemaDiff.affected_tables contains all unique table names from the operations
      - SchemaDiff.source_path and source_hash propagated from parsed

    Errors:
      - empty_operations (ValueError): parsed.operations is empty
          message: Cannot compute diff from a migration with zero operations
      - registry_lookup_failure (MigrationParseError): Registry raises an error when looking up schema or annotations for a table
          message: Failed to retrieve registry data for table

    Side effects: none
    Idempotent: yes
    """
    ...

def evaluate_gates(
    diff: SchemaDiff,
    component_context: ComponentContext,
) -> list:
    """
    Applies the gate rule data table against every entry in a SchemaDiff, using the component's declared data access tiers for tier-based rules. Returns ALL violations found — never short-circuits. Gate rules: audit_field drop → BLOCKED, immutable field modify → BLOCKED, encryption removal → HUMAN_GATE, classification tier not in component's data_access → HUMAN_GATE, PUBLIC-only change to declared-PUBLIC component → AUTO_PROCEED.

    Preconditions:
      - diff is a valid SchemaDiff with at least one entry
      - component_context.component_id is non-empty
      - component_context.declared_data_access_tiers is non-empty

    Postconditions:
      - Returned list contains all GateViolation instances — every rule is evaluated against every diff entry
      - Each GateViolation has a valid rule_id corresponding to a rule in the gate rule table
      - If no violations are found, an empty list is returned (implies AUTO_PROCEED overall)
      - Violations are ordered by severity descending (BLOCKED first, then HUMAN_GATE, then AUTO_PROCEED)

    Errors:
      - empty_diff (ValueError): diff.entries is empty
          message: Cannot evaluate gates on an empty diff

    Side effects: none
    Idempotent: yes
    """
    ...

def create_plan(
    diff: SchemaDiff,
    violations: list,
    registry: any,
    plans_dir: str,
) -> MigrationPlan:
    """
    Assembles a MigrationPlan from a SchemaDiff and list of GateViolations, computes the blast radius (affected tables + single-hop FK expansion from registry), assigns a UUID v4 plan_id, sets status to PENDING, and persists the plan as JSON to .ledger/plans/<plan_id>.json using atomic file write (write to temp, then rename).

    Preconditions:
      - diff is a valid SchemaDiff
      - violations is a list of GateViolation (may be empty)
      - plans_dir is a writable directory path
      - registry is a valid SchemaRegistry for FK lookups

    Postconditions:
      - Returned MigrationPlan.plan_id is a valid UUID v4 string
      - Returned MigrationPlan.status is PENDING
      - Returned MigrationPlan.overall_gate is the max severity across all violations, or AUTO_PROCEED if violations is empty
      - Returned MigrationPlan.blast_radius includes all affected_tables from diff plus single-hop FK-referenced tables
      - Blast radius does NOT include transitive FK references (single hop only)
      - A JSON file exists at plans_dir/<plan_id>.json containing the serialized plan
      - File write is atomic: partial writes do not leave corrupt plan files
      - created_at and updated_at are set to the same ISO 8601 timestamp at creation time

    Errors:
      - plans_dir_not_writable (PlanPersistenceError): The plans_dir does not exist or is not writable
          message: Cannot write to plans directory
      - atomic_write_failure (PlanPersistenceError): Atomic file write (temp + rename) fails due to disk error or permissions
          message: Atomic write of plan file failed
      - registry_fk_lookup_failure (MigrationParseError): Registry raises an error when looking up foreign key annotations for blast radius
          message: Failed to retrieve FK annotations for blast radius computation

    Side effects: Writes plan JSON file to .ledger/plans/<plan_id>.json
    Idempotent: no
    """
    ...

def approve_plan(
    plan_id: str,              # regex(^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$)
    reviewer: str,             # length(min=1)
    review_ref: str,           # length(min=1)
    rationale: str,            # length(min=1)
    plans_dir: str,
) -> MigrationPlan:
    """
    Records human approval for a HUMAN_GATE migration plan. Validates the plan exists and is in PENDING status with overall_gate == HUMAN_GATE, then transitions status to APPROVED, updates the plan JSON, and appends an ApprovalRecord to the changelog. Requires a --review reference and rationale.

    Preconditions:
      - plan_id is a valid UUID v4
      - reviewer, review_ref, and rationale are non-empty strings
      - A plan file exists at plans_dir/<plan_id>.json
      - The plan's current status is PENDING
      - The plan's overall_gate is HUMAN_GATE (BLOCKED plans cannot be approved; AUTO_PROCEED plans do not need approval)

    Postconditions:
      - Returned MigrationPlan.status is APPROVED
      - Returned MigrationPlan.updated_at reflects the approval timestamp
      - The plan JSON file at plans_dir/<plan_id>.json is updated atomically with new status
      - An ApprovalRecord is appended to the changelog
      - The plan_id, diff, violations, and overall_gate are unchanged from the original plan

    Errors:
      - plan_not_found (PlanNotFoundError): No plan file exists for the given plan_id in plans_dir
          plan_id: <plan_id>
          search_path: <plans_dir>
      - plan_already_approved (InvalidPlanTransitionError): Plan status is already APPROVED
          current_status: APPROVED
          requested_status: APPROVED
          message: Plan is already approved
      - plan_already_rejected (InvalidPlanTransitionError): Plan status is REJECTED
          current_status: REJECTED
          requested_status: APPROVED
          message: Cannot approve a rejected plan
      - plan_is_blocked (InvalidPlanTransitionError): Plan overall_gate is BLOCKED — BLOCKED plans cannot be approved
          current_status: PENDING
          requested_status: APPROVED
          message: Cannot approve a BLOCKED plan — migration must be modified to remove blocking violations
      - plan_is_auto_proceed (InvalidPlanTransitionError): Plan overall_gate is AUTO_PROCEED — does not require approval
          current_status: PENDING
          requested_status: APPROVED
          message: AUTO_PROCEED plans do not require manual approval
      - atomic_write_failure (PlanPersistenceError): Atomic file write for updated plan or changelog fails
          message: Failed to persist approval update

    Side effects: Updates plan JSON file at .ledger/plans/<plan_id>.json, Appends ApprovalRecord to changelog
    Idempotent: no
    """
    ...

def load_plan(
    plan_id: str,              # regex(^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$)
    plans_dir: str,
) -> MigrationPlan:
    """
    Loads a persisted MigrationPlan from .ledger/plans/<plan_id>.json. Deserializes from JSON and validates the plan schema version.

    Preconditions:
      - plan_id is a valid UUID v4
      - plans_dir exists and is readable

    Postconditions:
      - Returned MigrationPlan.plan_id equals the input plan_id
      - Returned MigrationPlan passes Pydantic validation for all fields
      - Returned MigrationPlan.schema_version is compatible with the current code version

    Errors:
      - plan_not_found (PlanNotFoundError): No JSON file exists at plans_dir/<plan_id>.json
          plan_id: <plan_id>
          search_path: <plans_dir>
      - corrupted_plan_file (MigrationParseError): The JSON file exists but cannot be deserialized into a valid MigrationPlan (invalid JSON, missing fields, schema mismatch)
          message: Plan file is corrupted or has incompatible schema version
      - file_read_error (PlanPersistenceError): File exists but cannot be read (permissions, encoding error)
          message: Cannot read plan file

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['OperationType', 'GateDecision', 'PlanStatus', 'ViolationSeverity', 'ColumnConstraint', 'ColumnOperation', 'ParseWarning', 'ParsedMigration', 'FieldAnnotation', 'DiffEntry', 'SchemaDiff', 'GateViolation', 'GateRuleEntry', 'ComponentContext', 'MigrationPlan', 'ApprovalRecord', 'MigrationParseError', 'PlanNotFoundError', 'InvalidPlanTransitionError', 'PlanPersistenceError', 'parse_migration', 'compute_diff', 'evaluate_gates', 'create_plan', 'approve_plan', 'load_plan']
