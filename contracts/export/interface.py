# === Export Generators (export) v1 ===
#  Dependencies: registry, propagation, config
# Generates output YAML for four downstream tools (Pact, Arbiter, Baton, Sentinel) by consulting the schema store and annotation propagation table generically. Each exporter is a pure function that filters propagation entries by semantic rule properties — never by hard-coded annotation names — ensuring custom annotations with propagation rules defined in ledger.yaml produce output without code changes. A shared iter_propagation_entries() helper yields field-level tuples from the propagation table, and a yaml_dump() utility serializes any export model to deterministic, diffable YAML.

# Module invariants:
#   - All four exporters use iter_propagation_entries() as their sole traversal mechanism over the propagation table — no exporter directly indexes or iterates propagation_table
#   - No exporter hard-codes annotation names (e.g. 'PII', 'FINANCIAL'); all filtering is by semantic rule properties (requires_masking, canary_eligible, severity, tier, test_type, etc.)
#   - Custom annotations defined in ledger.yaml with appropriate propagation rule properties produce correct export output without any code changes to exporters
#   - All export output lists are sorted by deterministic keys for stable, diffable YAML output
#   - Violations are always collected exhaustively — exporters never short-circuit on the first error
#   - An ExportResult output is None if and only if the violations list contains at least one ERROR-severity violation
#   - yaml_dump never normalizes, reorders, or reformats beyond exclude_none — preserving model field definition order
#   - Each exporter is a standalone function — no class hierarchy, no shared mutable state between exporters
#   - Faker is used for mock_generator references on PII/FINANCIAL fields; stdlib random is used for all other mock generation

class ExportViolationSeverity(Enum):
    """Severity levels for export violations, supporting the collect-all-errors requirement."""
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"

class ExportViolation:
    """A single violation encountered during export generation. Structural problems are ERROR, missing annotations are WARNING, informational notes are INFO."""
    severity: ExportViolationSeverity        # required, Severity level of this violation.
    field_path: str                          # required, Dot-delimited path to the field that triggered the violation, e.g. 'users.email'.
    message: str                             # required, Human-readable description of the violation.
    annotation_key: str = None               # optional, The annotation key involved, if applicable. Empty string if not annotation-specific.

class PropagationEntry:
    """A single entry from the annotation propagation table. The rule dict preserves extensibility — custom annotations add arbitrary keys without code changes."""
    field_ref: str                           # required, Fully qualified field reference, e.g. 'schema.table.column'.
    annotation_key: str                      # required, The annotation identifier, e.g. 'PII', 'FINANCIAL', or any custom annotation.
    rule: dict                               # required, Property bag from ledger.yaml propagation rules. Keys include semantic properties like requires_masking, canary_eligible, severity, tier, etc. Extensible for custom annotations.
    field_type: str                          # required, The data type of the field (e.g. 'string', 'integer', 'boolean').
    component_id: str                        # required, The component that owns this field's schema.

class PropagationEntryTuple:
    """Yielded tuple from iter_propagation_entries: (field_ref, annotation_key, rule_properties)."""
    field_ref: str                           # required, Fully qualified field reference.
    annotation_key: str                      # required, The annotation identifier.
    rule_properties: dict                    # required, Semantic properties from the propagation rule payload.
    field_type: str                          # required, Data type of the underlying field.
    component_id: str                        # required, Owning component identifier.

class PactAssertion:
    """A single Pact contract assertion derived from propagation rules. Used in component-scoped contract assertion YAML."""
    assertion_id: str                        # required, Unique identifier for this assertion, deterministically derived from field_ref and annotation_key.
    description: str                         # required, Human-readable description of what this assertion verifies.
    test_type: str                           # required, Type of test: 'shape', 'filter', 'method', derived from propagation rule properties.
    field_ref: str                           # required, Fully qualified field reference this assertion targets.
    filter: str = None                       # optional, Filter expression derived from propagation rules. Empty string if not applicable.
    shape: str = None                        # optional, Shape constraint derived from propagation rules. Empty string if not applicable.
    method: str = None                       # optional, Verification method derived from propagation rules. Empty string if not applicable.

class PactExport:
    """Top-level container for Pact export output. Contains component-scoped assertion lists."""
    component_id: str                        # required, The component this Pact export targets.
    assertions: list                         # required, List of contract assertions sorted by assertion_id for deterministic output.

class ArbiterRule:
    """A single Arbiter field-level classification rule for data governance."""
    pattern: str                             # required, Field matching pattern, e.g. '*.email' or 'users.ssn'.
    backend: str                             # required, Storage backend identifier where this rule applies.
    tier: str                                # required, Classification tier: PUBLIC, INTERNAL, PII, FINANCIAL, etc.
    annotations: list                        # required, List of annotation keys applied to this field.
    taint_on_raw_value: bool                 # required, Whether raw value access taints the data flow.
    mask_in_spans: bool                      # required, Whether this field should be masked in observability spans.

class ArbiterExport:
    """Top-level container for Arbiter export output."""
    rules: list                              # required, List of classification rules sorted by pattern for deterministic output.

class BatonEgressNode:
    """A single Baton egress node describing a data exit point with mock and masking metadata."""
    id: str                                  # required, Unique identifier for this egress node.
    owner: str                               # required, Component that owns this egress point.
    mock_generator: str                      # required, Reference to the mock data generator (faker provider or stdlib random function).
    masked_fields: list                      # required, Fields with non-PUBLIC classification that require masking at egress.
    canary_eligible_fields: list             # required, String-typed fields with PII or FINANCIAL annotations (via canary_eligible rule property) eligible for canary value injection.

class BatonExport:
    """Top-level container for Baton export output."""
    egress_nodes: list                       # required, List of egress nodes sorted by id for deterministic output.

class SentinelSeverityMapping:
    """A single Sentinel severity mapping derived from annotation propagation rules."""
    annotation_key: str                      # required, The annotation that this severity mapping applies to.
    severity: str                            # required, Severity level string derived from the propagation rule's severity property (e.g. 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW').
    field_pattern: str                       # required, Field pattern this mapping applies to.
    description: str                         # required, Human-readable description of why this severity level was assigned.

class SentinelExport:
    """Top-level container for Sentinel export output."""
    severity_mappings: list                  # required, List of severity mappings sorted by (severity, annotation_key, field_pattern) for deterministic output.

class ExportResultPact:
    """Result wrapper for Pact export. Contains typed output and collected violations."""
    output: PactExport = None                # optional, The generated Pact export, or None if errors prevented generation.
    violations: list                         # required, All violations collected during generation. May contain warnings even on success.

class ExportResultArbiter:
    """Result wrapper for Arbiter export. Contains typed output and collected violations."""
    output: ArbiterExport = None             # optional, The generated Arbiter export, or None if errors prevented generation.
    violations: list                         # required, All violations collected during generation.

class ExportResultBaton:
    """Result wrapper for Baton export. Contains typed output and collected violations."""
    output: BatonExport = None               # optional, The generated Baton export, or None if errors prevented generation.
    violations: list                         # required, All violations collected during generation.

class ExportResultSentinel:
    """Result wrapper for Sentinel export. Contains typed output and collected violations."""
    output: SentinelExport = None            # optional, The generated Sentinel export, or None if errors prevented generation.
    violations: list                         # required, All violations collected during generation.

YamlString = primitive  # A UTF-8 YAML string produced by yaml_dump(). Always uses block style, sort_keys=False.

def iter_propagation_entries(
    propagation_table: list,
    property_filter: dict = {},
) -> list:
    """
    Generic iterator over the propagation table. Yields PropagationEntryTuple for each field+annotation combination from the propagation table. Callers (exporters) apply their own property-based filters. This is the single shared entry point ensuring all exporters use the same traversal logic and custom annotations flow through automatically.

    Preconditions:
      - propagation_table is a list of PropagationEntry objects
      - property_filter keys and values are strings or booleans

    Postconditions:
      - Returned list contains only PropagationEntryTuple instances
      - Every returned entry satisfies all property_filter constraints
      - Output order is deterministic: sorted by (field_ref, annotation_key)

    Errors:
      - empty_propagation_table (ExportViolation): propagation_table is empty
          severity: WARNING
          message: Propagation table is empty; no entries to iterate.
      - invalid_entry_structure (ValueError): An entry in propagation_table lacks required fields (field_ref, annotation_key, rule)
          message: PropagationEntry missing required field: {field_name}

    Side effects: none
    Idempotent: yes
    """
    ...

def export_pact(
    component_id: str,
    propagation_table: list,
) -> ExportResultPact:
    """
    Generates Pact contract assertion YAML for a given component. Iterates propagation entries and produces assertions with assertion_id, description, test_type, field_ref, and filter/shape/method derived from propagation rule properties. Filters entries where rule contains assertion-relevant properties (test_type, shape, filter, method). Returns ExportResultPact with all violations collected.

    Preconditions:
      - component_id is a non-empty string
      - propagation_table is a list of PropagationEntry objects

    Postconditions:
      - If output is not None, output.assertions is sorted by assertion_id
      - Each assertion_id is unique within the output
      - All structural errors in propagation entries are reported as ERROR-severity violations
      - Missing optional annotation properties are reported as WARNING-severity violations
      - output is None if and only if violations contain at least one ERROR-severity entry

    Errors:
      - empty_propagation_table (ExportViolation): No propagation entries exist
          severity: WARNING
          message: No propagation entries found; Pact export will be empty.
      - no_matching_entries (ExportViolation): No propagation entries match the target component_id
          severity: WARNING
          message: No propagation entries found for component '{component_id}'.
      - missing_test_type_property (ExportViolation): A propagation rule lacks the 'test_type' property required for Pact assertion generation
          severity: ERROR
          message: Propagation rule for field '{field_ref}' annotation '{annotation_key}' missing required 'test_type' property.
      - invalid_test_type (ExportViolation): test_type value is not one of 'shape', 'filter', 'method'
          severity: ERROR
          message: Invalid test_type '{test_type}' for field '{field_ref}'. Must be 'shape', 'filter', or 'method'.

    Side effects: none
    Idempotent: yes
    """
    ...

def export_arbiter(
    propagation_table: list,
    default_backend: str = primary,
) -> ExportResultArbiter:
    """
    Generates Arbiter field-level classification rules YAML. Iterates propagation entries and produces rules with pattern, backend, tier, annotations, taint_on_raw_value, and mask_in_spans derived from propagation rule properties. Filters entries where rule contains tier or classification-relevant properties. Returns ExportResultArbiter with all violations collected.

    Preconditions:
      - propagation_table is a list of PropagationEntry objects
      - default_backend is a non-empty string

    Postconditions:
      - If output is not None, output.rules is sorted by pattern
      - Each (pattern, backend) combination is unique — multiple annotations on the same field are merged into a single rule
      - taint_on_raw_value is true if any annotation's rule has requires_masking=true or taint_on_raw_value=true
      - mask_in_spans is true if any annotation's rule has requires_masking=true or mask_in_spans=true
      - output is None if and only if violations contain at least one ERROR-severity entry

    Errors:
      - empty_propagation_table (ExportViolation): No propagation entries exist
          severity: WARNING
          message: No propagation entries found; Arbiter export will be empty.
      - missing_tier_property (ExportViolation): A propagation rule lacks the 'tier' property required for classification
          severity: ERROR
          message: Propagation rule for field '{field_ref}' annotation '{annotation_key}' missing required 'tier' property.
      - conflicting_tiers (ExportViolation): Multiple annotations on the same field specify different tier values
          severity: WARNING
          message: Field '{field_ref}' has conflicting tiers: {tiers}. Using highest classification.

    Side effects: none
    Idempotent: yes
    """
    ...

def export_baton(
    propagation_table: list,
) -> ExportResultBaton:
    """
    Generates Baton egress_nodes list YAML. Iterates propagation entries and produces egress nodes with id, owner, mock_generator reference, masked_fields (non-PUBLIC tier), and canary_eligible_fields (string-typed fields where rule has canary_eligible=true). Filters on requires_masking and canary_eligible rule properties. Returns ExportResultBaton with all violations collected.

    Preconditions:
      - propagation_table is a list of PropagationEntry objects

    Postconditions:
      - If output is not None, output.egress_nodes is sorted by id
      - Each egress node id is unique
      - masked_fields contains only fields where tier is not PUBLIC (determined by requires_masking rule property)
      - canary_eligible_fields contains only fields where field_type is 'string' and rule has canary_eligible=true
      - mock_generator references faker providers for PII/FINANCIAL fields and stdlib random for others
      - output is None if and only if violations contain at least one ERROR-severity entry

    Errors:
      - empty_propagation_table (ExportViolation): No propagation entries exist
          severity: WARNING
          message: No propagation entries found; Baton export will be empty.
      - missing_mock_generator (ExportViolation): A propagation rule for a field requiring mocking lacks 'mock_generator' property
          severity: WARNING
          message: No mock_generator specified for field '{field_ref}'; will use default generator.
      - non_string_canary_field (ExportViolation): A field with canary_eligible=true has a non-string field_type
          severity: INFO
          message: Field '{field_ref}' is canary_eligible but type is '{field_type}' (not string); skipping canary eligibility.
      - missing_owner (ExportViolation): Cannot determine owner component for an egress node
          severity: ERROR
          message: Cannot determine owner for egress node derived from field '{field_ref}'.

    Side effects: none
    Idempotent: yes
    """
    ...

def export_sentinel(
    propagation_table: list,
) -> ExportResultSentinel:
    """
    Generates Sentinel severity mapping YAML derived from annotation propagation rules. Iterates propagation entries and produces severity mappings keyed by annotation and field pattern, using the 'severity' property from propagation rules. Returns ExportResultSentinel with all violations collected.

    Preconditions:
      - propagation_table is a list of PropagationEntry objects

    Postconditions:
      - If output is not None, output.severity_mappings is sorted by (severity, annotation_key, field_pattern)
      - Every propagation entry with a 'severity' rule property produces exactly one SentinelSeverityMapping
      - Entries without a 'severity' rule property are skipped with a WARNING violation
      - output is None if and only if violations contain at least one ERROR-severity entry

    Errors:
      - empty_propagation_table (ExportViolation): No propagation entries exist
          severity: WARNING
          message: No propagation entries found; Sentinel export will be empty.
      - missing_severity_property (ExportViolation): A propagation rule lacks the 'severity' property
          severity: WARNING
          message: Propagation rule for field '{field_ref}' annotation '{annotation_key}' has no 'severity' property; skipping.
      - invalid_severity_value (ExportViolation): severity property value is not a recognized severity string
          severity: ERROR
          message: Invalid severity '{severity}' for field '{field_ref}' annotation '{annotation_key}'. Expected CRITICAL, HIGH, MEDIUM, or LOW.

    Side effects: none
    Idempotent: yes
    """
    ...

def yaml_dump(
    export_model: any,
) -> YamlString:
    """
    Serializes any Pydantic v2 export model to a deterministic YAML string. Uses model.model_dump(exclude_none=True) and yaml.dump(sort_keys=False) to preserve field order from the model definition. Output is stable and diffable.

    Preconditions:
      - export_model is a Pydantic v2 BaseModel instance with a model_dump() method
      - export_model is one of PactExport, ArbiterExport, BatonExport, SentinelExport

    Postconditions:
      - Returned string is valid YAML
      - None values are excluded from output
      - Key order matches model field definition order (sort_keys=False)
      - Calling yaml_dump twice on the same model produces identical output

    Errors:
      - not_a_pydantic_model (TypeError): export_model does not have a model_dump() method
          message: export_model must be a Pydantic v2 BaseModel instance, got {type}.
      - yaml_serialization_error (ValueError): yaml.dump raises an error on the model data
          message: Failed to serialize model to YAML: {error}

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['ExportViolationSeverity', 'ExportViolation', 'PropagationEntry', 'PropagationEntryTuple', 'PactAssertion', 'PactExport', 'ArbiterRule', 'ArbiterExport', 'BatonEgressNode', 'BatonExport', 'SentinelSeverityMapping', 'SentinelExport', 'ExportResultPact', 'ExportResultArbiter', 'ExportResultBaton', 'ExportResultSentinel', 'iter_propagation_entries', 'export_pact', 'export_arbiter', 'export_baton', 'export_sentinel', 'yaml_dump']
