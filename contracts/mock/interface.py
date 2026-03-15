# === Mock Data Generator (mock) v1 ===
#  Dependencies: config, registry
# Generates N mock records for a given backend_id and table. Seeds Python's random module and Faker per-field (using field index offset from base seed) to ensure deterministic output independent of field order changes. Field type mapping: uuid→uuid4, varchar→random string of appropriate length, bigint→random int, boolean→random bool, timestamptz→random datetime. Classification-aware generation: PII fields use Faker for realistic fakes (email, name, address), FINANCIAL fields use plausible monetary fakes. encrypted_at_rest and tokenized fields always produce token-shaped values (tok_{base64url_24chars}), never raw plaintext. Canary mode (--purpose canary) generates fingerprinted values matching ledger-canary-{tier}-{hex8} pattern shaped to field type (email gets @canary.invalid suffix, payment_token gets tok_ prefix). Canary registration with Arbiter attempted via httpx if arbiter_api is configured; skipped with warning if not. Seed from ledger.yaml mock.seed, overridable via --seed CLI flag.

# Module invariants:
#   - Generator precedence chain is always: canary > tokenized/encrypted > classification > type > fallback. No step may be skipped or reordered.
#   - Per-field seed is computed as base_seed + field_index_offset where field_index_offset is the field's zero-based position in the lexicographically sorted list of field names. This ensures determinism independent of field declaration order.
#   - Per-row seed is computed as field_seed + row_index, giving each (field, row) pair a unique deterministic seed.
#   - Each field gets its own random.Random instance seeded with field_seed; PII fields additionally get their own Faker instance seeded with field_seed.
#   - tokenized or encrypted_at_rest fields always produce values matching pattern tok_[A-Za-z0-9_-]{24}, never raw plaintext, regardless of classification or canary mode.
#   - Canary values always match the pattern ledger-canary-{tier}-[0-9a-f]{8} shaped to the field type (email suffix @canary.invalid, payment_token prefix tok_).
#   - The hex8 portion of canary values is derived as the first 8 hex characters of SHA-256(backend_id + table_name + field_name + str(row_index)).
#   - Unsupported SQL types produce a warning MockViolation and fall back to generating a random string, never raising an exception.
#   - Arbiter registration failures are caught, logged as warnings, and never propagate exceptions to the caller.
#   - All Pydantic validation errors are aggregated and returned as a list of MockViolation entries, not raised individually.
#   - row_count must be >= 1 and <= 1_000_000.
#   - When purpose is 'canary', tier must be provided and non-empty.
#   - Fields with nullable=true have a configurable probability (default 10%) of generating None for that cell.

class FieldClassification(Enum):
    """Classification tier for a schema field, determining which generator strategy is used."""
    PII = "PII"
    FINANCIAL = "FINANCIAL"
    INTERNAL = "INTERNAL"
    PUBLIC = "PUBLIC"

class MockPurpose(Enum):
    """The purpose of mock data generation, controlling value shaping strategy."""
    test = "test"
    canary = "canary"

class ViolationSeverity(Enum):
    """Severity level for mock generation violations."""
    error = "error"
    warning = "warning"

class FieldSpec:
    """Specification of a single table field for mock generation. sql_type is the raw SQL type string (e.g. 'varchar(255)', 'bigint', 'timestamptz'). max_length is parsed from varchar(N) if present; None otherwise."""
    field_name: str                          # required, Column name in the target table.
    sql_type: str                            # required, Raw SQL type string, e.g. 'varchar(255)', 'bigint', 'uuid', 'boolean', 'timestamptz'.
    max_length: Optional[int] = None         # optional, range(value is None or 1 <= value <= 65535), Maximum character length parsed from varchar(N). None for non-varchar types.
    classification: Optional[FieldClassification] = None # optional, Classification tier of the field. None treated as PUBLIC for generation purposes.
    encrypted_at_rest: bool = false          # optional, If true, generated value is always a token-shaped string (tok_{base64url_24chars}).
    tokenized: bool = false                  # optional, If true, generated value is always a token-shaped string (tok_{base64url_24chars}).
    nullable: bool = false                   # optional, If true, there is a configurable probability (default 10%) the generated value is None.

class MockGenerationRequest:
    """Request model for mock data generation. Validated by Pydantic with cross-field model_validator enforcing that tier is required when purpose is 'canary'."""
    backend_id: str                          # required, length(1 <= len(value) <= 128), Identifier of the backend system owning the table.
    table_name: str                          # required, length(1 <= len(value) <= 256), Name of the table to generate mock records for.
    fields: list[FieldSpec]                  # required, custom(len(value) >= 1), List of field specifications for the table. Must contain at least one field.
    row_count: int                           # required, range(1 <= value <= 1000000), Number of mock records to generate.
    seed: Optional[int] = None               # optional, Override seed for deterministic generation. If None, uses mock.seed from ledger.yaml config.
    purpose: MockPurpose = test              # optional, Purpose of generation: 'test' for standard mock data, 'canary' for fingerprinted canary values.
    tier: Optional[str] = None               # optional, custom(purpose != 'canary' or (value is not None and len(value) >= 1)), Data tier label for canary mode. Required when purpose is 'canary'. Used in canary pattern: ledger-canary-{tier}-{hex8}.
    arbiter_api: Optional[str] = None        # optional, regex(^https?://.+), Base URL for Arbiter API. If provided and purpose is 'canary', canary registration is attempted via httpx.
    null_probability: float = 0.1            # optional, range(0.0 <= value <= 1.0), Probability that a nullable field produces None for a given row.

class MockViolation:
    """A single violation or warning encountered during mock generation."""
    field_name: str                          # required, Name of the field that triggered the violation. May be empty for request-level violations.
    error_type: str                          # required, Machine-readable error category, e.g. 'unsupported_type', 'arbiter_unreachable', 'validation_error'.
    message: str                             # required, Human-readable description of the violation.
    severity: ViolationSeverity              # required, Severity level: 'error' for generation failures, 'warning' for fallbacks and non-fatal issues.

class MockGenerationResult:
    """Result of mock data generation. Contains the generated records, the seed used, canary registration status, and any warnings or errors encountered."""
    records: list[dict[str, any]]            # required, Generated mock records. Each dict maps field_name to generated value.
    seed_used: int                           # required, The base seed that was used for generation (from CLI, request, or config).
    canary_registered: Optional[bool] = None # optional, True if canary registration succeeded, False if attempted and failed, None if not attempted.
    warnings: list[str]                      # required, Human-readable warning messages (e.g. arbiter skipped, unsupported type fallback).
    errors: list[MockViolation]              # required, Structured list of all violations encountered during generation.
    row_count: int                           # required, Actual number of records generated (should equal request row_count unless errors occurred).

class CanaryRegistrationResult:
    """Result of attempting to register canary values with the Arbiter API."""
    success: bool                            # required, True if Arbiter accepted the canary registration.
    arbiter_response_code: Optional[int] = None # optional, HTTP status code from Arbiter, if a response was received.
    registration_id: Optional[str] = None    # optional, Unique registration ID returned by Arbiter on success.
    error_message: Optional[str] = None      # optional, Error description if registration failed.

class CanaryValue:
    """A single canary fingerprint value for a specific field and row."""
    field_name: str                          # required, The field this canary value belongs to.
    row_index: int                           # required, Zero-based row index within the generated batch.
    raw_fingerprint: str                     # required, The raw canary fingerprint string: ledger-canary-{tier}-{hex8}.
    shaped_value: any                        # required, The canary value shaped to the field's SQL type (e.g. email with @canary.invalid suffix).

class TypeGeneratorEntry:
    """An entry in the TYPE_GENERATORS registry mapping an SQL type to its generator callable."""
    sql_type_pattern: str                    # required, SQL type name (lowercase, without length modifiers), e.g. 'uuid', 'varchar', 'bigint', 'boolean', 'timestamptz'.
    generator_fn_name: str                   # required, Fully qualified name of the generator function: (Random, FieldSpec, int) -> any.
    description: str = None                  # optional, Human-readable description of what this generator produces.

class ClassificationGeneratorEntry:
    """An entry in the CLASSIFICATION_GENERATORS registry mapping a classification tier to its Faker-based generator callable."""
    classification: FieldClassification      # required, The classification tier this generator handles.
    generator_fn_name: str                   # required, Fully qualified name of the generator function: (Faker, FieldSpec, int) -> any.
    description: str = None                  # optional, Human-readable description of what this generator produces.

class SeedInfo:
    """Computed seed information for a specific field, used internally during generation."""
    base_seed: int                           # required, The base seed from config or CLI override.
    field_index_offset: int                  # required, Zero-based position of this field in the lexicographically sorted list of all field names.
    field_seed: int                          # required, Computed as base_seed + field_index_offset.
    field_name: str                          # required, The field name this seed info belongs to.

def generate_mock_records(
    request: MockGenerationRequest,
) -> MockGenerationResult:
    """
    Primary entry point. Generates N mock records for the specified table and fields according to the MockGenerationRequest. Applies the precedence chain: canary > tokenized/encrypted > classification > type > fallback. Returns a MockGenerationResult with records, seed used, canary registration status, warnings, and aggregated errors. Never raises exceptions for generation issues; all problems are captured in the result.

    Preconditions:
      - request has been validated by Pydantic (all field-level and model-level validators pass).
      - request.fields contains at least one FieldSpec.
      - If request.purpose is 'canary', request.tier is not None and not empty.

    Postconditions:
      - result.records has length equal to request.row_count unless generation-time errors prevented some rows.
      - result.seed_used reflects the actual seed used (from request.seed, or config fallback).
      - Every record in result.records has exactly len(request.fields) keys matching the field names.
      - All tokenized or encrypted_at_rest field values match pattern tok_[A-Za-z0-9_-]{24}.
      - All canary field values (when purpose='canary') contain the substring ledger-canary-{tier}.
      - result.canary_registered is None when purpose is 'test' or arbiter_api is None.
      - result.canary_registered is True or False when purpose is 'canary' and arbiter_api is not None.
      - Unsupported SQL types produce at least one MockViolation with severity 'warning' and the field falls back to random string generation.

    Errors:
      - invalid_request (ValidationError): MockGenerationRequest fails Pydantic validation (e.g. row_count < 1, missing tier for canary).
          detail: Aggregated list of all Pydantic validation errors as MockViolation entries.
      - no_seed_available (MockGenerationError): request.seed is None and ledger.yaml mock.seed is not configured or unreadable.
          detail: No seed available: neither --seed flag nor mock.seed in ledger.yaml is set.
      - unsupported_sql_type (MockViolation): A field's sql_type does not match any entry in TYPE_GENERATORS and no classification override applies.
          severity: warning
          detail: Unsupported SQL type '{sql_type}' for field '{field_name}'; falling back to random string.
      - canary_tier_missing (ValidationError): purpose is 'canary' but tier is None or empty.
          detail: tier is required when purpose is 'canary'.
      - arbiter_connection_failure (MockViolation): Canary registration attempted but httpx request to arbiter_api fails (timeout, DNS, HTTP error).
          severity: warning
          detail: Arbiter registration failed: {error}. Canary values generated but not registered.
      - arbiter_not_configured (MockViolation): purpose is 'canary' but arbiter_api is None.
          severity: warning
          detail: Arbiter API not configured; canary registration skipped.
      - duplicate_field_names (ValidationError): Two or more FieldSpec entries have the same field_name.
          detail: Duplicate field names detected: {duplicates}.

    Side effects: Attempts HTTP POST to Arbiter API when purpose='canary' and arbiter_api is configured.
    Idempotent: yes
    """
    ...

def compute_field_seeds(
    field_names: list[str],
    base_seed: int,
) -> list[SeedInfo]:
    """
    Computes the deterministic per-field seed info for all fields in a request. Sorts field names lexicographically to assign stable field_index_offsets, then computes field_seed = base_seed + field_index_offset for each field.

    Preconditions:
      - field_names is non-empty.
      - All field_names are unique.

    Postconditions:
      - Returned list has same length as field_names.
      - Each SeedInfo.field_index_offset is unique and in range [0, len(field_names)).
      - SeedInfo entries are sorted by field_name lexicographically.
      - SeedInfo.field_seed == base_seed + SeedInfo.field_index_offset for every entry.
      - Output is fully deterministic: same inputs always produce same outputs.

    Errors:
      - empty_field_names (ValueError): field_names list is empty.
          detail: field_names must contain at least one field name.
      - duplicate_field_names (ValueError): field_names contains duplicates.
          detail: field_names must be unique.

    Side effects: none
    Idempotent: yes
    """
    ...

def generate_field_value(
    field_spec: FieldSpec,
    field_seed: int,
    row_index: int,
    purpose: MockPurpose,
    tier: Optional[str] = None,
    backend_id: str,
    table_name: str,
    null_probability: float = 0.1,
) -> any:
    """
    Generates a single value for a specific field and row index. Applies the precedence chain: canary > tokenized/encrypted > classification > type > fallback. Creates an isolated Random or Faker instance seeded with (field_seed + row_index). Returns the generated value or a fallback string with a MockViolation if the type is unsupported.

    Preconditions:
      - field_seed is a valid integer.
      - row_index >= 0.
      - If purpose is 'canary', tier is not None and not empty.

    Postconditions:
      - If field_spec.tokenized or field_spec.encrypted_at_rest is true (and purpose != 'canary'), result matches pattern tok_[A-Za-z0-9_-]{24}.
      - If purpose is 'canary', result contains the canary fingerprint shaped to the field type.
      - If field_spec.nullable is true and the seeded random check hits null_probability, result is None.
      - Same inputs always produce the same output (deterministic).

    Errors:
      - unsupported_type_fallback (MockViolation): field_spec.sql_type is not in TYPE_GENERATORS and no classification override applies.
          severity: warning
          detail: Falling back to random string for unsupported type.
      - canary_without_tier (ValueError): purpose is 'canary' but tier is None.
          detail: tier must be provided for canary generation.

    Side effects: none
    Idempotent: yes
    """
    ...

def generate_canary_fingerprint(
    backend_id: str,
    table_name: str,
    field_name: str,
    row_index: int,
    tier: str,
) -> str:
    """
    Generates the raw canary fingerprint string for a specific field and row. Computes hex8 as the first 8 hex characters of SHA-256(backend_id + table_name + field_name + str(row_index)). Returns the fingerprint in format: ledger-canary-{tier}-{hex8}.

    Preconditions:
      - tier is non-empty.
      - row_index >= 0.

    Postconditions:
      - Result matches pattern ledger-canary-{tier}-[0-9a-f]{8}.
      - hex8 portion equals hashlib.sha256((backend_id + table_name + field_name + str(row_index)).encode()).hexdigest()[:8].
      - Deterministic: same inputs always produce the same output.

    Errors:
      - empty_tier (ValueError): tier is an empty string.
          detail: tier must not be empty.

    Side effects: none
    Idempotent: yes
    """
    ...

def shape_canary_to_type(
    raw_fingerprint: str,
    field_spec: FieldSpec,
) -> any:
    """
    Shapes a raw canary fingerprint string to match the expected format for a given SQL type and field classification. For example: email fields get '@canary.invalid' suffix, payment_token fields get 'tok_' prefix, uuid fields get a valid UUID structure with embedded fingerprint, varchar fields are truncated to max_length.

    Preconditions:
      - raw_fingerprint matches pattern ledger-canary-.*-[0-9a-f]{8}.

    Postconditions:
      - Result contains the raw_fingerprint substring (or a recognizable derivative) for canary detection.
      - If field_spec.sql_type starts with 'varchar' and field_spec.max_length is set, result length <= max_length.
      - If field_spec.sql_type is 'uuid', result is a valid UUID-formatted string.
      - If field_spec.classification is PII and field_name contains 'email', result ends with '@canary.invalid'.
      - If field_spec.tokenized is true, result starts with 'tok_'.

    Errors:
      - fingerprint_too_long_for_field (MockViolation): Shaped fingerprint exceeds field_spec.max_length after shaping.
          severity: warning
          detail: Canary fingerprint truncated to fit max_length constraint.

    Side effects: none
    Idempotent: yes
    """
    ...

def generate_token_value(
    rng: any,
) -> str:
    """
    Generates a token-shaped value matching pattern tok_{base64url_24chars}. Used for fields marked as tokenized or encrypted_at_rest. Uses the provided seeded Random instance for deterministic base64url character selection.

    Preconditions:
      - rng is a random.Random instance that has been properly seeded.

    Postconditions:
      - Result matches pattern tok_[A-Za-z0-9_-]{24}.
      - Result length is exactly 28 characters (4 prefix + 24 payload).
      - Deterministic: same seed produces same token.

    Side effects: none
    Idempotent: yes
    """
    ...

def register_canary_with_arbiter(
    arbiter_api: str,
    canary_values: list[CanaryValue],
    tier: str,
    backend_id: str,
    table_name: str,
) -> CanaryRegistrationResult:
    """
    Attempts to register generated canary fingerprint values with the Arbiter API via httpx POST. Wraps the call in try/except to ensure failures never propagate as exceptions. Returns a CanaryRegistrationResult indicating success/failure.

    Preconditions:
      - arbiter_api is a valid HTTP(S) URL.
      - canary_values is non-empty.
      - tier is non-empty.

    Postconditions:
      - If Arbiter returns HTTP 2xx, result.success is True and result.registration_id is set.
      - If Arbiter returns non-2xx or connection fails, result.success is False and result.error_message describes the failure.
      - Function never raises an exception regardless of network conditions.
      - result.arbiter_response_code is set if any HTTP response was received.

    Errors:
      - connection_timeout (CanaryRegistrationResult): httpx request times out.
          success: false
          error_message: Connection to Arbiter timed out.
      - dns_resolution_failure (CanaryRegistrationResult): Arbiter hostname cannot be resolved.
          success: false
          error_message: DNS resolution failed for Arbiter.
      - http_error_response (CanaryRegistrationResult): Arbiter returns HTTP 4xx or 5xx.
          success: false
          error_message: Arbiter returned HTTP {status_code}.
      - invalid_response_body (CanaryRegistrationResult): Arbiter returns 2xx but response body is not valid JSON or missing registration_id.
          success: false
          error_message: Invalid response body from Arbiter.

    Side effects: none
    Idempotent: no
    """
    ...

def resolve_seed(
    explicit_seed: Optional[int],
    config_seed: Optional[int],
) -> int:
    """
    Resolves the base seed to use for generation. Checks in order: explicit seed parameter (from CLI --seed or request), then mock.seed from ledger.yaml config. Raises MockGenerationError if neither is available.

    Postconditions:
      - Returns explicit_seed if not None, else config_seed if not None.
      - Raises MockGenerationError if both are None.

    Errors:
      - no_seed_available (MockGenerationError): Both explicit_seed and config_seed are None.
          detail: No seed available: provide --seed flag or set mock.seed in ledger.yaml.

    Side effects: none
    Idempotent: yes
    """
    ...

def parse_varchar_length(
    sql_type: str,
) -> Optional[int]:
    """
    Parses the max_length from a varchar SQL type string. Extracts N from 'varchar(N)' or 'character varying(N)'. Returns None if the type is not a varchar variant or has no length specifier.

    Postconditions:
      - If sql_type matches 'varchar(N)' or 'character varying(N)', returns N as int.
      - If sql_type is 'varchar' without length, returns None.
      - If sql_type is not a varchar variant, returns None.
      - Returned value, if not None, is >= 1.

    Errors:
      - invalid_length_value (ValueError): varchar(N) where N is not a valid positive integer (e.g. varchar(-1), varchar(abc)).
          detail: Invalid varchar length specification: '{sql_type}'.

    Side effects: none
    Idempotent: yes
    """
    ...

def get_type_generator(
    sql_type: str,
) -> Optional[any]:
    """
    Looks up the appropriate generator function for a given SQL type from the TYPE_GENERATORS registry. Returns the generator callable if found, or None if the type is not supported (caller should use fallback).

    Preconditions:
      - sql_type is lowercase and stripped of length modifiers.

    Postconditions:
      - Returns a callable with signature (Random, FieldSpec, int) -> any if sql_type is in TYPE_GENERATORS.
      - Returns None if sql_type is not found in TYPE_GENERATORS.

    Side effects: none
    Idempotent: yes
    """
    ...

def get_classification_generator(
    classification: Optional[FieldClassification],
) -> Optional[any]:
    """
    Looks up the appropriate Faker-based generator function for a given field classification from the CLASSIFICATION_GENERATORS registry. Returns the generator callable if found (PII or FINANCIAL), or None for PUBLIC/INTERNAL classifications.

    Postconditions:
      - Returns a callable with signature (Faker, FieldSpec, int) -> any if classification is PII or FINANCIAL.
      - Returns None if classification is PUBLIC, INTERNAL, or None.

    Side effects: none
    Idempotent: yes
    """
    ...

def validate_request(
    raw_input: dict[str, any],
) -> list[MockViolation]:
    """
    Validates a MockGenerationRequest, returning an aggregated list of all violations found. Checks Pydantic field validators, cross-field constraints (tier required for canary), and semantic rules (no duplicate field names, valid SQL types).

    Postconditions:
      - Returns an empty list if input is fully valid.
      - Returns all violations found (not just the first), each as a MockViolation.
      - Violations include field-level type errors, range violations, and cross-field constraint failures.

    Errors:
      - completely_invalid_input (list[MockViolation]): Input is not a dict or is missing all required fields.
          detail: All detected violations returned as MockViolation list.

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['FieldClassification', 'MockPurpose', 'ViolationSeverity', 'FieldSpec', 'MockGenerationRequest', 'MockViolation', 'MockGenerationResult', 'CanaryRegistrationResult', 'CanaryValue', 'TypeGeneratorEntry', 'ClassificationGeneratorEntry', 'SeedInfo', 'generate_mock_records', 'ValidationError', 'MockGenerationError', 'compute_field_seeds', 'generate_field_value', 'generate_canary_fingerprint', 'shape_canary_to_type', 'generate_token_value', 'register_canary_with_arbiter', 'resolve_seed', 'parse_varchar_length', 'get_type_generator', 'get_classification_generator', 'validate_request', 'list[MockViolation]']
