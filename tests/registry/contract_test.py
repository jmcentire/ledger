"""
Contract test suite for the Registry & Schema Store component.
Tests verify behavior against the contract, covering happy paths, edge cases,
error cases, and invariants.

Run with: pytest contract_test.py -v
"""
import os
import sys
import pathlib
import pytest
from datetime import datetime, timezone
from pathlib import Path

from registry import (
    init,
    register_backend,
    store_schema,
    list_backends,
    list_schemas,
    get_schema,
    validate_all,
    read_changelog,
    BackendType,
    BackendMetadata,
    SchemaRecord,
    ValidationResult,
    Violation,
    ViolationSeverity,
    ChangelogEntry,
    ChangeType,
    LedgerError,
    LedgerNotInitializedError,
    LedgerCorruptedError,
    DuplicateBackendError,
    OwnershipConflictError,
    BackendNotFoundError,
    SchemaParseError,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def uninitialized_root(tmp_path):
    """Bare tmp_path directory — no .ledger/ initialized."""
    return tmp_path


@pytest.fixture
def initialized_root(tmp_path):
    """tmp_path with init() already called."""
    init(tmp_path)
    return tmp_path


def _make_metadata(backend_id="test-backend", backend_type=BackendType.postgres,
                   owner_component="my-service"):
    """Helper to create a BackendMetadata with sensible defaults."""
    return BackendMetadata(
        backend_id=backend_id,
        backend_type=backend_type,
        owner_component=owner_component,
        registered_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_metadata():
    return _make_metadata()


@pytest.fixture
def populated_root(tmp_path):
    """Initialized root with one registered backend and one stored schema."""
    init(tmp_path)
    meta = _make_metadata(backend_id="main-db", backend_type=BackendType.postgres,
                          owner_component="billing-service")
    register_backend(tmp_path, meta, actor="setup")
    yaml_bytes = b"columns:\n  id: integer\n  name: varchar\n"
    store_schema(tmp_path, "main-db", "users", yaml_bytes, actor="setup")
    return tmp_path


VALID_YAML = b"columns:\n  id: integer\n  name: varchar\n"
INVALID_YAML = b"{{not: [valid: yaml"
YAML_WITH_COMMENTS = b"# This is a header comment\ncolumns:\n  id: integer  # primary key\n  name: varchar\n"
UNICODE_YAML = b"description: \"\xc3\xa9l\xc3\xa8ve\"\ncolumns:\n  id: integer\n"


# ===========================================================================
# 1. init() tests
# ===========================================================================

class TestInit:
    def test_init_happy_path(self, tmp_path):
        """init creates .ledger/ directory structure at given root."""
        init(tmp_path)
        ledger = tmp_path / ".ledger"
        assert ledger.is_dir()
        assert (ledger / "registry").is_dir()
        assert (ledger / "plans").is_dir()
        assert (ledger / "changelog.jsonl").is_file()

    def test_init_idempotent(self, initialized_root):
        """Calling init twice is a no-op."""
        # Should not raise
        init(initialized_root)
        ledger = initialized_root / ".ledger"
        assert ledger.is_dir()
        assert (ledger / "registry").is_dir()
        assert (ledger / "plans").is_dir()
        assert (ledger / "changelog.jsonl").is_file()

    def test_init_changelog_empty_on_creation(self, tmp_path):
        """Newly created changelog.jsonl is empty."""
        init(tmp_path)
        changelog = tmp_path / ".ledger" / "changelog.jsonl"
        assert changelog.stat().st_size == 0

    def test_init_corrupted_missing_registry(self, tmp_path):
        """Raises LedgerCorruptedError when .ledger/ exists but registry/ missing."""
        ledger = tmp_path / ".ledger"
        ledger.mkdir()
        (ledger / "plans").mkdir()
        (ledger / "changelog.jsonl").touch()
        # registry is missing
        with pytest.raises(LedgerCorruptedError) as exc_info:
            init(tmp_path)
        assert hasattr(exc_info.value, "missing_paths")

    def test_init_corrupted_missing_changelog(self, tmp_path):
        """Raises LedgerCorruptedError when changelog.jsonl is missing."""
        ledger = tmp_path / ".ledger"
        ledger.mkdir()
        (ledger / "registry").mkdir()
        (ledger / "plans").mkdir()
        # changelog.jsonl is missing
        with pytest.raises(LedgerCorruptedError):
            init(tmp_path)

    def test_init_corrupted_missing_plans(self, tmp_path):
        """Raises LedgerCorruptedError when plans/ is missing."""
        ledger = tmp_path / ".ledger"
        ledger.mkdir()
        (ledger / "registry").mkdir()
        (ledger / "changelog.jsonl").touch()
        # plans is missing
        with pytest.raises(LedgerCorruptedError):
            init(tmp_path)


# ===========================================================================
# 2. register_backend() tests
# ===========================================================================

class TestRegisterBackend:
    def test_register_backend_happy_path(self, initialized_root):
        """Registers a new backend and writes YAML metadata file."""
        meta = _make_metadata(backend_id="my-db")
        entry = register_backend(initialized_root, meta, actor="admin")
        assert entry.change_type == ChangeType.backend_registered
        assert entry.backend_id == "my-db"
        yaml_file = initialized_root / ".ledger" / "registry" / "my-db.yaml"
        assert yaml_file.is_file()

    def test_register_backend_changelog_entry_fields(self, initialized_root):
        """Returned ChangelogEntry has correct fields including sequence=1."""
        meta = _make_metadata(backend_id="first-db")
        entry = register_backend(initialized_root, meta, actor="deployer")
        assert entry.sequence == 1
        assert entry.actor == "deployer"
        assert entry.backend_id == "first-db"
        assert entry.change_type == ChangeType.backend_registered

    def test_register_backend_duplicate(self, initialized_root):
        """Raises DuplicateBackendError when registering same backend_id twice."""
        meta = _make_metadata(backend_id="my-db")
        register_backend(initialized_root, meta, actor="admin")
        with pytest.raises(DuplicateBackendError) as exc_info:
            register_backend(initialized_root, meta, actor="admin")
        assert exc_info.value.backend_id == "my-db"

    def test_register_backend_ownership_conflict(self, initialized_root):
        """Raises OwnershipConflictError when different component tries same id."""
        meta_a = _make_metadata(backend_id="shared-db", owner_component="service-a")
        register_backend(initialized_root, meta_a, actor="admin")
        meta_b = _make_metadata(backend_id="shared-db", owner_component="service-b")
        with pytest.raises((DuplicateBackendError, OwnershipConflictError)):
            register_backend(initialized_root, meta_b, actor="admin")

    def test_register_backend_not_initialized(self, uninitialized_root):
        """Raises LedgerNotInitializedError when .ledger/ does not exist."""
        meta = _make_metadata()
        with pytest.raises(LedgerNotInitializedError):
            register_backend(uninitialized_root, meta, actor="admin")

    def test_register_backend_sequence_monotonic(self, initialized_root):
        """Sequence numbers increase monotonically across registrations."""
        e1 = register_backend(initialized_root, _make_metadata("alpha-db", owner_component="svc-a"), "admin")
        e2 = register_backend(initialized_root, _make_metadata("beta-db", owner_component="svc-b"), "admin")
        e3 = register_backend(initialized_root, _make_metadata("gamma-db", owner_component="svc-c"), "admin")
        assert e1.sequence == 1
        assert e2.sequence == 2
        assert e3.sequence == 3

    def test_register_multiple_backends_different_types(self, initialized_root):
        """Can register backends of different types."""
        e1 = register_backend(initialized_root,
                              _make_metadata("pg-db", BackendType.postgres, "svc-a"), "admin")
        e2 = register_backend(initialized_root,
                              _make_metadata("redis-cache", BackendType.redis, "svc-b"), "admin")
        e3 = register_backend(initialized_root,
                              _make_metadata("s3-store", BackendType.s3, "svc-c"), "admin")
        assert e1.change_type == ChangeType.backend_registered
        assert e2.change_type == ChangeType.backend_registered
        assert e3.change_type == ChangeType.backend_registered

    def test_register_backend_writes_yaml_file(self, initialized_root):
        """register_backend creates a YAML file with metadata content."""
        import yaml
        meta = _make_metadata(backend_id="test-db", backend_type=BackendType.mysql,
                              owner_component="my-svc")
        register_backend(initialized_root, meta, actor="admin")
        yaml_file = initialized_root / ".ledger" / "registry" / "test-db.yaml"
        assert yaml_file.is_file()
        content = yaml.safe_load(yaml_file.read_text())
        assert content["backend_id"] == "test-db"
        assert content["owner_component"] == "my-svc"

    def test_changelog_entry_has_timestamp(self, initialized_root):
        """Every ChangelogEntry has a UTC timestamp."""
        meta = _make_metadata(backend_id="ts-db")
        entry = register_backend(initialized_root, meta, actor="admin")
        assert entry.timestamp is not None
        assert entry.timestamp.tzinfo is not None


# ===========================================================================
# 3. store_schema() tests
# ===========================================================================

class TestStoreSchema:
    def test_store_schema_happy_path(self, populated_root):
        """Stores schema YAML bytes and returns ChangelogEntry."""
        yaml_bytes = b"columns:\n  email: varchar\n"
        entry = store_schema(populated_root, "main-db", "emails", yaml_bytes, actor="dev")
        assert entry.change_type == ChangeType.schema_added
        assert entry.backend_id == "main-db"
        assert entry.table == "emails"
        schema_file = populated_root / ".ledger" / "registry" / "main-db" / "emails.yaml"
        assert schema_file.is_file()

    def test_store_schema_verbatim_roundtrip(self, populated_root):
        """Stored schema bytes are byte-exact identical to input."""
        yaml_bytes = b"# comment preserved\ncolumns:\n  id: integer  # inline\n  name: varchar\n\n"
        store_schema(populated_root, "main-db", "orders", yaml_bytes, actor="dev")
        schema_file = populated_root / ".ledger" / "registry" / "main-db" / "orders.yaml"
        assert schema_file.read_bytes() == yaml_bytes

    def test_store_schema_invalid_yaml(self, populated_root):
        """Raises SchemaParseError when raw_yaml is not valid YAML."""
        with pytest.raises(SchemaParseError) as exc_info:
            store_schema(populated_root, "main-db", "bad", INVALID_YAML, actor="dev")
        assert exc_info.value.backend_id == "main-db"
        assert exc_info.value.table == "bad"
        assert exc_info.value.parse_error != ""

    def test_store_schema_backend_not_found(self, initialized_root):
        """Raises BackendNotFoundError when backend_id is not registered."""
        with pytest.raises(BackendNotFoundError) as exc_info:
            store_schema(initialized_root, "nonexistent-db", "t", VALID_YAML, actor="dev")
        assert exc_info.value.backend_id == "nonexistent-db"

    def test_store_schema_not_initialized(self, uninitialized_root):
        """Raises LedgerNotInitializedError when .ledger/ does not exist."""
        with pytest.raises(LedgerNotInitializedError):
            store_schema(uninitialized_root, "x", "t", VALID_YAML, actor="dev")

    def test_store_schema_preserves_comments(self, populated_root):
        """YAML comments are preserved verbatim in stored file."""
        store_schema(populated_root, "main-db", "commented", YAML_WITH_COMMENTS, actor="dev")
        schema_file = populated_root / ".ledger" / "registry" / "main-db" / "commented.yaml"
        stored = schema_file.read_bytes()
        assert b"# This is a header comment" in stored
        assert b"# primary key" in stored

    def test_store_schema_changelog_entry(self, populated_root):
        """store_schema appends schema_added entry to changelog."""
        entry = store_schema(populated_root, "main-db", "items", VALID_YAML, actor="dev")
        assert entry.change_type == ChangeType.schema_added
        assert entry.backend_id == "main-db"
        assert entry.table == "items"

    def test_store_schema_creates_backend_subdir(self, initialized_root):
        """store_schema creates backend subdirectory if it does not exist."""
        meta = _make_metadata(backend_id="new-db")
        register_backend(initialized_root, meta, actor="admin")
        store_schema(initialized_root, "new-db", "t1", VALID_YAML, actor="dev")
        subdir = initialized_root / ".ledger" / "registry" / "new-db"
        assert subdir.is_dir()

    def test_store_schema_empty_yaml(self, populated_root):
        """Storing a minimal YAML document that is parseable."""
        # b"---\n" is valid YAML (empty document)
        try:
            entry = store_schema(populated_root, "main-db", "empty", b"---\n", actor="dev")
            # If it succeeds, that's acceptable
            assert entry.change_type == ChangeType.schema_added
        except SchemaParseError:
            # If implementation rejects empty docs, that's also acceptable
            pass

    def test_store_schema_binary_yaml_content(self, populated_root):
        """YAML with unicode characters is stored verbatim."""
        store_schema(populated_root, "main-db", "unicode", UNICODE_YAML, actor="dev")
        schema_file = populated_root / ".ledger" / "registry" / "main-db" / "unicode.yaml"
        assert schema_file.read_bytes() == UNICODE_YAML


# ===========================================================================
# 4. list_backends() tests
# ===========================================================================

class TestListBackends:
    def test_list_backends_empty(self, initialized_root):
        """Returns empty list when no backends registered."""
        result = list_backends(initialized_root)
        assert isinstance(result, list)
        assert len(result) == 0

    def test_list_backends_single(self, initialized_root):
        """Returns list with one BackendMetadata."""
        meta = _make_metadata(backend_id="solo-db", owner_component="svc")
        register_backend(initialized_root, meta, actor="admin")
        result = list_backends(initialized_root)
        assert len(result) == 1
        assert result[0].backend_id == "solo-db"

    def test_list_backends_sorted(self, initialized_root):
        """Multiple backends returned sorted by backend_id ascending."""
        for bid, owner in [("zulu-db", "svc-z"), ("alpha-db", "svc-a"), ("mike-db", "svc-m")]:
            register_backend(initialized_root, _make_metadata(bid, owner_component=owner), "admin")
        result = list_backends(initialized_root)
        assert len(result) == 3
        assert result[0].backend_id == "alpha-db"
        assert result[1].backend_id == "mike-db"
        assert result[2].backend_id == "zulu-db"

    def test_list_backends_not_initialized(self, uninitialized_root):
        """Raises LedgerNotInitializedError when .ledger/ does not exist."""
        with pytest.raises(LedgerNotInitializedError):
            list_backends(uninitialized_root)

    def test_list_backends_metadata_fields(self, initialized_root):
        """Each BackendMetadata in list has all required fields."""
        meta = _make_metadata(backend_id="field-db", backend_type=BackendType.redis,
                              owner_component="cache-svc")
        register_backend(initialized_root, meta, actor="admin")
        result = list_backends(initialized_root)
        assert len(result) == 1
        bm = result[0]
        assert bm.backend_id == "field-db"
        assert bm.backend_type == BackendType.redis
        assert bm.owner_component == "cache-svc"
        assert bm.registered_at is not None


# ===========================================================================
# 5. list_schemas() tests
# ===========================================================================

class TestListSchemas:
    def test_list_schemas_empty(self, initialized_root):
        """Returns empty list when backend has no schemas."""
        meta = _make_metadata(backend_id="empty-db")
        register_backend(initialized_root, meta, actor="admin")
        result = list_schemas(initialized_root, "empty-db")
        assert len(result) == 0

    def test_list_schemas_single(self, populated_root):
        """Returns list with one SchemaRecord."""
        result = list_schemas(populated_root, "main-db")
        assert len(result) == 1
        assert result[0].table_name == "users"
        assert result[0].raw_content == b"columns:\n  id: integer\n  name: varchar\n"

    def test_list_schemas_sorted(self, populated_root):
        """Multiple schemas returned sorted by table_name ascending."""
        store_schema(populated_root, "main-db", "accounts", VALID_YAML, actor="dev")
        store_schema(populated_root, "main-db", "orders", VALID_YAML, actor="dev")
        # Already has 'users' from fixture
        result = list_schemas(populated_root, "main-db")
        assert len(result) == 3
        assert result[0].table_name == "accounts"
        assert result[1].table_name == "orders"
        assert result[2].table_name == "users"

    def test_list_schemas_backend_not_found(self, initialized_root):
        """Raises BackendNotFoundError for unregistered backend_id."""
        with pytest.raises(BackendNotFoundError):
            list_schemas(initialized_root, "nonexistent-db")

    def test_list_schemas_not_initialized(self, uninitialized_root):
        """Raises LedgerNotInitializedError when .ledger/ does not exist."""
        with pytest.raises(LedgerNotInitializedError):
            list_schemas(uninitialized_root, "any-db")

    def test_list_schemas_raw_and_parsed(self, populated_root):
        """Each SchemaRecord has both raw_content (bytes) and parsed_content (dict)."""
        result = list_schemas(populated_root, "main-db")
        rec = result[0]
        assert isinstance(rec.raw_content, bytes)
        assert isinstance(rec.parsed_content, dict)
        assert "columns" in rec.parsed_content


# ===========================================================================
# 6. get_schema() tests
# ===========================================================================

class TestGetSchema:
    def test_get_schema_found(self, populated_root):
        """Returns SchemaRecord when schema exists."""
        result = get_schema(populated_root, "main-db", "users")
        assert result is not None
        assert result.backend_id == "main-db"
        assert result.table_name == "users"
        assert result.raw_content == b"columns:\n  id: integer\n  name: varchar\n"

    def test_get_schema_not_found(self, populated_root):
        """Returns None when schema does not exist for given table."""
        result = get_schema(populated_root, "main-db", "nonexistent")
        assert result is None

    def test_get_schema_not_initialized(self, uninitialized_root):
        """Raises LedgerNotInitializedError when .ledger/ does not exist."""
        with pytest.raises(LedgerNotInitializedError):
            get_schema(uninitialized_root, "any-db", "t")

    def test_get_schema_verbatim_bytes(self, populated_root):
        """get_schema returns verbatim raw_content matching stored bytes exactly."""
        yaml_bytes = b"# header\nkey: value  # comment\n  nested: true\n"
        # This specific YAML might not be valid as-is; use a known valid one
        yaml_bytes = b"# header\nkey: value  # comment\n"
        store_schema(populated_root, "main-db", "verbatim", yaml_bytes, actor="dev")
        result = get_schema(populated_root, "main-db", "verbatim")
        assert result is not None
        assert result.raw_content == yaml_bytes

    def test_get_schema_parsed_content_dict(self, populated_root):
        """get_schema returns parsed_content as a dict."""
        result = get_schema(populated_root, "main-db", "users")
        assert result is not None
        assert isinstance(result.parsed_content, dict)
        assert result.parsed_content["columns"]["id"] == "integer"

    def test_get_schema_after_multiple_stores(self, populated_root):
        """get_schema returns the latest stored schema for a given table."""
        v1 = b"version: 1\ncolumns:\n  id: integer\n"
        v2 = b"version: 2\ncolumns:\n  id: bigint\n"
        store_schema(populated_root, "main-db", "evolving", v1, actor="dev")
        store_schema(populated_root, "main-db", "evolving", v2, actor="dev")
        result = get_schema(populated_root, "main-db", "evolving")
        assert result is not None
        assert result.raw_content == v2


# ===========================================================================
# 7. validate_all() tests
# ===========================================================================

class TestValidateAll:
    def test_validate_all_empty_registry(self, initialized_root):
        """validate_all on empty initialized registry returns valid=True."""
        result = validate_all(initialized_root)
        assert isinstance(result, ValidationResult)
        assert result.valid is True
        assert len(result.violations) == 0

    def test_validate_all_clean_state(self, populated_root):
        """validate_all returns valid=True for clean registry with valid schemas."""
        result = validate_all(populated_root)
        assert isinstance(result, ValidationResult)
        # A clean state should have no error-level violations
        error_violations = [v for v in result.violations if v.severity == ViolationSeverity.error]
        assert len(error_violations) == 0
        assert result.valid is True

    def test_validate_all_not_initialized(self, uninitialized_root):
        """Raises LedgerNotInitializedError when .ledger/ does not exist."""
        with pytest.raises(LedgerNotInitializedError):
            validate_all(uninitialized_root)

    def test_validate_all_valid_property(self, initialized_root):
        """ValidationResult.valid is True iff no error-severity violations."""
        result = validate_all(initialized_root)
        error_violations = [v for v in result.violations if v.severity == ViolationSeverity.error]
        if len(error_violations) == 0:
            assert result.valid is True
        else:
            assert result.valid is False

    def test_validate_all_returns_validation_result(self, initialized_root):
        """validate_all returns a ValidationResult with violations list."""
        result = validate_all(initialized_root)
        assert hasattr(result, "violations")
        assert hasattr(result, "valid")
        assert isinstance(result.violations, list)


# ===========================================================================
# 8. read_changelog() tests
# ===========================================================================

class TestReadChangelog:
    def test_read_changelog_empty(self, initialized_root):
        """Returns empty list when changelog has no entries."""
        result = read_changelog(initialized_root, backend_id="", limit=0)
        assert isinstance(result, list)
        assert len(result) == 0

    def test_read_changelog_all_entries(self, initialized_root):
        """Returns all entries when no filter and limit=0."""
        for i, bid in enumerate(["db-a", "db-b", "db-c"]):
            register_backend(initialized_root,
                             _make_metadata(bid, owner_component=f"svc-{i}"), "admin")
        result = read_changelog(initialized_root, backend_id="", limit=0)
        assert len(result) == 3

    def test_read_changelog_filter_by_backend(self, initialized_root):
        """Filters entries to only matching backend_id."""
        register_backend(initialized_root,
                         _make_metadata("db-a", owner_component="svc-a"), "admin")
        register_backend(initialized_root,
                         _make_metadata("db-b", owner_component="svc-b"), "admin")
        result = read_changelog(initialized_root, backend_id="db-a", limit=0)
        assert all(e.backend_id == "db-a" for e in result)
        assert len(result) >= 1

    def test_read_changelog_limit(self, initialized_root):
        """Returns at most 'limit' entries when limit > 0."""
        for i in range(5):
            register_backend(initialized_root,
                             _make_metadata(f"db-{i:03d}", owner_component=f"svc-{i}"), "admin")
        result = read_changelog(initialized_root, backend_id="", limit=2)
        assert len(result) == 2

    def test_read_changelog_ordered_by_sequence(self, initialized_root):
        """Entries are ordered by sequence number ascending."""
        for i, bid in enumerate(["db-x", "db-y", "db-z"]):
            register_backend(initialized_root,
                             _make_metadata(bid, owner_component=f"svc-{i}"), "admin")
        result = read_changelog(initialized_root, backend_id="", limit=0)
        for i in range(1, len(result)):
            assert result[i].sequence > result[i - 1].sequence

    def test_read_changelog_not_initialized(self, uninitialized_root):
        """Raises LedgerNotInitializedError when .ledger/ does not exist."""
        with pytest.raises(LedgerNotInitializedError):
            read_changelog(uninitialized_root, backend_id="", limit=0)

    def test_read_changelog_filter_and_limit_combined(self, initialized_root):
        """Filter by backend_id and limit work together."""
        meta_a = _make_metadata("backend-a", owner_component="svc-a")
        register_backend(initialized_root, meta_a, actor="admin")
        # Store multiple schemas for backend-a to generate more entries
        for table in ["t1", "t2", "t3", "t4", "t5"]:
            store_schema(initialized_root, "backend-a", table, VALID_YAML, actor="dev")
        meta_b = _make_metadata("backend-b", owner_component="svc-b")
        register_backend(initialized_root, meta_b, actor="admin")

        result = read_changelog(initialized_root, backend_id="backend-a", limit=2)
        assert len(result) == 2
        assert all(e.backend_id == "backend-a" for e in result)

    def test_read_changelog_schema_added_entries(self, populated_root):
        """Changelog contains both backend_registered and schema_added entries."""
        result = read_changelog(populated_root, backend_id="", limit=0)
        change_types = [e.change_type for e in result]
        assert ChangeType.backend_registered in change_types
        assert ChangeType.schema_added in change_types


# ===========================================================================
# 9. Parametrized not_initialized tests
# ===========================================================================

class TestNotInitializedAllFunctions:
    """All public functions raise LedgerNotInitializedError on uninitialized root."""

    def test_register_backend_not_initialized(self, uninitialized_root):
        with pytest.raises(LedgerNotInitializedError):
            register_backend(uninitialized_root, _make_metadata(), "admin")

    def test_store_schema_not_initialized(self, uninitialized_root):
        with pytest.raises(LedgerNotInitializedError):
            store_schema(uninitialized_root, "x", "t", VALID_YAML, "dev")

    def test_list_backends_not_initialized(self, uninitialized_root):
        with pytest.raises(LedgerNotInitializedError):
            list_backends(uninitialized_root)

    def test_list_schemas_not_initialized(self, uninitialized_root):
        with pytest.raises(LedgerNotInitializedError):
            list_schemas(uninitialized_root, "x")

    def test_get_schema_not_initialized(self, uninitialized_root):
        with pytest.raises(LedgerNotInitializedError):
            get_schema(uninitialized_root, "x", "t")

    def test_validate_all_not_initialized(self, uninitialized_root):
        with pytest.raises(LedgerNotInitializedError):
            validate_all(uninitialized_root)

    def test_read_changelog_not_initialized(self, uninitialized_root):
        with pytest.raises(LedgerNotInitializedError):
            read_changelog(uninitialized_root, backend_id="", limit=0)


# ===========================================================================
# 10. Invariant tests
# ===========================================================================

class TestInvariants:
    def test_backend_id_unique(self, initialized_root):
        """Backend IDs are unique across the entire registry."""
        meta = _make_metadata(backend_id="unique-db")
        register_backend(initialized_root, meta, actor="admin")
        with pytest.raises((DuplicateBackendError, LedgerError)):
            register_backend(initialized_root, meta, actor="admin")

    def test_ownership_exclusivity(self, initialized_root):
        """No two components may own the same backend."""
        meta_a = _make_metadata(backend_id="owned-db", owner_component="component-a")
        register_backend(initialized_root, meta_a, actor="admin")
        meta_b = _make_metadata(backend_id="owned-db", owner_component="component-b")
        with pytest.raises((DuplicateBackendError, OwnershipConflictError)):
            register_backend(initialized_root, meta_b, actor="admin")

    def test_changelog_append_only(self, initialized_root):
        """Changelog entries from earlier operations persist after later operations."""
        e1 = register_backend(initialized_root,
                              _make_metadata("db-first", owner_component="svc-1"), "admin")
        entries_after_1 = read_changelog(initialized_root, backend_id="", limit=0)
        assert len(entries_after_1) == 1

        e2 = register_backend(initialized_root,
                              _make_metadata("db-second", owner_component="svc-2"), "admin")
        entries_after_2 = read_changelog(initialized_root, backend_id="", limit=0)
        assert len(entries_after_2) == 2
        # First entry unchanged
        assert entries_after_2[0].sequence == entries_after_1[0].sequence
        assert entries_after_2[0].backend_id == entries_after_1[0].backend_id

    def test_sequence_no_gaps(self, initialized_root):
        """Changelog sequence numbers are 1,2,...,n with no gaps."""
        register_backend(initialized_root,
                         _make_metadata("db-a", owner_component="svc-a"), "admin")
        register_backend(initialized_root,
                         _make_metadata("db-b", owner_component="svc-b"), "admin")
        store_schema(initialized_root, "db-a", "t1", VALID_YAML, actor="dev")
        store_schema(initialized_root, "db-b", "t2", VALID_YAML, actor="dev")

        entries = read_changelog(initialized_root, backend_id="", limit=0)
        sequences = [e.sequence for e in entries]
        assert sequences == list(range(1, len(sequences) + 1))

    def test_schema_verbatim_invariant(self, initialized_root):
        """Schema YAML is stored verbatim — never normalized or reformatted."""
        meta = _make_metadata(backend_id="verbatim-db")
        register_backend(initialized_root, meta, actor="admin")
        # Deliberately use unusual but valid YAML formatting
        raw = b"# Leading comment\nfoo:   bar\nbaz:  'qux'\nlist:\n - 1\n - 2\n"
        store_schema(initialized_root, "verbatim-db", "mytable", raw, actor="dev")
        record = get_schema(initialized_root, "verbatim-db", "mytable")
        assert record is not None
        assert record.raw_content == raw

    def test_validate_all_valid_only_when_no_errors(self, initialized_root):
        """ValidationResult.valid is True iff zero error-severity violations."""
        result = validate_all(initialized_root)
        error_count = sum(1 for v in result.violations if v.severity == ViolationSeverity.error)
        if error_count == 0:
            assert result.valid is True
        else:
            assert result.valid is False

    def test_init_idempotent_no_data_loss(self, initialized_root):
        """Calling init again does not destroy existing data."""
        meta = _make_metadata(backend_id="survive-db")
        register_backend(initialized_root, meta, actor="admin")
        store_schema(initialized_root, "survive-db", "t1", VALID_YAML, actor="dev")

        # Call init again
        init(initialized_root)

        # Data should still be there
        backends = list_backends(initialized_root)
        assert any(b.backend_id == "survive-db" for b in backends)
        schema = get_schema(initialized_root, "survive-db", "t1")
        assert schema is not None
        assert schema.raw_content == VALID_YAML

    def test_register_list_roundtrip(self, initialized_root):
        """Registering backends and listing them returns matching data."""
        ids = ["aaa-db", "bbb-db", "ccc-db"]
        for bid in ids:
            register_backend(initialized_root,
                             _make_metadata(bid, owner_component=f"svc-{bid}"), "admin")
        result = list_backends(initialized_root)
        result_ids = [b.backend_id for b in result]
        assert result_ids == sorted(ids)

    def test_store_get_roundtrip(self, initialized_root):
        """store_schema + get_schema round-trip preserves bytes exactly."""
        meta = _make_metadata(backend_id="roundtrip-db")
        register_backend(initialized_root, meta, actor="admin")
        raw = b"exact: bytes\npreserved: true\n# with comment\n"
        store_schema(initialized_root, "roundtrip-db", "tbl", raw, actor="dev")
        record = get_schema(initialized_root, "roundtrip-db", "tbl")
        assert record is not None
        assert record.raw_content == raw
        assert record.backend_id == "roundtrip-db"
        assert record.table_name == "tbl"
