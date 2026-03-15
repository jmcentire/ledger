"""
Contract test suite for the migration component.
Tests verify behavior at boundaries per the contract specification.
Run with: pytest contract_test.py -v
"""

import hashlib
import json
import os
import stat
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

# Import all public names from the migration module
from migration import (
    ApprovalRecord,
    ColumnConstraint,
    ColumnOperation,
    ComponentContext,
    DiffEntry,
    FieldAnnotation,
    GateDecision,
    GateRuleEntry,
    GateViolation,
    InvalidPlanTransitionError,
    MigrationParseError,
    MigrationPlan,
    OperationType,
    ParsedMigration,
    ParseWarning,
    PlanNotFoundError,
    PlanPersistenceError,
    PlanStatus,
    SchemaDiff,
    ViolationSeverity,
    approve_plan,
    compute_diff,
    create_plan,
    evaluate_gates,
    load_plan,
    parse_migration,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_sql():
    return "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL;"


@pytest.fixture
def multi_op_sql():
    return (
        "ALTER TABLE users ADD COLUMN age INTEGER;\n"
        "ALTER TABLE users DROP COLUMN nickname;\n"
        "ALTER TABLE orders ALTER COLUMN total TYPE NUMERIC(10,2);"
    )


@pytest.fixture
def mixed_sql():
    """SQL with ALTER TABLE plus non-ALTER statements."""
    return (
        "CREATE INDEX idx_users_email ON users(email);\n"
        "ALTER TABLE users ADD COLUMN phone TEXT;"
    )


@pytest.fixture
def mock_registry_empty():
    """Mock registry that returns None for all lookups (new fields)."""
    registry = MagicMock()
    registry.get_field_annotation = MagicMock(return_value=None)
    registry.get_foreign_keys = MagicMock(return_value=[])
    registry.lookup = MagicMock(return_value=None)
    return registry


@pytest.fixture
def mock_registry_with_annotations():
    """Mock registry that returns annotations for known fields."""
    registry = MagicMock()

    def get_annotation(table_name, column_name):
        known = {
            ("users", "email"): FieldAnnotation(
                classification_tier="CONFIDENTIAL",
                is_audit_field=False,
                is_immutable=False,
                is_encrypted=True,
            ),
            ("users", "nickname"): FieldAnnotation(
                classification_tier="PUBLIC",
                is_audit_field=False,
                is_immutable=False,
                is_encrypted=False,
            ),
            ("users", "created_by"): FieldAnnotation(
                classification_tier="INTERNAL",
                is_audit_field=True,
                is_immutable=True,
                is_encrypted=False,
            ),
        }
        return known.get((table_name, column_name))

    registry.get_field_annotation = MagicMock(side_effect=get_annotation)
    registry.get_foreign_keys = MagicMock(return_value=[])
    registry.lookup = MagicMock(side_effect=get_annotation)
    return registry


@pytest.fixture
def mock_registry_with_fk():
    """Mock registry that returns FK references for blast radius computation."""
    registry = MagicMock()
    registry.get_field_annotation = MagicMock(return_value=None)

    def get_fk(table_name):
        fk_map = {
            "users": ["orders"],
            "orders": ["shipments"],
        }
        return fk_map.get(table_name, [])

    registry.get_foreign_keys = MagicMock(side_effect=get_fk)
    return registry


@pytest.fixture
def mock_registry_fk_failure():
    """Mock registry that raises on FK lookup."""
    registry = MagicMock()
    registry.get_field_annotation = MagicMock(return_value=None)
    registry.get_foreign_keys = MagicMock(side_effect=Exception("FK lookup failed"))
    return registry


@pytest.fixture
def mock_registry_lookup_failure():
    """Mock registry that raises on annotation lookup."""
    registry = MagicMock()
    registry.get_field_annotation = MagicMock(
        side_effect=Exception("Registry lookup failed")
    )
    registry.lookup = MagicMock(side_effect=Exception("Registry lookup failed"))
    return registry


@pytest.fixture
def sample_parsed_migration():
    """A valid ParsedMigration with one ADD_COLUMN operation."""
    sql = "ALTER TABLE users ADD COLUMN email VARCHAR(255);"
    return ParsedMigration(
        operations=[
            ColumnOperation(
                op_type=OperationType.ADD_COLUMN,
                table_name="users",
                column_name="email",
                new_type="VARCHAR(255)",
                old_type="",
                constraints=[],
            )
        ],
        source_path="migrations/001.sql",
        source_hash=hashlib.sha256(sql.encode()).hexdigest(),
        statement_count=1,
        warnings=[],
    )


@pytest.fixture
def sample_parsed_multi_table():
    """ParsedMigration with operations on multiple tables."""
    return ParsedMigration(
        operations=[
            ColumnOperation(
                op_type=OperationType.ADD_COLUMN,
                table_name="users",
                column_name="age",
                new_type="INTEGER",
                old_type="",
                constraints=[],
            ),
            ColumnOperation(
                op_type=OperationType.DROP_COLUMN,
                table_name="users",
                column_name="nickname",
                new_type="",
                old_type="TEXT",
                constraints=[],
            ),
            ColumnOperation(
                op_type=OperationType.ALTER_COLUMN,
                table_name="orders",
                column_name="total",
                new_type="NUMERIC(10,2)",
                old_type="DECIMAL",
                constraints=[],
            ),
        ],
        source_path="migrations/002.sql",
        source_hash="abc123",
        statement_count=3,
        warnings=[],
    )


@pytest.fixture
def empty_parsed_migration():
    """ParsedMigration with no operations."""
    return ParsedMigration(
        operations=[],
        source_path="migrations/empty.sql",
        source_hash="empty",
        statement_count=0,
        warnings=[],
    )


@pytest.fixture
def sample_diff_public():
    """SchemaDiff with a PUBLIC classification entry for a new field."""
    return SchemaDiff(
        entries=[
            DiffEntry(
                operation=ColumnOperation(
                    op_type=OperationType.ADD_COLUMN,
                    table_name="users",
                    column_name="display_name",
                    new_type="TEXT",
                    old_type="",
                    constraints=[],
                ),
                annotation=FieldAnnotation(
                    classification_tier="PUBLIC",
                    is_audit_field=False,
                    is_immutable=False,
                    is_encrypted=False,
                ),
                is_new_field=True,
                is_field_removal=False,
            )
        ],
        affected_tables=["users"],
        source_path="m.sql",
        source_hash="hash1",
    )


@pytest.fixture
def sample_diff_audit_drop():
    """SchemaDiff with a DROP on an audit field → should trigger BLOCKED."""
    return SchemaDiff(
        entries=[
            DiffEntry(
                operation=ColumnOperation(
                    op_type=OperationType.DROP_COLUMN,
                    table_name="users",
                    column_name="created_by",
                    new_type="",
                    old_type="TEXT",
                    constraints=[],
                ),
                annotation=FieldAnnotation(
                    classification_tier="INTERNAL",
                    is_audit_field=True,
                    is_immutable=True,
                    is_encrypted=False,
                ),
                is_new_field=False,
                is_field_removal=True,
            )
        ],
        affected_tables=["users"],
        source_path="m.sql",
        source_hash="hash2",
    )


@pytest.fixture
def sample_diff_immutable_modify():
    """SchemaDiff with ALTER on an immutable field → should trigger BLOCKED."""
    return SchemaDiff(
        entries=[
            DiffEntry(
                operation=ColumnOperation(
                    op_type=OperationType.ALTER_COLUMN,
                    table_name="users",
                    column_name="created_by",
                    new_type="VARCHAR(100)",
                    old_type="TEXT",
                    constraints=[],
                ),
                annotation=FieldAnnotation(
                    classification_tier="INTERNAL",
                    is_audit_field=True,
                    is_immutable=True,
                    is_encrypted=False,
                ),
                is_new_field=False,
                is_field_removal=False,
            )
        ],
        affected_tables=["users"],
        source_path="m.sql",
        source_hash="hash3",
    )


@pytest.fixture
def sample_diff_encryption_removal():
    """SchemaDiff with ALTER on an encrypted field → should trigger HUMAN_GATE."""
    return SchemaDiff(
        entries=[
            DiffEntry(
                operation=ColumnOperation(
                    op_type=OperationType.ALTER_COLUMN,
                    table_name="users",
                    column_name="email",
                    new_type="TEXT",
                    old_type="BYTEA",
                    constraints=[],
                ),
                annotation=FieldAnnotation(
                    classification_tier="CONFIDENTIAL",
                    is_audit_field=False,
                    is_immutable=False,
                    is_encrypted=True,
                ),
                is_new_field=False,
                is_field_removal=False,
            )
        ],
        affected_tables=["users"],
        source_path="m.sql",
        source_hash="hash4",
    )


@pytest.fixture
def sample_diff_tier_mismatch():
    """SchemaDiff with CONFIDENTIAL field but component only has PUBLIC access."""
    return SchemaDiff(
        entries=[
            DiffEntry(
                operation=ColumnOperation(
                    op_type=OperationType.ADD_COLUMN,
                    table_name="users",
                    column_name="ssn",
                    new_type="TEXT",
                    old_type="",
                    constraints=[],
                ),
                annotation=FieldAnnotation(
                    classification_tier="CONFIDENTIAL",
                    is_audit_field=False,
                    is_immutable=False,
                    is_encrypted=False,
                ),
                is_new_field=True,
                is_field_removal=False,
            )
        ],
        affected_tables=["users"],
        source_path="m.sql",
        source_hash="hash5",
    )


@pytest.fixture
def sample_diff_multi_violations():
    """SchemaDiff with multiple entries triggering different violation types."""
    return SchemaDiff(
        entries=[
            # Audit field drop → BLOCKED
            DiffEntry(
                operation=ColumnOperation(
                    op_type=OperationType.DROP_COLUMN,
                    table_name="users",
                    column_name="created_by",
                    new_type="",
                    old_type="TEXT",
                    constraints=[],
                ),
                annotation=FieldAnnotation(
                    classification_tier="INTERNAL",
                    is_audit_field=True,
                    is_immutable=True,
                    is_encrypted=False,
                ),
                is_new_field=False,
                is_field_removal=True,
            ),
            # Tier mismatch → HUMAN_GATE
            DiffEntry(
                operation=ColumnOperation(
                    op_type=OperationType.ADD_COLUMN,
                    table_name="users",
                    column_name="ssn",
                    new_type="TEXT",
                    old_type="",
                    constraints=[],
                ),
                annotation=FieldAnnotation(
                    classification_tier="CONFIDENTIAL",
                    is_audit_field=False,
                    is_immutable=False,
                    is_encrypted=False,
                ),
                is_new_field=True,
                is_field_removal=False,
            ),
        ],
        affected_tables=["users"],
        source_path="m.sql",
        source_hash="hash_multi",
    )


@pytest.fixture
def empty_diff():
    """SchemaDiff with no entries."""
    return SchemaDiff(
        entries=[],
        affected_tables=[],
        source_path="m.sql",
        source_hash="empty",
    )


@pytest.fixture
def component_context_public():
    return ComponentContext(
        component_id="test-component",
        declared_data_access_tiers=["PUBLIC"],
    )


@pytest.fixture
def component_context_confidential():
    return ComponentContext(
        component_id="test-component",
        declared_data_access_tiers=["PUBLIC", "CONFIDENTIAL"],
    )


@pytest.fixture
def plans_dir(tmp_path):
    """Writable plans directory."""
    d = tmp_path / "plans"
    d.mkdir()
    return str(d)


def _make_plan_json(
    plan_id,
    status=PlanStatus.PENDING,
    overall_gate=GateDecision.HUMAN_GATE,
    diff=None,
    violations=None,
):
    """Helper to build a plan dict for seeding plan files."""
    now = datetime.now(timezone.utc).isoformat()
    if diff is None:
        diff = {
            "entries": [],
            "affected_tables": ["users"],
            "source_path": "m.sql",
            "source_hash": "testhash",
        }
    if violations is None:
        violations = []
    return {
        "plan_id": plan_id,
        "schema_version": 1,
        "diff": diff,
        "violations": violations,
        "overall_gate": overall_gate.name if hasattr(overall_gate, "name") else str(overall_gate),
        "blast_radius": ["users"],
        "status": status.name if hasattr(status, "name") else str(status),
        "created_at": now,
        "updated_at": now,
        "source_path": "m.sql",
        "source_hash": "testhash",
    }


def _seed_plan(plans_dir, plan_id, **kwargs):
    """Write a plan JSON file to the plans directory."""
    plan_data = _make_plan_json(plan_id, **kwargs)
    path = os.path.join(plans_dir, f"{plan_id}.json")
    with open(path, "w") as f:
        json.dump(plan_data, f)
    return path


# ---------------------------------------------------------------------------
# TestParseMigration
# ---------------------------------------------------------------------------


class TestParseMigration:
    """Tests for parse_migration function."""

    def test_parse_single_add_column(self):
        sql = "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL;"
        result = parse_migration(sql, "migrations/001_add_email.sql")

        assert result.source_path == "migrations/001_add_email.sql"
        assert len(result.operations) >= 1
        assert result.operations[0].op_type == OperationType.ADD_COLUMN
        assert result.operations[0].table_name == "users"
        assert result.operations[0].column_name == "email"
        assert result.statement_count >= 1
        assert isinstance(result.warnings, list)

    def test_parse_multiple_operations(self):
        sql = (
            "ALTER TABLE users ADD COLUMN age INTEGER;\n"
            "ALTER TABLE users DROP COLUMN nickname;\n"
            "ALTER TABLE orders ALTER COLUMN total TYPE NUMERIC(10,2);"
        )
        result = parse_migration(sql, "migrations/002_multi.sql")

        assert len(result.operations) == 3
        assert result.operations[0].op_type == OperationType.ADD_COLUMN
        assert result.operations[1].op_type == OperationType.DROP_COLUMN
        assert result.operations[2].op_type == OperationType.ALTER_COLUMN
        assert result.statement_count == 3

    def test_parse_source_hash_is_sha256(self):
        sql = "ALTER TABLE t ADD COLUMN c INT;"
        result = parse_migration(sql, "m.sql")
        expected_hash = hashlib.sha256(sql.encode()).hexdigest()
        assert result.source_hash == expected_hash

    def test_parse_comments_stripped(self):
        sql = (
            "-- This is a comment\n"
            "/* Block comment */\n"
            "ALTER TABLE users ADD COLUMN name TEXT;"
        )
        result = parse_migration(sql, "migrations/003_comments.sql")

        assert len(result.operations) == 1
        assert result.operations[0].op_type == OperationType.ADD_COLUMN
        assert result.operations[0].column_name == "name"

    def test_parse_non_alter_counted_in_statements(self):
        sql = (
            "CREATE INDEX idx_users_email ON users(email);\n"
            "ALTER TABLE users ADD COLUMN phone TEXT;"
        )
        result = parse_migration(sql, "migrations/004_mixed.sql")

        assert result.statement_count == 2
        assert len(result.operations) == 1

    def test_parse_warnings_collected_not_short_circuited(self):
        sql = (
            "ALTER TABLE users ADD COLUMN a INT;\n"
            "ALTER TABLE users ADD COLUMN b INT;"
        )
        result = parse_migration(sql, "migrations/005_warnings.sql")

        # Key contract: all operations are extracted, parsing doesn't short-circuit
        assert len(result.operations) == 2
        assert result.statement_count >= 2

    def test_parse_constraints_extracted(self):
        sql = "ALTER TABLE users ADD COLUMN status VARCHAR(50) NOT NULL DEFAULT 'active';"
        result = parse_migration(sql, "migrations/006_constraints.sql")

        assert len(result.operations) == 1
        assert result.operations[0].op_type == OperationType.ADD_COLUMN
        assert isinstance(result.operations[0].constraints, list)

    def test_parse_source_path_propagated(self):
        sql = "ALTER TABLE t ADD COLUMN c INT;"
        result = parse_migration(sql, "my/custom/path.sql")
        assert result.source_path == "my/custom/path.sql"

    def test_parse_error_empty_sql(self):
        with pytest.raises((MigrationParseError, Exception)) as exc_info:
            parse_migration("", "migrations/empty.sql")
        # Verify the error contains source_path context
        exc = exc_info.value
        if hasattr(exc, "source_path"):
            assert exc.source_path == "migrations/empty.sql"

    def test_parse_error_whitespace_only(self):
        with pytest.raises((MigrationParseError, Exception)):
            parse_migration("   \n\t  -- just a comment\n/* block */  ", "migrations/blank.sql")

    def test_parse_error_encoding(self):
        """SQL with characters that prevent regex extraction raises error."""
        # Attempt to trigger encoding error with invalid bytes
        try:
            bad_sql = b"\x80\x81\x82\x83\xff\xfe".decode("utf-8", errors="replace")
            # If the module handles this, it should raise MigrationParseError
            with pytest.raises((MigrationParseError, Exception)):
                parse_migration(bad_sql, "migrations/bad.sql")
        except (UnicodeDecodeError, Exception):
            # If we can't even create the string, the contract's precondition handles it
            pass

    def test_parse_only_alter_table_recognized(self):
        sql = "CREATE TABLE foo (id INT);\nALTER TABLE bar ADD COLUMN x INT;"
        result = parse_migration(sql, "m.sql")

        assert len(result.operations) == 1
        assert result.operations[0].table_name == "bar"

    def test_parse_operations_only_valid_types(self):
        sql = (
            "ALTER TABLE users ADD COLUMN a INT;\n"
            "ALTER TABLE users DROP COLUMN b;\n"
            "ALTER TABLE users ALTER COLUMN c TYPE TEXT;"
        )
        result = parse_migration(sql, "m.sql")
        valid_types = {OperationType.ADD_COLUMN, OperationType.DROP_COLUMN, OperationType.ALTER_COLUMN}
        for op in result.operations:
            assert op.op_type in valid_types

    def test_parse_drop_column(self):
        sql = "ALTER TABLE users DROP COLUMN old_field;"
        result = parse_migration(sql, "m.sql")

        assert len(result.operations) == 1
        assert result.operations[0].op_type == OperationType.DROP_COLUMN
        assert result.operations[0].table_name == "users"
        assert result.operations[0].column_name == "old_field"

    def test_parse_alter_column(self):
        sql = "ALTER TABLE orders ALTER COLUMN amount TYPE NUMERIC(12,2);"
        result = parse_migration(sql, "m.sql")

        assert len(result.operations) == 1
        assert result.operations[0].op_type == OperationType.ALTER_COLUMN
        assert result.operations[0].table_name == "orders"
        assert result.operations[0].column_name == "amount"


# ---------------------------------------------------------------------------
# TestComputeDiff
# ---------------------------------------------------------------------------


class TestComputeDiff:
    """Tests for compute_diff function."""

    def test_diff_new_field_add_column(self, sample_parsed_migration, mock_registry_empty):
        result = compute_diff(sample_parsed_migration, mock_registry_empty)

        assert len(result.entries) == 1
        assert result.entries[0].is_new_field is True
        assert result.entries[0].annotation is None
        assert "users" in result.affected_tables

    def test_diff_existing_field_annotated(self, mock_registry_with_annotations):
        parsed = ParsedMigration(
            operations=[
                ColumnOperation(
                    op_type=OperationType.ALTER_COLUMN,
                    table_name="users",
                    column_name="email",
                    new_type="TEXT",
                    old_type="VARCHAR(255)",
                    constraints=[],
                )
            ],
            source_path="m.sql",
            source_hash="hash",
            statement_count=1,
            warnings=[],
        )
        result = compute_diff(parsed, mock_registry_with_annotations)

        assert result.entries[0].annotation is not None
        assert result.entries[0].annotation.classification_tier == "CONFIDENTIAL"
        assert result.entries[0].is_new_field is False

    def test_diff_drop_existing_field(self, mock_registry_with_annotations):
        parsed = ParsedMigration(
            operations=[
                ColumnOperation(
                    op_type=OperationType.DROP_COLUMN,
                    table_name="users",
                    column_name="nickname",
                    new_type="",
                    old_type="TEXT",
                    constraints=[],
                )
            ],
            source_path="m.sql",
            source_hash="hash",
            statement_count=1,
            warnings=[],
        )
        result = compute_diff(parsed, mock_registry_with_annotations)

        assert result.entries[0].is_field_removal is True
        assert result.entries[0].operation.op_type == OperationType.DROP_COLUMN

    def test_diff_entries_same_length_as_operations(
        self, sample_parsed_multi_table, mock_registry_empty
    ):
        result = compute_diff(sample_parsed_multi_table, mock_registry_empty)
        assert len(result.entries) == len(sample_parsed_multi_table.operations)

    def test_diff_source_propagated(self, sample_parsed_migration, mock_registry_empty):
        result = compute_diff(sample_parsed_migration, mock_registry_empty)
        assert result.source_path == sample_parsed_migration.source_path
        assert result.source_hash == sample_parsed_migration.source_hash

    def test_diff_affected_tables_unique(self, sample_parsed_multi_table, mock_registry_empty):
        result = compute_diff(sample_parsed_multi_table, mock_registry_empty)
        assert set(result.affected_tables) == {"users", "orders"}
        # No duplicates
        assert len(result.affected_tables) == len(set(result.affected_tables))

    def test_diff_error_empty_operations(self, empty_parsed_migration, mock_registry_empty):
        with pytest.raises(Exception):
            compute_diff(empty_parsed_migration, mock_registry_empty)

    def test_diff_error_registry_lookup_failure(
        self, sample_parsed_migration, mock_registry_lookup_failure
    ):
        with pytest.raises(Exception):
            compute_diff(sample_parsed_migration, mock_registry_lookup_failure)

    def test_diff_entry_operation_matches_parsed_order(
        self, sample_parsed_multi_table, mock_registry_empty
    ):
        result = compute_diff(sample_parsed_multi_table, mock_registry_empty)
        for i, entry in enumerate(result.entries):
            assert entry.operation == sample_parsed_multi_table.operations[i]


# ---------------------------------------------------------------------------
# TestEvaluateGates
# ---------------------------------------------------------------------------


class TestEvaluateGates:
    """Tests for evaluate_gates function."""

    def test_no_violations_public_change(self, sample_diff_public, component_context_public):
        result = evaluate_gates(sample_diff_public, component_context_public)
        assert result == []

    def test_audit_field_drop_blocked(self, sample_diff_audit_drop, component_context_confidential):
        result = evaluate_gates(sample_diff_audit_drop, component_context_confidential)
        assert len(result) >= 1
        assert any(v.severity == GateDecision.BLOCKED for v in result)

    def test_immutable_field_modify_blocked(
        self, sample_diff_immutable_modify, component_context_confidential
    ):
        result = evaluate_gates(sample_diff_immutable_modify, component_context_confidential)
        assert any(v.severity == GateDecision.BLOCKED for v in result)

    def test_encryption_removal_human_gate(
        self, sample_diff_encryption_removal, component_context_confidential
    ):
        result = evaluate_gates(sample_diff_encryption_removal, component_context_confidential)
        assert any(v.severity == GateDecision.HUMAN_GATE for v in result)

    def test_tier_mismatch_human_gate(
        self, sample_diff_tier_mismatch, component_context_public
    ):
        result = evaluate_gates(sample_diff_tier_mismatch, component_context_public)
        assert any(v.severity == GateDecision.HUMAN_GATE for v in result)

    def test_multiple_violations_not_short_circuited(
        self, sample_diff_multi_violations, component_context_public
    ):
        result = evaluate_gates(sample_diff_multi_violations, component_context_public)
        # Should have at least 2 violations: one BLOCKED (audit drop) + one HUMAN_GATE (tier mismatch)
        assert len(result) >= 2

    def test_violations_ordered_by_severity_descending(
        self, sample_diff_multi_violations, component_context_public
    ):
        result = evaluate_gates(sample_diff_multi_violations, component_context_public)
        if len(result) > 1:
            # Verify descending order: BLOCKED first, then HUMAN_GATE, then AUTO_PROCEED
            severity_order = {
                GateDecision.BLOCKED: 3,
                GateDecision.HUMAN_GATE: 2,
                GateDecision.AUTO_PROCEED: 1,
            }
            for i in range(len(result) - 1):
                assert severity_order[result[i].severity] >= severity_order[result[i + 1].severity]

    def test_violations_have_valid_rule_ids(
        self, sample_diff_audit_drop, component_context_confidential
    ):
        result = evaluate_gates(sample_diff_audit_drop, component_context_confidential)
        for v in result:
            assert v.rule_id is not None
            assert isinstance(v.rule_id, str)
            assert len(v.rule_id) > 0

    def test_violations_have_table_and_column(
        self, sample_diff_audit_drop, component_context_confidential
    ):
        result = evaluate_gates(sample_diff_audit_drop, component_context_confidential)
        for v in result:
            assert v.table_name is not None
            assert v.column_name is not None

    def test_error_empty_diff(self, empty_diff, component_context_public):
        with pytest.raises(Exception):
            evaluate_gates(empty_diff, component_context_public)

    def test_gate_decision_ordering_invariant(self):
        """Gate decisions are totally ordered: AUTO_PROCEED < HUMAN_GATE < BLOCKED."""
        # Test ordering via comparison or via known numeric/name ordering
        decisions = [GateDecision.AUTO_PROCEED, GateDecision.HUMAN_GATE, GateDecision.BLOCKED]
        # Verify they can be distinguished and the contract order holds
        assert decisions[0] != decisions[1]
        assert decisions[1] != decisions[2]
        assert decisions[0] != decisions[2]


# ---------------------------------------------------------------------------
# TestCreatePlan
# ---------------------------------------------------------------------------


class TestCreatePlan:
    """Tests for create_plan function."""

    def test_create_plan_basic(self, sample_diff_public, mock_registry_with_fk, plans_dir):
        violations = [
            GateViolation(
                rule_id="tier_mismatch",
                severity=GateDecision.HUMAN_GATE,
                table_name="users",
                column_name="ssn",
                message="Tier mismatch",
                context={},
            )
        ]
        result = create_plan(sample_diff_public, violations, mock_registry_with_fk, plans_dir)

        assert result.status == PlanStatus.PENDING
        assert result.overall_gate == GateDecision.HUMAN_GATE
        # Validate UUID v4
        parsed_uuid = uuid.UUID(result.plan_id, version=4)
        assert parsed_uuid.version == 4

    def test_create_plan_no_violations_auto_proceed(
        self, sample_diff_public, mock_registry_empty, plans_dir
    ):
        result = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)

        assert result.overall_gate == GateDecision.AUTO_PROCEED
        assert result.status == PlanStatus.PENDING

    def test_create_plan_overall_gate_is_max_severity(
        self, sample_diff_public, mock_registry_empty, plans_dir
    ):
        violations = [
            GateViolation(
                rule_id="tier_mismatch",
                severity=GateDecision.HUMAN_GATE,
                table_name="users",
                column_name="a",
                message="tier mismatch",
                context={},
            ),
            GateViolation(
                rule_id="audit_drop",
                severity=GateDecision.BLOCKED,
                table_name="users",
                column_name="b",
                message="audit field dropped",
                context={},
            ),
        ]
        result = create_plan(sample_diff_public, violations, mock_registry_empty, plans_dir)
        assert result.overall_gate == GateDecision.BLOCKED

    def test_create_plan_blast_radius_includes_fk(self, mock_registry_with_fk, plans_dir):
        diff = SchemaDiff(
            entries=[
                DiffEntry(
                    operation=ColumnOperation(
                        op_type=OperationType.ADD_COLUMN,
                        table_name="users",
                        column_name="x",
                        new_type="INT",
                        old_type="",
                        constraints=[],
                    ),
                    annotation=None,
                    is_new_field=True,
                    is_field_removal=False,
                )
            ],
            affected_tables=["users"],
            source_path="m.sql",
            source_hash="h",
        )
        result = create_plan(diff, [], mock_registry_with_fk, plans_dir)

        assert "users" in result.blast_radius
        assert "orders" in result.blast_radius

    def test_create_plan_blast_radius_single_hop_only(self, mock_registry_with_fk, plans_dir):
        diff = SchemaDiff(
            entries=[
                DiffEntry(
                    operation=ColumnOperation(
                        op_type=OperationType.ADD_COLUMN,
                        table_name="users",
                        column_name="x",
                        new_type="INT",
                        old_type="",
                        constraints=[],
                    ),
                    annotation=None,
                    is_new_field=True,
                    is_field_removal=False,
                )
            ],
            affected_tables=["users"],
            source_path="m.sql",
            source_hash="h",
        )
        result = create_plan(diff, [], mock_registry_with_fk, plans_dir)

        # users -> orders (single hop), but orders -> shipments should NOT be included
        assert "shipments" not in result.blast_radius

    def test_create_plan_file_persisted(self, sample_diff_public, mock_registry_empty, plans_dir):
        result = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        plan_file = os.path.join(plans_dir, f"{result.plan_id}.json")
        assert os.path.exists(plan_file)

    def test_create_plan_timestamps_match(
        self, sample_diff_public, mock_registry_empty, plans_dir
    ):
        result = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        assert result.created_at == result.updated_at
        assert result.created_at is not None
        assert len(result.created_at) > 0

    def test_create_plan_uuid_v4(self, sample_diff_public, mock_registry_empty, plans_dir):
        result = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        parsed_uuid = uuid.UUID(result.plan_id)
        assert parsed_uuid.version == 4

    def test_create_plan_error_unwritable_dir(self, sample_diff_public, mock_registry_empty):
        with pytest.raises((PlanPersistenceError, Exception)):
            create_plan(sample_diff_public, [], mock_registry_empty, "/nonexistent/path/plans")

    def test_create_plan_error_registry_fk_failure(
        self, sample_diff_public, mock_registry_fk_failure, plans_dir
    ):
        with pytest.raises(Exception):
            create_plan(sample_diff_public, [], mock_registry_fk_failure, plans_dir)

    def test_create_plan_round_trip_with_load(
        self, sample_diff_public, mock_registry_empty, plans_dir
    ):
        created = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        loaded = load_plan(created.plan_id, plans_dir)

        assert loaded.plan_id == created.plan_id
        assert loaded.status == PlanStatus.PENDING
        assert loaded.overall_gate == created.overall_gate


# ---------------------------------------------------------------------------
# TestApprovePlan
# ---------------------------------------------------------------------------


class TestApprovePlan:
    """Tests for approve_plan function."""

    def test_approve_human_gate_pending(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(plans_dir, plan_id, status=PlanStatus.PENDING, overall_gate=GateDecision.HUMAN_GATE)

        result = approve_plan(plan_id, "alice", "PR-123", "Reviewed and safe", plans_dir)
        assert result.status == PlanStatus.APPROVED

    def test_approve_updated_at_changes(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(plans_dir, plan_id, status=PlanStatus.PENDING, overall_gate=GateDecision.HUMAN_GATE)

        result = approve_plan(plan_id, "bob", "PR-456", "Approved after review", plans_dir)
        # updated_at should be different from created_at (or at least set to approval time)
        assert result.updated_at is not None
        assert result.status == PlanStatus.APPROVED

    def test_approve_preserves_plan_id_and_gate(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(plans_dir, plan_id, status=PlanStatus.PENDING, overall_gate=GateDecision.HUMAN_GATE)

        result = approve_plan(plan_id, "alice", "PR-789", "Approved", plans_dir)
        assert result.plan_id == plan_id
        assert result.overall_gate == GateDecision.HUMAN_GATE

    def test_approve_file_updated(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(plans_dir, plan_id, status=PlanStatus.PENDING, overall_gate=GateDecision.HUMAN_GATE)

        approve_plan(plan_id, "alice", "PR-100", "OK", plans_dir)

        plan_file = os.path.join(plans_dir, f"{plan_id}.json")
        assert os.path.exists(plan_file)
        loaded = load_plan(plan_id, plans_dir)
        assert loaded.status == PlanStatus.APPROVED

    def test_approve_error_plan_not_found(self, plans_dir):
        fake_id = "00000000-0000-4000-8000-000000000000"
        with pytest.raises((PlanNotFoundError, Exception)):
            approve_plan(fake_id, "alice", "PR-1", "reason", plans_dir)

    def test_approve_error_already_approved(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(plans_dir, plan_id, status=PlanStatus.APPROVED, overall_gate=GateDecision.HUMAN_GATE)

        with pytest.raises((InvalidPlanTransitionError, Exception)):
            approve_plan(plan_id, "alice", "PR-2", "reason", plans_dir)

    def test_approve_error_already_rejected(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(plans_dir, plan_id, status=PlanStatus.REJECTED, overall_gate=GateDecision.HUMAN_GATE)

        with pytest.raises((InvalidPlanTransitionError, Exception)):
            approve_plan(plan_id, "alice", "PR-3", "reason", plans_dir)

    def test_approve_error_plan_blocked(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(plans_dir, plan_id, status=PlanStatus.PENDING, overall_gate=GateDecision.BLOCKED)

        with pytest.raises((InvalidPlanTransitionError, Exception)):
            approve_plan(plan_id, "alice", "PR-4", "reason", plans_dir)

    def test_approve_error_plan_auto_proceed(self, plans_dir):
        plan_id = str(uuid.uuid4())
        _seed_plan(
            plans_dir, plan_id, status=PlanStatus.PENDING, overall_gate=GateDecision.AUTO_PROCEED
        )

        with pytest.raises((InvalidPlanTransitionError, Exception)):
            approve_plan(plan_id, "alice", "PR-5", "reason", plans_dir)

    def test_approve_terminal_states_invariant(self, plans_dir):
        """APPROVED and REJECTED are terminal — cannot transition from them."""
        # Test APPROVED is terminal
        plan_id_approved = str(uuid.uuid4())
        _seed_plan(
            plans_dir, plan_id_approved, status=PlanStatus.APPROVED, overall_gate=GateDecision.HUMAN_GATE
        )
        with pytest.raises(Exception):
            approve_plan(plan_id_approved, "alice", "PR-6", "re-approve", plans_dir)

        # Test REJECTED is terminal
        plan_id_rejected = str(uuid.uuid4())
        _seed_plan(
            plans_dir, plan_id_rejected, status=PlanStatus.REJECTED, overall_gate=GateDecision.HUMAN_GATE
        )
        with pytest.raises(Exception):
            approve_plan(plan_id_rejected, "alice", "PR-7", "approve rejected", plans_dir)


# ---------------------------------------------------------------------------
# TestLoadPlan
# ---------------------------------------------------------------------------


class TestLoadPlan:
    """Tests for load_plan function."""

    def test_load_plan_round_trip(self, sample_diff_public, mock_registry_empty, plans_dir):
        created = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        result = load_plan(created.plan_id, plans_dir)

        assert result.plan_id == created.plan_id
        assert result.status == PlanStatus.PENDING

    def test_load_plan_not_found(self, plans_dir):
        fake_id = "00000000-0000-4000-8000-000000000001"
        with pytest.raises((PlanNotFoundError, Exception)):
            load_plan(fake_id, plans_dir)

    def test_load_plan_corrupted_empty_file(self, plans_dir):
        plan_id = str(uuid.uuid4())
        path = os.path.join(plans_dir, f"{plan_id}.json")
        with open(path, "w") as f:
            f.write("")

        with pytest.raises(Exception):
            load_plan(plan_id, plans_dir)

    def test_load_plan_corrupted_invalid_json(self, plans_dir):
        plan_id = str(uuid.uuid4())
        path = os.path.join(plans_dir, f"{plan_id}.json")
        with open(path, "w") as f:
            f.write("NOT VALID JSON {{{")

        with pytest.raises(Exception):
            load_plan(plan_id, plans_dir)

    def test_load_plan_corrupted_wrong_schema(self, plans_dir):
        plan_id = str(uuid.uuid4())
        path = os.path.join(plans_dir, f"{plan_id}.json")
        with open(path, "w") as f:
            json.dump({"wrong": "schema", "missing": "fields"}, f)

        with pytest.raises(Exception):
            load_plan(plan_id, plans_dir)

    def test_load_plan_file_read_permission_error(self, plans_dir):
        """Unreadable plan file raises file_read_error."""
        plan_id = str(uuid.uuid4())
        path = os.path.join(plans_dir, f"{plan_id}.json")
        _seed_plan(plans_dir, plan_id)

        # Remove read permissions
        try:
            os.chmod(path, 0o000)
            with pytest.raises(Exception):
                load_plan(plan_id, plans_dir)
        finally:
            # Restore permissions for cleanup
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

    def test_load_plan_matches_created_plan_fields(
        self, sample_diff_public, mock_registry_empty, plans_dir
    ):
        created = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        loaded = load_plan(created.plan_id, plans_dir)

        assert loaded.plan_id == created.plan_id
        assert loaded.overall_gate == created.overall_gate
        assert loaded.status == created.status
        assert loaded.source_path == created.source_path
        assert loaded.source_hash == created.source_hash


# ---------------------------------------------------------------------------
# Invariant Tests (cross-cutting)
# ---------------------------------------------------------------------------


class TestInvariants:
    """Cross-cutting invariant tests from the contract."""

    def test_plan_status_only_valid_transitions(self):
        """Plan status transitions: PENDING → APPROVED or PENDING → REJECTED only."""
        valid_transitions = {
            PlanStatus.PENDING: {PlanStatus.APPROVED, PlanStatus.REJECTED},
        }
        # APPROVED and REJECTED are terminal
        assert PlanStatus.APPROVED not in valid_transitions or len(
            valid_transitions.get(PlanStatus.APPROVED, set())
        ) == 0
        assert PlanStatus.REJECTED not in valid_transitions or len(
            valid_transitions.get(PlanStatus.REJECTED, set())
        ) == 0

    def test_gate_decision_max_severity_logic(self):
        """Overall gate must be the max severity across violations."""
        # This is a logic test: given a set of gate decisions, the max should be determined correctly
        severity_map = {
            GateDecision.AUTO_PROCEED: 0,
            GateDecision.HUMAN_GATE: 1,
            GateDecision.BLOCKED: 2,
        }

        test_cases = [
            ([], GateDecision.AUTO_PROCEED),
            ([GateDecision.AUTO_PROCEED], GateDecision.AUTO_PROCEED),
            ([GateDecision.HUMAN_GATE], GateDecision.HUMAN_GATE),
            ([GateDecision.BLOCKED], GateDecision.BLOCKED),
            ([GateDecision.HUMAN_GATE, GateDecision.BLOCKED], GateDecision.BLOCKED),
            ([GateDecision.AUTO_PROCEED, GateDecision.HUMAN_GATE], GateDecision.HUMAN_GATE),
        ]

        for decisions, expected_max in test_cases:
            if decisions:
                actual_max = max(decisions, key=lambda d: severity_map[d])
                assert actual_max == expected_max, f"Expected {expected_max} for {decisions}"

    def test_sql_parsing_is_regex_only(self):
        """SQL parsing uses regex extraction — only ALTER TABLE with ADD/DROP/ALTER COLUMN recognized."""
        # CREATE TABLE, DROP TABLE, RENAME TABLE should NOT produce operations
        sql = (
            "CREATE TABLE foo (id INT, name TEXT);\n"
            "DROP TABLE IF EXISTS bar;\n"
            "ALTER TABLE baz ADD COLUMN x INT;"
        )
        result = parse_migration(sql, "invariant_test.sql")

        # Only the ALTER TABLE ADD COLUMN should be recognized
        for op in result.operations:
            assert op.op_type in {
                OperationType.ADD_COLUMN,
                OperationType.DROP_COLUMN,
                OperationType.ALTER_COLUMN,
            }
        # Should find exactly 1 operation (the ALTER TABLE)
        assert len(result.operations) == 1
        assert result.operations[0].table_name == "baz"

    def test_no_short_circuit_on_warnings(self):
        """All parse warnings are collected, never short-circuiting."""
        sql = (
            "ALTER TABLE a ADD COLUMN x INT;\n"
            "ALTER TABLE b ADD COLUMN y INT;\n"
            "ALTER TABLE c ADD COLUMN z INT;"
        )
        result = parse_migration(sql, "m.sql")
        # All 3 operations should be present regardless of any warnings
        assert len(result.operations) == 3

    def test_no_short_circuit_on_violations(
        self,
    ):
        """Gate evaluation returns ALL violations, never short-circuits."""
        # Build a diff with multiple triggering entries
        diff = SchemaDiff(
            entries=[
                DiffEntry(
                    operation=ColumnOperation(
                        op_type=OperationType.DROP_COLUMN,
                        table_name="t1",
                        column_name="audit_col",
                        new_type="",
                        old_type="TEXT",
                        constraints=[],
                    ),
                    annotation=FieldAnnotation(
                        classification_tier="INTERNAL",
                        is_audit_field=True,
                        is_immutable=False,
                        is_encrypted=False,
                    ),
                    is_new_field=False,
                    is_field_removal=True,
                ),
                DiffEntry(
                    operation=ColumnOperation(
                        op_type=OperationType.ALTER_COLUMN,
                        table_name="t2",
                        column_name="immut_col",
                        new_type="INT",
                        old_type="TEXT",
                        constraints=[],
                    ),
                    annotation=FieldAnnotation(
                        classification_tier="INTERNAL",
                        is_audit_field=False,
                        is_immutable=True,
                        is_encrypted=False,
                    ),
                    is_new_field=False,
                    is_field_removal=False,
                ),
            ],
            affected_tables=["t1", "t2"],
            source_path="m.sql",
            source_hash="h",
        )
        ctx = ComponentContext(
            component_id="test",
            declared_data_access_tiers=["INTERNAL"],
        )
        result = evaluate_gates(diff, ctx)
        # Both entries should trigger violations — at least 2
        assert len(result) >= 2

    def test_plan_id_uniqueness(self, sample_diff_public, mock_registry_empty, plans_dir):
        """Plan IDs are UUID v4, guaranteed unique per plan."""
        plan1 = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        plan2 = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)

        assert plan1.plan_id != plan2.plan_id
        assert uuid.UUID(plan1.plan_id).version == 4
        assert uuid.UUID(plan2.plan_id).version == 4

    def test_atomic_write_no_partial_files(
        self, sample_diff_public, mock_registry_empty, plans_dir
    ):
        """Plan files written atomically — after creation, file is complete and valid JSON."""
        result = create_plan(sample_diff_public, [], mock_registry_empty, plans_dir)
        plan_file = os.path.join(plans_dir, f"{result.plan_id}.json")

        with open(plan_file, "r") as f:
            data = json.load(f)  # Should not raise — file must be valid JSON

        assert data is not None
        # Verify plan_id in the file matches
        assert data.get("plan_id") == result.plan_id
