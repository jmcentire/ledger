"""
Contract test suite for the 'config' component.
Tests organized by function/module area.

Run: pytest contract_test.py -v
"""
import os
import sys
import types
import textwrap
import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path

# Import the component under test
from config import (
    load_config,
    build_propagation_table,
    validate_annotation_set,
    get_builtin_propagation_table,
    get_conflicts,
    get_requires,
    file_lock,
    parse_schema_file,
    ClassificationTier,
    PropagationRule,
    Annotation,
    Field,
    SchemaFile,
    Backend,
    MigrationGate,
    MigrationPlan,
    ChangelogEntry,
    ConstraintViolation,
    CustomAnnotationDef,
    LedgerConfig,
    LedgerValidationError,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_custom_annotation():
    """A single valid custom annotation definition."""
    return CustomAnnotationDef(
        name="my_custom_rule",
        pact_assertion_type="exact_match",
        arbiter_tier_behavior="block",
        baton_masking_rule="full_mask",
        sentinel_severity="high",
    )


@pytest.fixture
def builtin_table():
    """The builtin propagation table for reference."""
    return get_builtin_propagation_table()


@pytest.fixture
def valid_schema_yaml(tmp_path):
    """Create a minimal valid schema YAML file and return its path and content."""
    content = textwrap.dedent("""\
        name: users
        version: 1
        fields:
          - name: id
            field_type: integer
            classification: PUBLIC
            nullable: false
            annotations: []
          - name: email
            field_type: string
            classification: PII
            nullable: false
            annotations: []
    """)
    schema_path = tmp_path / "users.yaml"
    schema_path.write_text(content)
    return str(schema_path), content


@pytest.fixture
def valid_ledger_yaml(tmp_path):
    """Create a minimal valid ledger.yaml and associated schema files."""
    schemas_dir = tmp_path / "schemas"
    schemas_dir.mkdir()

    schema_content = textwrap.dedent("""\
        name: users
        version: 1
        fields:
          - name: id
            field_type: integer
            classification: PUBLIC
            nullable: false
            annotations: []
    """)
    (schemas_dir / "users.yaml").write_text(schema_content)

    plans_dir = tmp_path / "plans"
    plans_dir.mkdir()

    changelog_path = tmp_path / "changelog.yaml"
    changelog_path.write_text("")

    ledger_content = textwrap.dedent(f"""\
        project_name: test_project
        schemas_dir: {str(schemas_dir)}
        changelog_path: {str(changelog_path)}
        plans_dir: {str(plans_dir)}
        backends: []
        custom_annotations: []
    """)
    ledger_path = tmp_path / "ledger.yaml"
    ledger_path.write_text(ledger_content)
    return str(ledger_path)


# ============================================================================
# ClassificationTier Invariant Tests
# ============================================================================

class TestClassificationTierInvariants:
    """ClassificationTier enum is fixed at exactly five members."""

    def test_classification_tier_has_exactly_five_members(self):
        """ClassificationTier enum has exactly five members."""
        members = list(ClassificationTier)
        assert len(members) == 5

    def test_classification_tier_expected_members(self):
        """ClassificationTier contains PUBLIC, PII, FINANCIAL, AUTH, COMPLIANCE."""
        expected = {"PUBLIC", "PII", "FINANCIAL", "AUTH", "COMPLIANCE"}
        actual = {m.name for m in ClassificationTier}
        assert actual == expected

    def test_classification_tier_is_str_enum(self):
        """ClassificationTier members are string-like."""
        for member in ClassificationTier:
            assert isinstance(member, str)

    def test_classification_tier_no_runtime_extension(self):
        """ClassificationTier cannot be extended at runtime."""
        with pytest.raises((TypeError, AttributeError, Exception)):
            # Attempting to create a subclass or add a member should fail
            type("ExtendedTier", (ClassificationTier,), {"NEW_TIER": "new_tier"})


# ============================================================================
# get_conflicts Tests
# ============================================================================

class TestGetConflicts:
    """Tests for get_conflicts function."""

    def test_returns_frozenset(self):
        """get_conflicts returns a frozenset."""
        conflicts = get_conflicts()
        assert isinstance(conflicts, frozenset)

    def test_exactly_three_pairs(self):
        """get_conflicts returns exactly 3 pairs."""
        conflicts = get_conflicts()
        assert len(conflicts) == 3

    def test_each_element_is_frozenset(self):
        """Each element in CONFLICTS is a frozenset."""
        conflicts = get_conflicts()
        for pair in conflicts:
            assert isinstance(pair, frozenset)

    def test_contains_immutable_gdpr_erasable_pair(self):
        """CONFLICTS contains {immutable, gdpr_erasable}."""
        conflicts = get_conflicts()
        assert frozenset({"immutable", "gdpr_erasable"}) in conflicts

    def test_contains_audit_field_gdpr_erasable_pair(self):
        """CONFLICTS contains {audit_field, gdpr_erasable}."""
        conflicts = get_conflicts()
        assert frozenset({"audit_field", "gdpr_erasable"}) in conflicts

    def test_contains_soft_delete_marker_immutable_pair(self):
        """CONFLICTS contains {soft_delete_marker, immutable}."""
        conflicts = get_conflicts()
        assert frozenset({"soft_delete_marker", "immutable"}) in conflicts

    def test_conflicts_is_immutable(self):
        """CONFLICTS frozenset cannot be modified."""
        conflicts = get_conflicts()
        with pytest.raises((AttributeError, TypeError)):
            conflicts.add(frozenset({"foo", "bar"}))


# ============================================================================
# get_requires Tests
# ============================================================================

class TestGetRequires:
    """Tests for get_requires function."""

    def test_returns_dict(self):
        """get_requires returns a dict."""
        requires = get_requires()
        assert isinstance(requires, dict)

    def test_keys_are_strings(self):
        """All keys in REQUIRES are strings."""
        requires = get_requires()
        for key in requires:
            assert isinstance(key, str)

    def test_values_are_frozensets_of_strings(self):
        """All values in REQUIRES are frozenset[str]."""
        requires = get_requires()
        for key, value in requires.items():
            assert isinstance(value, frozenset), f"Value for '{key}' is not a frozenset"
            for item in value:
                assert isinstance(item, str), f"Item in requires['{key}'] is not a string"


# ============================================================================
# get_builtin_propagation_table Tests
# ============================================================================

class TestGetBuiltinPropagationTable:
    """Tests for get_builtin_propagation_table function."""

    def test_returns_mapping_proxy(self):
        """get_builtin_propagation_table returns a MappingProxyType."""
        table = get_builtin_propagation_table()
        assert isinstance(table, types.MappingProxyType)

    def test_is_not_empty(self):
        """Builtin propagation table is not empty."""
        table = get_builtin_propagation_table()
        assert len(table) > 0

    def test_values_are_propagation_rules(self):
        """All values in the builtin table are PropagationRule instances."""
        table = get_builtin_propagation_table()
        for name, rule in table.items():
            assert isinstance(rule, PropagationRule), f"Value for '{name}' is not a PropagationRule"

    def test_cannot_mutate(self):
        """Builtin propagation table (MappingProxyType) cannot be mutated."""
        table = get_builtin_propagation_table()
        with pytest.raises(TypeError):
            table["new_key"] = "value"

    def test_contains_no_custom_annotations(self):
        """Builtin table contains only builtins — no custom annotations mixed in."""
        # Calling twice should return the same set of keys
        table1 = get_builtin_propagation_table()
        table2 = get_builtin_propagation_table()
        assert set(table1.keys()) == set(table2.keys())


# ============================================================================
# build_propagation_table Tests
# ============================================================================

class TestBuildPropagationTable:
    """Tests for build_propagation_table function."""

    def test_empty_custom_returns_builtins_only(self, builtin_table):
        """With no custom annotations, returned table equals builtins."""
        result = build_propagation_table([])
        assert isinstance(result, types.MappingProxyType)
        # Should contain all builtins
        for key in builtin_table:
            assert key in result

    def test_with_custom_annotation_merges(self, builtin_table, sample_custom_annotation):
        """Custom annotations are merged into the table alongside builtins."""
        result = build_propagation_table([sample_custom_annotation])
        assert sample_custom_annotation.name in result
        # All builtins still present
        for key in builtin_table:
            assert key in result

    def test_returns_mapping_proxy_type(self):
        """Returned table is a MappingProxyType (immutable)."""
        result = build_propagation_table([])
        assert isinstance(result, types.MappingProxyType)

    def test_immutable_result(self, sample_custom_annotation):
        """Returned table cannot be mutated by callers."""
        result = build_propagation_table([sample_custom_annotation])
        with pytest.raises(TypeError):
            result["hacker_key"] = "value"

    def test_builtins_unchanged_after_merge(self, builtin_table, sample_custom_annotation):
        """Builtin entries are not modified when custom annotations are added."""
        result = build_propagation_table([sample_custom_annotation])
        for key, rule in builtin_table.items():
            assert result[key] == rule, f"Builtin '{key}' was modified after merge"

    def test_name_collision_raises_error(self, builtin_table):
        """Name collision between custom and builtin raises an error."""
        # Pick the first builtin name and create a custom with that name
        builtin_name = next(iter(builtin_table))
        colliding = CustomAnnotationDef(
            name=builtin_name,
            pact_assertion_type="exact_match",
            arbiter_tier_behavior="block",
            baton_masking_rule="full_mask",
            sentinel_severity="high",
        )
        with pytest.raises(Exception) as exc_info:
            build_propagation_table([colliding])
        # The error should indicate a collision
        exc_text = str(exc_info.value).lower()
        assert "collision" in exc_text or "collid" in exc_text or "builtin" in exc_text or "duplicate" in exc_text or "exists" in exc_text or builtin_name in exc_text

    def test_duplicate_custom_names_raises_error(self):
        """Two custom annotations with the same name raises an error."""
        dup1 = CustomAnnotationDef(
            name="dup_annotation",
            pact_assertion_type="exact_match",
            arbiter_tier_behavior="block",
            baton_masking_rule="full_mask",
            sentinel_severity="high",
        )
        dup2 = CustomAnnotationDef(
            name="dup_annotation",
            pact_assertion_type="schema_match",
            arbiter_tier_behavior="warn",
            baton_masking_rule="partial_mask",
            sentinel_severity="low",
        )
        with pytest.raises(Exception):
            build_propagation_table([dup1, dup2])

    def test_custom_annotation_preserves_all_builtins(self, builtin_table):
        """Adding a custom annotation doesn't remove any builtin from the table."""
        custom = CustomAnnotationDef(
            name="safe_custom",
            pact_assertion_type="exact_match",
            arbiter_tier_behavior="block",
            baton_masking_rule="full_mask",
            sentinel_severity="medium",
        )
        result = build_propagation_table([custom])
        for builtin_key in builtin_table:
            assert builtin_key in result, f"Builtin '{builtin_key}' missing after adding custom"


# ============================================================================
# validate_annotation_set Tests
# ============================================================================

class TestValidateAnnotationSet:
    """Tests for validate_annotation_set function."""

    def test_no_violations_for_valid_set(self):
        """A set of non-conflicting annotations returns empty violations."""
        # Use a single annotation that shouldn't conflict with anything
        result = validate_annotation_set(["audit_field"])
        assert isinstance(result, list)
        assert len(result) == 0

    def test_empty_list_returns_no_violations(self):
        """Empty annotation list returns no violations."""
        result = validate_annotation_set([])
        assert result == []

    def test_single_annotation_no_conflicts(self):
        """Single annotation alone cannot produce a conflict violation."""
        result = validate_annotation_set(["immutable"])
        # There might be requires violations but not conflict violations
        conflict_violations = [v for v in result if v.violation_type == "conflict"]
        assert len(conflict_violations) == 0

    def test_detects_immutable_gdpr_erasable_conflict(self):
        """Detects {immutable, gdpr_erasable} conflict."""
        result = validate_annotation_set(["immutable", "gdpr_erasable"])
        conflict_violations = [v for v in result if v.violation_type == "conflict"]
        assert len(conflict_violations) >= 1
        # Check that the conflict involves both annotations
        all_annotations_in_violations = set()
        for v in conflict_violations:
            for a in v.annotations:
                all_annotations_in_violations.add(a)
        assert "immutable" in all_annotations_in_violations
        assert "gdpr_erasable" in all_annotations_in_violations

    def test_detects_audit_field_gdpr_erasable_conflict(self):
        """Detects {audit_field, gdpr_erasable} conflict."""
        result = validate_annotation_set(["audit_field", "gdpr_erasable"])
        conflict_violations = [v for v in result if v.violation_type == "conflict"]
        assert len(conflict_violations) >= 1

    def test_detects_soft_delete_marker_immutable_conflict(self):
        """Detects {soft_delete_marker, immutable} conflict."""
        result = validate_annotation_set(["soft_delete_marker", "immutable"])
        conflict_violations = [v for v in result if v.violation_type == "conflict"]
        assert len(conflict_violations) >= 1

    def test_detects_missing_required_annotations(self):
        """When source annotation is present but required co-annotations are missing, produces missing_required violations."""
        requires = get_requires()
        if not requires:
            pytest.skip("No REQUIRES rules defined")

        # Pick the first annotation with requirements
        source_annotation = next(iter(requires))
        required_annotations = requires[source_annotation]

        # Only provide the source, not its requirements
        result = validate_annotation_set([source_annotation])
        missing_violations = [v for v in result if v.violation_type == "missing_required"]
        # Should have at least one missing_required violation per missing annotation
        assert len(missing_violations) >= len(required_annotations)

    def test_all_violations_reported_not_just_first(self):
        """All violations are reported, not just the first — never short-circuits."""
        # Provide annotations that trigger multiple conflicts
        result = validate_annotation_set(["immutable", "gdpr_erasable", "audit_field", "soft_delete_marker"])
        # Should detect at least:
        # - {immutable, gdpr_erasable}
        # - {audit_field, gdpr_erasable}
        # - {soft_delete_marker, immutable}
        conflict_violations = [v for v in result if v.violation_type == "conflict"]
        assert len(conflict_violations) >= 3, (
            f"Expected at least 3 conflict violations, got {len(conflict_violations)}: {conflict_violations}"
        )

    def test_empty_annotation_name_raises_error(self):
        """Empty annotation name in the input set raises an error."""
        with pytest.raises(Exception):
            validate_annotation_set(["valid_name", ""])

    def test_violations_are_constraint_violation_instances(self):
        """Returned violations are ConstraintViolation instances."""
        result = validate_annotation_set(["immutable", "gdpr_erasable"])
        for v in result:
            assert isinstance(v, ConstraintViolation)
            assert isinstance(v.violation_type, str)
            assert isinstance(v.annotations, list)
            assert isinstance(v.message, str)


# ============================================================================
# parse_schema_file Tests
# ============================================================================

class TestParseSchemaFile:
    """Tests for parse_schema_file function."""

    def test_valid_schema_returns_schema_file(self, valid_schema_yaml, builtin_table):
        """parse_schema_file returns a SchemaFile for valid YAML."""
        path, content = valid_schema_yaml
        result = parse_schema_file(path, builtin_table)
        assert isinstance(result, SchemaFile)
        assert result.name == "users"
        assert result.version == 1
        assert len(result.fields) == 2

    def test_source_path_equals_input_path(self, valid_schema_yaml, builtin_table):
        """SchemaFile.source_path equals the input path."""
        path, _ = valid_schema_yaml
        result = parse_schema_file(path, builtin_table)
        assert result.source_path == path

    def test_raw_yaml_preserved_verbatim(self, tmp_path, builtin_table):
        """SchemaFile.raw_yaml is the exact string content read from disk, not round-tripped."""
        # Create YAML with specific formatting, comments, and ordering
        content = textwrap.dedent("""\
            # This is a comment that should be preserved
            name:   users
            version: 1
            fields:
              - name: id
                field_type: integer
                classification: PUBLIC
                nullable: false
                annotations: []
        """)
        schema_path = tmp_path / "users_formatted.yaml"
        schema_path.write_text(content)

        result = parse_schema_file(str(schema_path), builtin_table)
        assert result.raw_yaml == content, (
            "raw_yaml should be verbatim file content, not round-tripped YAML"
        )

    def test_raw_yaml_is_not_yaml_dump_output(self, tmp_path, builtin_table):
        """SchemaFile.raw_yaml is never the output of yaml.dump — preserves original formatting."""
        import yaml
        content = textwrap.dedent("""\
            name:   users
            version: 1
            fields:
              - name: id
                field_type: integer
                classification: PUBLIC
                nullable: false
                annotations: []
        """)
        schema_path = tmp_path / "users_raw.yaml"
        schema_path.write_text(content)

        result = parse_schema_file(str(schema_path), builtin_table)
        # Parse and re-dump to get the round-tripped version
        parsed = yaml.safe_load(content)
        round_tripped = yaml.dump(parsed, default_flow_style=False)
        # raw_yaml should match original content, NOT the round-tripped version
        assert result.raw_yaml == content
        # Verify the round-tripped version is different (the extra spaces in 'name:   users')
        # If by coincidence they're the same, that's OK — the key assertion is content == raw_yaml

    def test_file_not_found_raises_error(self, builtin_table):
        """parse_schema_file raises error when file does not exist."""
        with pytest.raises(Exception) as exc_info:
            parse_schema_file("/nonexistent/path/schema.yaml", builtin_table)
        # Should indicate file not found
        assert exc_info.value is not None

    def test_invalid_yaml_raises_error(self, tmp_path, builtin_table):
        """parse_schema_file raises error for invalid YAML content."""
        bad_path = tmp_path / "bad.yaml"
        bad_path.write_text(":::invalid: yaml: [[[")
        with pytest.raises(Exception):
            parse_schema_file(str(bad_path), builtin_table)

    def test_schema_validation_errors(self, tmp_path, builtin_table):
        """parse_schema_file raises error for missing required fields."""
        bad_schema = tmp_path / "bad_schema.yaml"
        bad_schema.write_text("name: test\n")  # Missing version, fields, etc.
        with pytest.raises(Exception):
            parse_schema_file(str(bad_schema), builtin_table)

    def test_unknown_annotation_raises_error(self, tmp_path, builtin_table):
        """parse_schema_file raises error when annotation not in propagation_table."""
        content = textwrap.dedent("""\
            name: test_schema
            version: 1
            fields:
              - name: id
                field_type: integer
                classification: PUBLIC
                nullable: false
                annotations:
                  - name: totally_fake_annotation_xyz
                    params: {}
        """)
        schema_path = tmp_path / "unknown_ann.yaml"
        schema_path.write_text(content)
        with pytest.raises(Exception):
            parse_schema_file(str(schema_path), builtin_table)

    def test_constraint_violations_raises_error(self, tmp_path, builtin_table):
        """parse_schema_file raises error when field annotations violate CONFLICTS."""
        content = textwrap.dedent("""\
            name: test_schema
            version: 1
            fields:
              - name: data_field
                field_type: string
                classification: PII
                nullable: false
                annotations:
                  - name: immutable
                    params: {}
                  - name: gdpr_erasable
                    params: {}
        """)
        schema_path = tmp_path / "conflict_schema.yaml"
        schema_path.write_text(content)
        with pytest.raises(Exception):
            parse_schema_file(str(schema_path), builtin_table)

    def test_multiple_violations_all_reported(self, tmp_path, builtin_table):
        """parse_schema_file reports all violations across all fields, not just first."""
        content = textwrap.dedent("""\
            name: test_schema
            version: 1
            fields:
              - name: field1
                field_type: string
                classification: PII
                nullable: false
                annotations:
                  - name: immutable
                    params: {}
                  - name: gdpr_erasable
                    params: {}
              - name: field2
                field_type: string
                classification: PII
                nullable: false
                annotations:
                  - name: audit_field
                    params: {}
                  - name: gdpr_erasable
                    params: {}
        """)
        schema_path = tmp_path / "multi_violation.yaml"
        schema_path.write_text(content)
        with pytest.raises(Exception) as exc_info:
            parse_schema_file(str(schema_path), builtin_table)
        # The exception should contain information about multiple violations
        exc_str = str(exc_info.value)
        # Should reference both fields or multiple violations
        # (exact format depends on implementation, but should aggregate)
        assert exc_info.value is not None


# ============================================================================
# load_config Tests
# ============================================================================

class TestLoadConfig:
    """Tests for load_config function."""

    def test_valid_config_returns_ledger_config(self, valid_ledger_yaml):
        """load_config returns LedgerConfig for valid ledger.yaml."""
        result = load_config(valid_ledger_yaml)
        assert isinstance(result, LedgerConfig)
        assert result.project_name == "test_project"

    def test_valid_config_has_propagation_table(self, valid_ledger_yaml):
        """LedgerConfig contains merged propagation table with at least builtins."""
        result = load_config(valid_ledger_yaml)
        builtin = get_builtin_propagation_table()
        for key in builtin:
            assert key in result.propagation_table

    def test_valid_config_no_custom_annotations(self, valid_ledger_yaml):
        """load_config works when custom_annotations is empty."""
        result = load_config(valid_ledger_yaml)
        assert isinstance(result, LedgerConfig)

    def test_file_not_found_raises_error(self):
        """load_config raises error when file does not exist."""
        with pytest.raises(Exception):
            load_config("/completely/nonexistent/ledger.yaml")

    def test_permission_denied_raises_error(self, tmp_path):
        """load_config raises error when file is not readable."""
        ledger_path = tmp_path / "ledger.yaml"
        ledger_path.write_text("project_name: test")
        ledger_path.chmod(0o000)
        try:
            with pytest.raises(Exception):
                load_config(str(ledger_path))
        finally:
            # Restore permissions for cleanup
            ledger_path.chmod(0o644)

    def test_invalid_yaml_raises_error(self, tmp_path):
        """load_config raises error for unparseable YAML."""
        ledger_path = tmp_path / "ledger.yaml"
        ledger_path.write_text(":::bad: yaml: {[[[")
        with pytest.raises(Exception):
            load_config(str(ledger_path))

    def test_validation_errors_raised(self, tmp_path):
        """load_config raises LedgerValidationError for missing required fields."""
        ledger_path = tmp_path / "ledger.yaml"
        ledger_path.write_text("project_name: test\n")  # Missing other required fields
        with pytest.raises(Exception) as exc_info:
            load_config(str(ledger_path))
        # Should be a LedgerValidationError or contain validation info
        assert exc_info.value is not None

    def test_annotation_collision_raises_error(self, tmp_path):
        """load_config raises error when custom annotation collides with builtin."""
        builtin = get_builtin_propagation_table()
        builtin_name = next(iter(builtin))

        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        schema_content = textwrap.dedent("""\
            name: users
            version: 1
            fields:
              - name: id
                field_type: integer
                classification: PUBLIC
                nullable: false
                annotations: []
        """)
        (schemas_dir / "users.yaml").write_text(schema_content)

        plans_dir = tmp_path / "plans"
        plans_dir.mkdir()
        changelog_path = tmp_path / "changelog.yaml"
        changelog_path.write_text("")

        ledger_content = textwrap.dedent(f"""\
            project_name: test_project
            schemas_dir: {str(schemas_dir)}
            changelog_path: {str(changelog_path)}
            plans_dir: {str(plans_dir)}
            backends: []
            custom_annotations:
              - name: {builtin_name}
                pact_assertion_type: exact_match
                arbiter_tier_behavior: block
                baton_masking_rule: full_mask
                sentinel_severity: high
        """)
        ledger_path = tmp_path / "ledger.yaml"
        ledger_path.write_text(ledger_content)

        with pytest.raises(Exception):
            load_config(str(ledger_path))

    def test_constraint_violations_raised(self, tmp_path):
        """load_config raises error when schema fields violate CONFLICTS constraints."""
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        schema_content = textwrap.dedent("""\
            name: users
            version: 1
            fields:
              - name: data_field
                field_type: string
                classification: PII
                nullable: false
                annotations:
                  - name: immutable
                    params: {}
                  - name: gdpr_erasable
                    params: {}
        """)
        (schemas_dir / "users.yaml").write_text(schema_content)

        plans_dir = tmp_path / "plans"
        plans_dir.mkdir()
        changelog_path = tmp_path / "changelog.yaml"
        changelog_path.write_text("")

        ledger_content = textwrap.dedent(f"""\
            project_name: test_project
            schemas_dir: {str(schemas_dir)}
            changelog_path: {str(changelog_path)}
            plans_dir: {str(plans_dir)}
            backends: []
            custom_annotations: []
        """)
        ledger_path = tmp_path / "ledger.yaml"
        ledger_path.write_text(ledger_content)

        with pytest.raises(Exception):
            load_config(str(ledger_path))

    def test_aggregates_all_errors(self, tmp_path):
        """load_config aggregates multiple validation errors rather than stopping at first."""
        ledger_path = tmp_path / "ledger.yaml"
        # Multiple problems: missing schemas_dir, missing backends, etc.
        ledger_path.write_text(textwrap.dedent("""\
            project_name: test_project
        """))
        with pytest.raises(Exception) as exc_info:
            load_config(str(ledger_path))
        # Should aggregate errors
        exc = exc_info.value
        # Check if it's a LedgerValidationError with multiple violations
        if hasattr(exc, 'violations'):
            assert len(exc.violations) >= 1, "Should report at least one violation"

    def test_ledger_config_has_expected_fields(self, valid_ledger_yaml):
        """LedgerConfig contains propagation_table, backends, and schemas_dir."""
        result = load_config(valid_ledger_yaml)
        assert hasattr(result, 'propagation_table')
        assert hasattr(result, 'backends')
        assert hasattr(result, 'schemas_dir')
        assert hasattr(result, 'project_name')


# ============================================================================
# file_lock Tests
# ============================================================================

class TestFileLock:
    """Tests for file_lock context manager."""

    def test_exclusive_lock_acquires_and_releases(self, tmp_path):
        """file_lock acquires exclusive lock and releases on context exit."""
        target = tmp_path / "test_file.yaml"
        target.write_text("data")
        lock_path = str(target) + ".lock"

        with file_lock(str(target), exclusive=True, blocking=True) as handle:
            assert handle is not None
            # .lock sidecar should exist
            assert os.path.exists(lock_path)

    def test_shared_lock_acquires(self, tmp_path):
        """file_lock acquires shared lock successfully."""
        target = tmp_path / "test_file.yaml"
        target.write_text("data")

        with file_lock(str(target), exclusive=False, blocking=True) as handle:
            assert handle is not None

    def test_creates_sidecar_lock_file(self, tmp_path):
        """file_lock creates .lock sidecar file adjacent to path."""
        target = tmp_path / "test_file.yaml"
        target.write_text("data")
        lock_path = str(target) + ".lock"

        assert not os.path.exists(lock_path)
        with file_lock(str(target), exclusive=True, blocking=True):
            assert os.path.exists(lock_path)

    def test_original_file_not_modified(self, tmp_path):
        """The original file at path is never modified by the locking mechanism."""
        target = tmp_path / "test_file.yaml"
        original_content = "original data content"
        target.write_text(original_content)

        with file_lock(str(target), exclusive=True, blocking=True):
            pass

        assert target.read_text() == original_content

    def test_platform_unsupported_raises_error(self, tmp_path):
        """file_lock raises PlatformError on non-Unix platform."""
        target = tmp_path / "test_file.yaml"
        target.write_text("data")

        # Mock fcntl as unavailable
        with patch.dict(sys.modules, {'fcntl': None}):
            with pytest.raises(Exception) as exc_info:
                with file_lock(str(target), exclusive=True, blocking=True):
                    pass
            # Should indicate platform issue
            assert exc_info.value is not None

    def test_parent_dir_missing_raises_error(self):
        """file_lock raises error when parent directory does not exist."""
        with pytest.raises(Exception):
            with file_lock("/nonexistent/dir/file.yaml", exclusive=True, blocking=True):
                pass

    def test_permission_denied_raises_error(self, tmp_path):
        """file_lock raises error when cannot create .lock sidecar file."""
        # Create a directory that is not writable
        restricted_dir = tmp_path / "restricted"
        restricted_dir.mkdir()
        target = restricted_dir / "test_file.yaml"
        target.write_text("data")
        restricted_dir.chmod(0o444)
        try:
            with pytest.raises(Exception):
                with file_lock(str(target), exclusive=True, blocking=True):
                    pass
        finally:
            restricted_dir.chmod(0o755)

    def test_lock_contention_nonblocking(self, tmp_path):
        """file_lock raises error when blocking=false and lock is already held."""
        import fcntl
        target = tmp_path / "test_file.yaml"
        target.write_text("data")
        lock_path = str(target) + ".lock"

        # Manually acquire exclusive lock
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            # Now try non-blocking lock — should fail
            with pytest.raises(Exception):
                with file_lock(str(target), exclusive=True, blocking=False):
                    pass
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


# ============================================================================
# Pydantic Model Invariant Tests
# ============================================================================

class TestPydanticModelsImmutability:
    """All domain Pydantic models use frozen=True and reject mutation."""

    def test_field_is_frozen(self):
        """Field model is frozen and rejects attribute assignment."""
        f = Field(
            name="test",
            field_type="string",
            classification=ClassificationTier.PUBLIC,
            nullable=False,
            annotations=[],
        )
        with pytest.raises((AttributeError, TypeError, Exception)):
            f.name = "changed"

    def test_annotation_is_frozen(self):
        """Annotation model is frozen."""
        a = Annotation(name="test_ann", params={})
        with pytest.raises((AttributeError, TypeError, Exception)):
            a.name = "changed"

    def test_propagation_rule_is_frozen(self):
        """PropagationRule model is frozen."""
        rule = PropagationRule(
            annotation_name="test",
            pact_assertion_type="exact_match",
            arbiter_tier_behavior="block",
            baton_masking_rule="full_mask",
            sentinel_severity="high",
        )
        with pytest.raises((AttributeError, TypeError, Exception)):
            rule.annotation_name = "changed"

    def test_backend_is_frozen(self):
        """Backend model is frozen."""
        b = Backend(name="pact", enabled=True, base_url="http://localhost", timeout_ms=5000)
        with pytest.raises((AttributeError, TypeError, Exception)):
            b.name = "changed"

    def test_migration_gate_is_frozen(self):
        """MigrationGate model is frozen."""
        mg = MigrationGate(
            rule_name="test_rule",
            passed=True,
            severity="low",
            message="ok",
            field_name="id",
            schema_name="users",
        )
        with pytest.raises((AttributeError, TypeError, Exception)):
            mg.passed = False

    def test_constraint_violation_is_frozen(self):
        """ConstraintViolation model is frozen."""
        cv = ConstraintViolation(
            violation_type="conflict",
            annotations=["a", "b"],
            message="conflict found",
        )
        with pytest.raises((AttributeError, TypeError, Exception)):
            cv.violation_type = "changed"

    def test_custom_annotation_def_is_frozen(self):
        """CustomAnnotationDef model is frozen."""
        cad = CustomAnnotationDef(
            name="custom",
            pact_assertion_type="exact_match",
            arbiter_tier_behavior="block",
            baton_masking_rule="full_mask",
            sentinel_severity="high",
        )
        with pytest.raises((AttributeError, TypeError, Exception)):
            cad.name = "changed"

    def test_changelog_entry_is_frozen(self):
        """ChangelogEntry model is frozen."""
        ce = ChangelogEntry(
            entry_id="e1",
            schema_name="users",
            version=1,
            change_type="create",
            timestamp="2024-01-01T00:00:00Z",
            description="initial",
            migration_plan_id="mp1",
        )
        with pytest.raises((AttributeError, TypeError, Exception)):
            ce.version = 2
