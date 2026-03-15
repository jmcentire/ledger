"""
Adversarial hidden acceptance tests for Configuration & Data Models.

These tests target gaps in visible test coverage, catching implementations
that hardcode returns or shortcut validation rather than truly satisfying
the contract.
"""
import os
import sys
import tempfile
import textwrap
from types import MappingProxyType
from unittest.mock import patch

import pytest

from src.config import (
    Annotation,
    Backend,
    ChangelogEntry,
    ClassificationTier,
    ConstraintViolation,
    CustomAnnotationDef,
    Field,
    LedgerConfig,
    MigrationGate,
    MigrationPlan,
    PropagationRule,
    SchemaFile,
    build_propagation_table,
    file_lock,
    get_builtin_propagation_table,
    get_conflicts,
    get_requires,
    parse_schema_file,
    validate_annotation_set,
)


# ---------------------------------------------------------------------------
# CONFLICTS invariant tests
# ---------------------------------------------------------------------------

class TestGoodhartConflicts:

    def test_goodhart_conflicts_pairs_are_frozensets_of_two(self):
        """Each element in CONFLICTS must be a frozenset of exactly 2 strings."""
        conflicts = get_conflicts()
        assert isinstance(conflicts, frozenset)
        for pair in conflicts:
            assert isinstance(pair, frozenset), f"Inner element {pair!r} is not a frozenset"
            assert len(pair) == 2, f"Inner element has {len(pair)} members, expected 2"
            for name in pair:
                assert isinstance(name, str), f"Element {name!r} in pair is not a str"

    def test_goodhart_conflicts_contains_specific_strings(self):
        """CONFLICTS must reference the exact annotation names from the contract."""
        conflicts = get_conflicts()
        all_names = set()
        for pair in conflicts:
            all_names.update(pair)
        assert "immutable" in all_names
        assert "gdpr_erasable" in all_names
        assert "audit_field" in all_names
        assert "soft_delete_marker" in all_names

    def test_goodhart_get_conflicts_called_twice_identical(self):
        """Multiple calls to get_conflicts must return equal frozensets."""
        c1 = get_conflicts()
        c2 = get_conflicts()
        assert c1 == c2
        assert isinstance(c1, frozenset)
        assert isinstance(c2, frozenset)


# ---------------------------------------------------------------------------
# REQUIRES invariant tests
# ---------------------------------------------------------------------------

class TestGoodhartRequires:

    def test_goodhart_get_requires_called_twice_identical(self):
        """Multiple calls to get_requires must return equal mappings."""
        r1 = get_requires()
        r2 = get_requires()
        assert r1 == r2

    def test_goodhart_requires_keys_are_strings(self):
        """All keys in REQUIRES must be non-empty strings; values must be frozenset[str]."""
        requires = get_requires()
        for k, v in requires.items():
            assert isinstance(k, str) and len(k) > 0, f"Key {k!r} invalid"
            assert isinstance(v, frozenset), f"Value for {k!r} is not a frozenset"
            for elem in v:
                assert isinstance(elem, str), f"Element {elem!r} in requires[{k!r}] is not str"

    def test_goodhart_builtin_table_has_requires_annotations(self):
        """Every annotation name that is a key in REQUIRES must exist in the builtin propagation table."""
        requires = get_requires()
        table = get_builtin_propagation_table()
        for key in requires:
            assert key in table, f"REQUIRES key {key!r} not found in builtin table"


# ---------------------------------------------------------------------------
# Builtin propagation table tests
# ---------------------------------------------------------------------------

class TestGoodhartBuiltinTable:

    def test_goodhart_builtin_table_values_are_propagation_rules(self):
        """Every value in the builtin table must be a PropagationRule with all required fields."""
        table = get_builtin_propagation_table()
        assert len(table) > 0, "Builtin table should not be empty"
        for key, rule in table.items():
            assert isinstance(rule, PropagationRule), f"Value for {key!r} is not PropagationRule"
            assert hasattr(rule, "annotation_name")
            assert hasattr(rule, "pact_assertion_type")
            assert hasattr(rule, "arbiter_tier_behavior")
            assert hasattr(rule, "baton_masking_rule")
            assert hasattr(rule, "sentinel_severity")

    def test_goodhart_builtin_table_keys_match_annotation_names(self):
        """Each key in the builtin table must equal the annotation_name of its PropagationRule."""
        table = get_builtin_propagation_table()
        for key, rule in table.items():
            assert key == rule.annotation_name, (
                f"Key {key!r} does not match rule.annotation_name {rule.annotation_name!r}"
            )

    def test_goodhart_builtin_table_has_conflict_annotations(self):
        """The builtin table must contain all annotations referenced in CONFLICTS."""
        table = get_builtin_propagation_table()
        conflicts = get_conflicts()
        for pair in conflicts:
            for name in pair:
                assert name in table, f"Conflict annotation {name!r} not in builtin table"

    def test_goodhart_builtin_table_non_empty(self):
        """The builtin table must have at least 4 entries (the conflict annotations at minimum)."""
        table = get_builtin_propagation_table()
        assert len(table) >= 4

    def test_goodhart_get_builtin_table_called_twice_returns_same(self):
        """Two calls to get_builtin_propagation_table should return tables with identical contents."""
        t1 = get_builtin_propagation_table()
        t2 = get_builtin_propagation_table()
        assert set(t1.keys()) == set(t2.keys())
        for k in t1:
            assert t1[k] == t2[k]


# ---------------------------------------------------------------------------
# build_propagation_table tests
# ---------------------------------------------------------------------------

class TestGoodhartBuildPropagationTable:

    def test_goodhart_build_table_multiple_custom_annotations(self):
        """Multiple distinct custom annotations should all appear in the merged table."""
        customs = [
            CustomAnnotationDef(
                name="custom_alpha",
                pact_assertion_type="check",
                arbiter_tier_behavior="allow",
                baton_masking_rule="none",
                sentinel_severity="low",
            ),
            CustomAnnotationDef(
                name="custom_beta",
                pact_assertion_type="verify",
                arbiter_tier_behavior="deny",
                baton_masking_rule="full",
                sentinel_severity="high",
            ),
            CustomAnnotationDef(
                name="custom_gamma",
                pact_assertion_type="assert",
                arbiter_tier_behavior="warn",
                baton_masking_rule="partial",
                sentinel_severity="medium",
            ),
        ]
        table = build_propagation_table(customs)
        builtins = get_builtin_propagation_table()
        for c in customs:
            assert c.name in table, f"Custom annotation {c.name!r} missing from merged table"
        for bk in builtins:
            assert bk in table, f"Builtin {bk!r} missing after merge"
        assert len(table) == len(builtins) + len(customs)

    def test_goodhart_build_table_custom_values_are_propagation_rules(self):
        """Custom annotations in the merged table must be PropagationRule instances."""
        customs = [
            CustomAnnotationDef(
                name="my_custom_rule",
                pact_assertion_type="check",
                arbiter_tier_behavior="allow",
                baton_masking_rule="none",
                sentinel_severity="low",
            ),
        ]
        table = build_propagation_table(customs)
        val = table["my_custom_rule"]
        assert isinstance(val, PropagationRule), (
            f"Expected PropagationRule, got {type(val).__name__}"
        )
        assert val.annotation_name == "my_custom_rule"

    def test_goodhart_build_table_collision_detects_any_builtin(self):
        """Collision detection must work for every builtin name, not just commonly tested ones."""
        builtins = get_builtin_propagation_table()
        # Pick a builtin name that's less likely to be hardcoded in visible tests
        all_builtin_names = list(builtins.keys())
        for bname in all_builtin_names:
            bad_custom = CustomAnnotationDef(
                name=bname,
                pact_assertion_type="check",
                arbiter_tier_behavior="allow",
                baton_masking_rule="none",
                sentinel_severity="low",
            )
            with pytest.raises(Exception):
                build_propagation_table([bad_custom])

    def test_goodhart_build_table_three_duplicate_custom_names(self):
        """Duplicate detection should work even with three or more customs sharing the same name."""
        customs = [
            CustomAnnotationDef(
                name="dup_name",
                pact_assertion_type="check",
                arbiter_tier_behavior="allow",
                baton_masking_rule="none",
                sentinel_severity="low",
            ),
            CustomAnnotationDef(
                name="dup_name",
                pact_assertion_type="verify",
                arbiter_tier_behavior="deny",
                baton_masking_rule="full",
                sentinel_severity="high",
            ),
            CustomAnnotationDef(
                name="dup_name",
                pact_assertion_type="assert",
                arbiter_tier_behavior="warn",
                baton_masking_rule="partial",
                sentinel_severity="medium",
            ),
        ]
        with pytest.raises(Exception):
            build_propagation_table(customs)

    def test_goodhart_build_table_returns_new_mapping_each_call(self):
        """Each call should return a separate mapping — different custom inputs should yield different tables."""
        c1 = [
            CustomAnnotationDef(
                name="ext_one",
                pact_assertion_type="check",
                arbiter_tier_behavior="allow",
                baton_masking_rule="none",
                sentinel_severity="low",
            ),
        ]
        c2 = [
            CustomAnnotationDef(
                name="ext_two",
                pact_assertion_type="check",
                arbiter_tier_behavior="allow",
                baton_masking_rule="none",
                sentinel_severity="low",
            ),
        ]
        t1 = build_propagation_table(c1)
        t2 = build_propagation_table(c2)
        assert "ext_one" in t1
        assert "ext_one" not in t2
        assert "ext_two" in t2
        assert "ext_two" not in t1


# ---------------------------------------------------------------------------
# validate_annotation_set tests
# ---------------------------------------------------------------------------

class TestGoodhartValidateAnnotationSet:

    def test_goodhart_validate_conflict_violation_type_field(self):
        """Conflict violation must have violation_type='conflict' and reference both annotations."""
        violations = validate_annotation_set(["immutable", "gdpr_erasable"])
        conflict_violations = [v for v in violations if v.violation_type == "conflict"]
        assert len(conflict_violations) == 1
        v = conflict_violations[0]
        assert "immutable" in v.annotations
        assert "gdpr_erasable" in v.annotations

    def test_goodhart_validate_missing_required_violation_type_field(self):
        """Missing required violation must have violation_type='missing_required'."""
        requires = get_requires()
        if not requires:
            pytest.skip("No REQUIRES rules defined")
        # Pick a key from REQUIRES and provide it without its requirements
        source = next(iter(requires))
        violations = validate_annotation_set([source])
        req_violations = [v for v in violations if v.violation_type == "missing_required"]
        # There should be at least one missing_required if the source has requirements
        if requires[source]:
            assert len(req_violations) >= 1
            for v in req_violations:
                assert isinstance(v.annotations, list)
                for a in v.annotations:
                    assert isinstance(a, str)

    def test_goodhart_validate_duplicate_annotations_in_input(self):
        """Duplicate annotation names should not cause duplicate violations."""
        v1 = validate_annotation_set(["immutable", "gdpr_erasable"])
        v2 = validate_annotation_set(["immutable", "gdpr_erasable", "immutable"])
        conflict_v1 = [v for v in v1 if v.violation_type == "conflict"]
        conflict_v2 = [v for v in v2 if v.violation_type == "conflict"]
        assert len(conflict_v1) == len(conflict_v2)

    def test_goodhart_validate_no_false_conflicts_for_non_conflicting(self):
        """audit_field + soft_delete_marker is NOT a defined conflict pair — should produce no conflict violations."""
        violations = validate_annotation_set(["audit_field", "soft_delete_marker"])
        conflict_violations = [v for v in violations if v.violation_type == "conflict"]
        assert len(conflict_violations) == 0, (
            f"False conflict detected for {{audit_field, soft_delete_marker}}: {conflict_violations}"
        )

    def test_goodhart_validate_conflict_is_symmetric(self):
        """Order of annotations should not affect conflict detection."""
        v1 = validate_annotation_set(["gdpr_erasable", "audit_field"])
        v2 = validate_annotation_set(["audit_field", "gdpr_erasable"])
        assert len(v1) == len(v2)
        types1 = sorted([v.violation_type for v in v1])
        types2 = sorted([v.violation_type for v in v2])
        assert types1 == types2

    def test_goodhart_validate_empty_string_among_valid(self):
        """Empty string detection should work even when mixed with valid annotations."""
        with pytest.raises(Exception):
            validate_annotation_set(["immutable", "", "audit_field"])

    def test_goodhart_validate_only_conflict_no_false_requires(self):
        """A pure conflict should not produce spurious missing_required violations unless genuinely violated."""
        requires = get_requires()
        # immutable + gdpr_erasable — check if either triggers REQUIRES
        violations = validate_annotation_set(["immutable", "gdpr_erasable"])
        for v in violations:
            if v.violation_type == "missing_required":
                # This is only valid if one of the two annotations actually has a REQUIRES rule
                # that is not satisfied by the other
                source_in_requires = any(
                    ann in requires for ann in ["immutable", "gdpr_erasable"]
                )
                assert source_in_requires, (
                    f"Spurious missing_required violation: {v}"
                )

    def test_goodhart_validate_requires_with_all_satisfied(self):
        """When all required co-annotations are present, no missing_required violations should appear."""
        requires = get_requires()
        if not requires:
            pytest.skip("No REQUIRES rules defined")
        # Build an annotation set that satisfies all REQUIRES
        source = next(iter(requires))
        needed = requires[source]
        full_set = [source] + list(needed)
        violations = validate_annotation_set(full_set)
        missing_req = [v for v in violations if v.violation_type == "missing_required"]
        # Filter to only violations related to our source annotation
        # There should be none for the source since we provided all requirements
        for v in missing_req:
            # If the source is in the violation annotations, it means we failed to provide something
            # But we provided everything, so this should not happen for our source
            if source in v.annotations:
                required_names = set(v.annotations) - {source}
                for rn in required_names:
                    assert rn not in needed, (
                        f"False missing_required for {source}: {rn} was provided"
                    )

    def test_goodhart_validate_annotations_unknown_names_no_crash(self):
        """Unknown annotation names should not cause crashes in validate_annotation_set."""
        # validate_annotation_set only checks CONFLICTS and REQUIRES; unknown names
        # should just pass through without errors (that's parse_schema_file's job)
        result = validate_annotation_set(["totally_unknown_xyz_annotation_42"])
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# ClassificationTier enum tests
# ---------------------------------------------------------------------------

class TestGoodhartClassificationTier:

    def test_goodhart_classification_tier_is_strenum(self):
        """ClassificationTier members must be instances of str (StrEnum)."""
        assert isinstance(ClassificationTier.PUBLIC, str)
        assert isinstance(ClassificationTier.PII, str)
        assert isinstance(ClassificationTier.FINANCIAL, str)
        assert isinstance(ClassificationTier.AUTH, str)
        assert isinstance(ClassificationTier.COMPLIANCE, str)

    def test_goodhart_classification_tier_membership_test(self):
        """Valid tier values should be members; invalid values should not be constructible."""
        valid_names = {"PUBLIC", "PII", "FINANCIAL", "AUTH", "COMPLIANCE"}
        actual_names = {m.name for m in ClassificationTier}
        assert actual_names == valid_names
        # Ensure non-members cannot be constructed
        with pytest.raises(Exception):
            ClassificationTier("SECRET")
        with pytest.raises(Exception):
            ClassificationTier("INTERNAL")

    def test_goodhart_classification_tier_values_are_strings(self):
        """Each ClassificationTier member's value should be a string."""
        for member in ClassificationTier:
            assert isinstance(member.value, str), f"{member.name}.value is not str"


# ---------------------------------------------------------------------------
# Frozen Pydantic model tests (beyond Field which visible tests cover)
# ---------------------------------------------------------------------------

class TestGoodhartFrozenModels:

    def test_goodhart_field_model_frozen_all_attrs(self):
        """Field model must reject assignment on all attributes, not just 'name'."""
        f = Field(
            name="test",
            field_type="string",
            classification=ClassificationTier.PUBLIC,
            nullable=False,
            annotations=[],
        )
        for attr in ("name", "field_type", "classification", "nullable", "annotations"):
            with pytest.raises(Exception):
                setattr(f, attr, "new_value")

    def test_goodhart_annotation_model_frozen(self):
        """Annotation model must be frozen on both fields."""
        a = Annotation(name="test_ann", params={})
        with pytest.raises(Exception):
            a.name = "changed"
        with pytest.raises(Exception):
            a.params = {"x": 1}

    def test_goodhart_schema_file_model_frozen(self):
        """SchemaFile must be frozen on all fields."""
        sf = SchemaFile(
            name="test_schema",
            version=1,
            fields=[],
            raw_yaml="raw content",
            source_path="/tmp/test.yaml",
        )
        for attr in ("name", "version", "fields", "raw_yaml", "source_path"):
            with pytest.raises(Exception):
                setattr(sf, attr, "new_value")

    def test_goodhart_backend_model_frozen(self):
        """Backend model must be frozen."""
        b = Backend(
            name="pact", enabled=True, base_url="http://localhost", timeout_ms=5000
        )
        with pytest.raises(Exception):
            b.enabled = False
        with pytest.raises(Exception):
            b.timeout_ms = 9999

    def test_goodhart_migration_gate_model_frozen(self):
        """MigrationGate must be frozen."""
        mg = MigrationGate(
            rule_name="r", passed=True, severity="low",
            message="ok", field_name="f", schema_name="s"
        )
        with pytest.raises(Exception):
            mg.passed = False
        with pytest.raises(Exception):
            mg.severity = "high"

    def test_goodhart_migration_plan_model_frozen(self):
        """MigrationPlan must be frozen."""
        mp = MigrationPlan(
            plan_id="p1", schema_name="s", from_version=1, to_version=2,
            gates=[], approved=False, created_at="2024-01-01T00:00:00Z"
        )
        with pytest.raises(Exception):
            mp.approved = True
        with pytest.raises(Exception):
            mp.plan_id = "changed"

    def test_goodhart_changelog_entry_model_frozen(self):
        """ChangelogEntry must be frozen."""
        ce = ChangelogEntry(
            entry_id="e1", schema_name="s", version=1,
            change_type="create", timestamp="2024-01-01T00:00:00Z",
            description="test", migration_plan_id="mp1"
        )
        with pytest.raises(Exception):
            ce.description = "changed"
        with pytest.raises(Exception):
            ce.change_type = "delete"

    def test_goodhart_constraint_violation_model_frozen(self):
        """ConstraintViolation must be frozen."""
        cv = ConstraintViolation(
            violation_type="conflict",
            annotations=["a", "b"],
            message="test conflict"
        )
        with pytest.raises(Exception):
            cv.violation_type = "changed"
        with pytest.raises(Exception):
            cv.message = "changed"

    def test_goodhart_propagation_rule_model_frozen(self):
        """PropagationRule must be frozen."""
        pr = PropagationRule(
            annotation_name="test",
            pact_assertion_type="check",
            arbiter_tier_behavior="allow",
            baton_masking_rule="none",
            sentinel_severity="low",
        )
        with pytest.raises(Exception):
            pr.annotation_name = "changed"
        with pytest.raises(Exception):
            pr.sentinel_severity = "high"

    def test_goodhart_ledger_config_model_frozen(self):
        """LedgerConfig must be frozen."""
        lc = LedgerConfig(
            project_name="test",
            schemas_dir="/tmp/schemas",
            changelog_path="/tmp/changelog.yaml",
            plans_dir="/tmp/plans",
            backends=[],
            custom_annotations=[],
            propagation_table={},
        )
        with pytest.raises(Exception):
            lc.project_name = "changed"
        with pytest.raises(Exception):
            lc.propagation_table = {"x": "y"}


# ---------------------------------------------------------------------------
# file_lock tests
# ---------------------------------------------------------------------------

class TestGoodhartFileLock:

    def test_goodhart_file_lock_sidecar_naming(self):
        """The .lock sidecar file should be adjacent to the given path with .lock extension."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "myfile.yaml")
            with open(target, "w") as f:
                f.write("content")
            expected_lock = target + ".lock"
            with file_lock(target, exclusive=True, blocking=True) as handle:
                assert os.path.exists(expected_lock), "Sidecar .lock file not created"
                assert handle.lock_path.endswith(".lock")
                # Sidecar should be in same directory
                assert os.path.dirname(handle.lock_path) == tmpdir

    def test_goodhart_file_lock_does_not_modify_original(self):
        """file_lock must never modify the original file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "data.yaml")
            original_content = "key: value\n# comment\n"
            with open(target, "w") as f:
                f.write(original_content)
            with file_lock(target, exclusive=True, blocking=True):
                pass
            with open(target, "r") as f:
                after_content = f.read()
            assert after_content == original_content

    def test_goodhart_file_lock_fd_closed_after_exit(self):
        """After exiting context, the fd should be closed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "locktest.yaml")
            with open(target, "w") as f:
                f.write("data")
            with file_lock(target, exclusive=True, blocking=True) as handle:
                fd = handle.fd
            # After exit, fd should be closed
            with pytest.raises(OSError):
                os.fstat(fd)

    def test_goodhart_file_lock_handle_has_expected_fields(self):
        """FileLockHandle must have lock_path (str), fd (int), exclusive (bool)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "test.yaml")
            with open(target, "w") as f:
                f.write("x")
            with file_lock(target, exclusive=True, blocking=True) as handle:
                assert isinstance(handle.lock_path, str)
                assert handle.lock_path.endswith(".lock")
                assert isinstance(handle.fd, int)
                assert isinstance(handle.exclusive, bool)
                assert handle.exclusive is True

            with file_lock(target, exclusive=False, blocking=True) as handle:
                assert handle.exclusive is False


# ---------------------------------------------------------------------------
# parse_schema_file tests
# ---------------------------------------------------------------------------

class TestGoodhartParseSchemaFile:

    def test_goodhart_parse_schema_raw_yaml_preserves_comments(self):
        """raw_yaml must preserve comments from the original YAML file."""
        table = build_propagation_table([])
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_path = os.path.join(tmpdir, "schema.yaml")
            content = textwrap.dedent("""\
                # This is a comment that should be preserved
                name: test_schema
                version: 1
                fields:
                  - name: id
                    field_type: integer
                    classification: PUBLIC
                    nullable: false
                    annotations: []
            """)
            with open(schema_path, "w") as f:
                f.write(content)
            result = parse_schema_file(schema_path, table)
            assert "# This is a comment" in result.raw_yaml
            assert result.raw_yaml == content

    def test_goodhart_parse_schema_preserves_trailing_newline(self):
        """raw_yaml must preserve trailing newlines exactly as on disk."""
        table = build_propagation_table([])
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_path = os.path.join(tmpdir, "schema.yaml")
            content = textwrap.dedent("""\
                name: trailing_test
                version: 1
                fields:
                  - name: id
                    field_type: integer
                    classification: PUBLIC
                    nullable: false
                    annotations: []


            """)
            # Content ends with two newlines
            with open(schema_path, "w") as f:
                f.write(content)
            result = parse_schema_file(schema_path, table)
            assert result.raw_yaml == content
            assert result.raw_yaml.endswith("\n\n")

    def test_goodhart_parse_schema_source_path_exact(self):
        """source_path must be exactly the input path string, not a resolved/canonicalized version."""
        table = build_propagation_table([])
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_path = os.path.join(tmpdir, "myschema.yaml")
            content = textwrap.dedent("""\
                name: path_test
                version: 1
                fields:
                  - name: val
                    field_type: string
                    classification: PUBLIC
                    nullable: true
                    annotations: []
            """)
            with open(schema_path, "w") as f:
                f.write(content)
            result = parse_schema_file(schema_path, table)
            assert result.source_path == schema_path

    def test_goodhart_parse_schema_unknown_annotation_detected(self):
        """parse_schema_file must detect annotations not in the propagation_table."""
        table = build_propagation_table([])
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_path = os.path.join(tmpdir, "bad_ann.yaml")
            content = textwrap.dedent("""\
                name: bad_ann_schema
                version: 1
                fields:
                  - name: data
                    field_type: string
                    classification: PUBLIC
                    nullable: false
                    annotations:
                      - name: completely_nonexistent_annotation_xyz
                        params: {}
            """)
            with open(schema_path, "w") as f:
                f.write(content)
            with pytest.raises(Exception):
                parse_schema_file(schema_path, table)

    def test_goodhart_parse_schema_conflict_across_fields(self):
        """parse_schema_file must validate annotation constraints for every field, not just the first."""
        table = build_propagation_table([])
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_path = os.path.join(tmpdir, "multi_field.yaml")
            # First field is valid, second field has a conflict
            content = textwrap.dedent("""\
                name: multi_field_schema
                version: 1
                fields:
                  - name: good_field
                    field_type: string
                    classification: PUBLIC
                    nullable: false
                    annotations: []
                  - name: bad_field
                    field_type: string
                    classification: PII
                    nullable: false
                    annotations:
                      - name: immutable
                        params: {{}}
                      - name: gdpr_erasable
                        params: {{}}
            """)
            with open(schema_path, "w") as f:
                f.write(content)
            with pytest.raises(Exception):
                parse_schema_file(schema_path, table)
