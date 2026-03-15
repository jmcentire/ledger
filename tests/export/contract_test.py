"""
Contract test suite for the 'export' component.
Tests organized by function: iter_propagation_entries, export_pact, export_arbiter,
export_baton, export_sentinel, yaml_dump, and cross-cutting invariants.

Run with: pytest contract_test.py -v
"""

import pytest
import yaml
from unittest.mock import MagicMock, patch
from copy import deepcopy

# Import all exports from the component
from export import (
    iter_propagation_entries,
    export_pact,
    export_arbiter,
    export_baton,
    export_sentinel,
    yaml_dump,
    ExportViolationSeverity,
    ExportViolation,
    PropagationEntry,
    PropagationEntryTuple,
    PactAssertion,
    PactExport,
    ArbiterRule,
    ArbiterExport,
    BatonEgressNode,
    BatonExport,
    SentinelSeverityMapping,
    SentinelExport,
    ExportResultPact,
    ExportResultArbiter,
    ExportResultBaton,
    ExportResultSentinel,
)


# ============================================================================
# Shared helpers and factories
# ============================================================================

def assert_violation(violation, severity=None, field_path=None, message_contains=None, annotation_key=None):
    """Consistent violation field assertions."""
    assert isinstance(violation, ExportViolation), f"Expected ExportViolation, got {type(violation)}"
    if severity is not None:
        assert violation.severity == severity, (
            f"Expected severity {severity}, got {violation.severity}"
        )
    if field_path is not None:
        assert violation.field_path == field_path, (
            f"Expected field_path '{field_path}', got '{violation.field_path}'"
        )
    if message_contains is not None:
        assert message_contains.lower() in violation.message.lower(), (
            f"Expected message containing '{message_contains}', got '{violation.message}'"
        )
    if annotation_key is not None:
        assert violation.annotation_key == annotation_key, (
            f"Expected annotation_key '{annotation_key}', got '{violation.annotation_key}'"
        )


def has_error_violation(violations):
    """Check if violations contain at least one ERROR-severity entry."""
    return any(v.severity == ExportViolationSeverity.ERROR for v in violations)


def make_propagation_entry(
    field_ref="user.email",
    annotation_key="PII",
    rule=None,
    field_type="string",
    component_id="service-a",
):
    """Factory for creating PropagationEntry instances."""
    if rule is None:
        rule = {"tier": "CONFIDENTIAL", "requires_masking": True, "severity": "high"}
    return PropagationEntry(
        field_ref=field_ref,
        annotation_key=annotation_key,
        rule=rule,
        field_type=field_type,
        component_id=component_id,
    )


def make_pact_entry(
    field_ref="user.email",
    annotation_key="PII",
    test_type="shape",
    component_id="service-a",
    shape="object",
    filter_val="required",
    method="GET",
):
    """Factory for PropagationEntry suitable for Pact export."""
    return make_propagation_entry(
        field_ref=field_ref,
        annotation_key=annotation_key,
        rule={
            "test_type": test_type,
            "shape": shape,
            "filter": filter_val,
            "method": method,
        },
        field_type="string",
        component_id=component_id,
    )


def make_arbiter_entry(
    field_ref="user.email",
    annotation_key="PII",
    tier="CONFIDENTIAL",
    requires_masking=True,
    backend=None,
    taint_on_raw_value=False,
    mask_in_spans=False,
):
    """Factory for PropagationEntry suitable for Arbiter export."""
    rule = {
        "tier": tier,
        "requires_masking": requires_masking,
        "taint_on_raw_value": taint_on_raw_value,
        "mask_in_spans": mask_in_spans,
    }
    if backend:
        rule["backend"] = backend
    return make_propagation_entry(
        field_ref=field_ref,
        annotation_key=annotation_key,
        rule=rule,
    )


def make_baton_entry(
    field_ref="user.email",
    annotation_key="PII",
    tier="CONFIDENTIAL",
    requires_masking=True,
    mock_generator="faker.email",
    canary_eligible=False,
    field_type="string",
    component_id="service-a",
    owner="team-alpha",
):
    """Factory for PropagationEntry suitable for Baton export."""
    rule = {
        "tier": tier,
        "requires_masking": requires_masking,
        "mock_generator": mock_generator,
        "canary_eligible": canary_eligible,
        "owner": owner,
    }
    return make_propagation_entry(
        field_ref=field_ref,
        annotation_key=annotation_key,
        rule=rule,
        field_type=field_type,
        component_id=component_id,
    )


def make_sentinel_entry(
    field_ref="user.email",
    annotation_key="PII",
    severity="high",
    description="PII field detected",
):
    """Factory for PropagationEntry suitable for Sentinel export."""
    rule = {"severity": severity, "description": description}
    return make_propagation_entry(
        field_ref=field_ref,
        annotation_key=annotation_key,
        rule=rule,
    )


# ============================================================================
# Tests for iter_propagation_entries
# ============================================================================

class TestIterPropagationEntries:
    """Tests for the shared traversal function."""

    def test_happy_path_returns_sorted_tuples(self):
        """Valid propagation table with empty filter returns all entries sorted."""
        table = [
            make_propagation_entry(field_ref="z.field", annotation_key="B"),
            make_propagation_entry(field_ref="a.field", annotation_key="A"),
            make_propagation_entry(field_ref="a.field", annotation_key="Z"),
        ]
        result = iter_propagation_entries(table, property_filter={})
        assert len(result) == 3
        # Verify sorted by (field_ref, annotation_key)
        refs = [(e.field_ref, e.annotation_key) for e in result]
        assert refs == sorted(refs), f"Output not sorted: {refs}"
        # Verify all are PropagationEntryTuple
        for entry in result:
            assert isinstance(entry, PropagationEntryTuple)

    def test_filter_matching_subset(self):
        """Property filter returns only matching entries."""
        table = [
            make_propagation_entry(
                field_ref="user.email",
                rule={"requires_masking": True, "tier": "CONFIDENTIAL"},
            ),
            make_propagation_entry(
                field_ref="user.name",
                rule={"requires_masking": False, "tier": "PUBLIC"},
            ),
            make_propagation_entry(
                field_ref="user.ssn",
                rule={"requires_masking": True, "tier": "RESTRICTED"},
            ),
        ]
        result = iter_propagation_entries(table, property_filter={"requires_masking": True})
        assert len(result) == 2
        field_refs = {e.field_ref for e in result}
        assert "user.email" in field_refs
        assert "user.ssn" in field_refs
        assert "user.name" not in field_refs

    def test_empty_table_error(self):
        """Empty propagation table raises appropriate error."""
        with pytest.raises(Exception) as exc_info:
            iter_propagation_entries([], property_filter={})
        assert "empty" in str(exc_info.value).lower() or "propagation_table" in str(exc_info.value).lower()

    def test_invalid_entry_structure(self):
        """Entry lacking required fields raises invalid_entry_structure error."""
        # Create an entry-like object missing required fields
        bad_entry = MagicMock()
        bad_entry.field_ref = "user.email"
        # Missing annotation_key and rule
        del bad_entry.annotation_key
        del bad_entry.rule
        with pytest.raises(Exception) as exc_info:
            iter_propagation_entries([bad_entry], property_filter={})
        # Should indicate structural problem
        assert "invalid" in str(exc_info.value).lower() or "structure" in str(exc_info.value).lower() or "required" in str(exc_info.value).lower()

    def test_filter_matches_none_returns_empty(self):
        """Filter that matches no entries returns empty list."""
        table = [
            make_propagation_entry(rule={"tier": "PUBLIC"}),
        ]
        result = iter_propagation_entries(
            table, property_filter={"nonexistent_property": "value"}
        )
        assert result == []

    def test_single_entry(self):
        """Single-entry table works correctly."""
        table = [make_propagation_entry(field_ref="user.email", annotation_key="PII")]
        result = iter_propagation_entries(table, property_filter={})
        assert len(result) == 1
        assert isinstance(result[0], PropagationEntryTuple)
        assert result[0].field_ref == "user.email"
        assert result[0].annotation_key == "PII"

    def test_deterministic_sort_order(self):
        """Output order is deterministic: sorted by (field_ref, annotation_key)."""
        table = [
            make_propagation_entry(field_ref="c.field", annotation_key="Z"),
            make_propagation_entry(field_ref="a.field", annotation_key="M"),
            make_propagation_entry(field_ref="a.field", annotation_key="A"),
            make_propagation_entry(field_ref="b.field", annotation_key="X"),
        ]
        result = iter_propagation_entries(table, property_filter={})
        pairs = [(e.field_ref, e.annotation_key) for e in result]
        assert pairs == sorted(pairs)

    def test_custom_annotations_flow_through(self):
        """Custom annotations with arbitrary rule keys pass through without code changes."""
        custom_entry = make_propagation_entry(
            field_ref="order.custom_field",
            annotation_key="CUSTOM_ANNOTATION",
            rule={"custom_prop": "custom_value", "tier": "INTERNAL"},
        )
        table = [custom_entry]
        result = iter_propagation_entries(table, property_filter={})
        assert len(result) == 1
        assert result[0].annotation_key == "CUSTOM_ANNOTATION"
        assert result[0].rule_properties.get("custom_prop") == "custom_value"

    def test_boolean_filter_values(self):
        """Property filter with boolean values works correctly."""
        table = [
            make_propagation_entry(
                field_ref="a.field",
                rule={"canary_eligible": True, "tier": "PUBLIC"},
            ),
            make_propagation_entry(
                field_ref="b.field",
                rule={"canary_eligible": False, "tier": "PUBLIC"},
            ),
        ]
        result = iter_propagation_entries(
            table, property_filter={"canary_eligible": True}
        )
        assert len(result) == 1
        assert result[0].field_ref == "a.field"


# ============================================================================
# Tests for export_pact
# ============================================================================

class TestExportPact:
    """Tests for Pact contract assertion export."""

    def test_happy_path(self):
        """Valid propagation table produces non-empty PactExport with sorted assertions."""
        table = [
            make_pact_entry(field_ref="user.email", annotation_key="PII", component_id="svc-a"),
            make_pact_entry(field_ref="user.name", annotation_key="PII", component_id="svc-a", test_type="filter"),
        ]
        result = export_pact("svc-a", table)
        assert isinstance(result, ExportResultPact)
        assert result.output is not None
        assert isinstance(result.output, PactExport)
        assert result.output.component_id == "svc-a"
        assert len(result.output.assertions) >= 1
        # Verify sorted by assertion_id
        ids = [a.assertion_id for a in result.output.assertions]
        assert ids == sorted(ids), f"Assertions not sorted by assertion_id: {ids}"
        # Verify unique assertion_ids
        assert len(ids) == len(set(ids)), "Assertion IDs are not unique"
        # No ERROR violations
        assert not has_error_violation(result.violations)

    def test_empty_propagation_table_error(self):
        """Empty table produces ERROR violation."""
        result = export_pact("svc-a", [])
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("empty" in v.message.lower() for v in error_violations)

    def test_no_matching_entries_error(self):
        """No entries matching component_id produces ERROR violation."""
        table = [make_pact_entry(component_id="other-service")]
        result = export_pact("svc-a", table)
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("match" in v.message.lower() or "no" in v.message.lower() for v in error_violations)

    def test_missing_test_type_property_error(self):
        """Rule without test_type produces ERROR violation."""
        entry = make_propagation_entry(
            field_ref="user.email",
            annotation_key="PII",
            rule={"shape": "object"},  # missing test_type
            component_id="svc-a",
        )
        result = export_pact("svc-a", [entry])
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("test_type" in v.message.lower() for v in error_violations)

    def test_invalid_test_type_error(self):
        """Invalid test_type value produces ERROR violation."""
        entry = make_pact_entry(test_type="invalid_type", component_id="svc-a")
        result = export_pact("svc-a", [entry])
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("test_type" in v.message.lower() or "invalid" in v.message.lower() for v in error_violations)

    def test_output_none_iff_error_violations(self):
        """output is None if and only if violations contain ERROR severity."""
        # Valid case
        table = [make_pact_entry(component_id="svc-a")]
        result = export_pact("svc-a", table)
        if has_error_violation(result.violations):
            assert result.output is None
        else:
            assert result.output is not None

        # Error case
        result_err = export_pact("svc-a", [])
        assert has_error_violation(result_err.violations)
        assert result_err.output is None

    def test_unique_assertion_ids(self):
        """All assertion_ids in output are unique."""
        table = [
            make_pact_entry(field_ref="user.email", annotation_key="PII", component_id="svc-a"),
            make_pact_entry(field_ref="user.name", annotation_key="PII", component_id="svc-a", test_type="filter"),
            make_pact_entry(field_ref="user.phone", annotation_key="CONTACT", component_id="svc-a", test_type="method"),
        ]
        result = export_pact("svc-a", table)
        if result.output is not None:
            ids = [a.assertion_id for a in result.output.assertions]
            assert len(ids) == len(set(ids))

    def test_multiple_violations_collected(self):
        """Multiple problems produce ALL violations, not just the first."""
        table = [
            make_propagation_entry(
                field_ref="user.email",
                annotation_key="PII",
                rule={"shape": "object"},  # missing test_type
                component_id="svc-a",
            ),
            make_propagation_entry(
                field_ref="user.name",
                annotation_key="NAME",
                rule={"test_type": "invalid_type"},  # invalid test_type
                component_id="svc-a",
            ),
        ]
        result = export_pact("svc-a", table)
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert len(error_violations) >= 2, (
            f"Expected at least 2 ERROR violations for 2 problems, got {len(error_violations)}"
        )

    def test_warning_for_missing_optional_properties(self):
        """Missing optional annotation properties reported as WARNING violations."""
        entry = make_propagation_entry(
            field_ref="user.email",
            annotation_key="PII",
            rule={"test_type": "shape"},  # missing optional shape, filter, method
            component_id="svc-a",
        )
        result = export_pact("svc-a", [entry])
        warning_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.WARNING]
        assert len(warning_violations) >= 1, "Expected WARNING violations for missing optional properties"

    @pytest.mark.parametrize("test_type", ["shape", "filter", "method"], ids=["shape", "filter", "method"])
    def test_valid_test_types_accepted(self, test_type):
        """All valid test_type values are accepted."""
        entry = make_pact_entry(test_type=test_type, component_id="svc-a")
        result = export_pact("svc-a", [entry])
        error_violations = [
            v for v in result.violations
            if v.severity == ExportViolationSeverity.ERROR and "test_type" in v.message.lower()
        ]
        assert len(error_violations) == 0, f"test_type '{test_type}' should be valid"


# ============================================================================
# Tests for export_arbiter
# ============================================================================

class TestExportArbiter:
    """Tests for Arbiter field-level classification export."""

    def test_happy_path(self):
        """Valid table produces ArbiterExport with rules sorted by pattern."""
        table = [
            make_arbiter_entry(field_ref="user.ssn", tier="RESTRICTED"),
            make_arbiter_entry(field_ref="user.email", tier="CONFIDENTIAL"),
        ]
        result = export_arbiter(table, default_backend="vault")
        assert isinstance(result, ExportResultArbiter)
        assert result.output is not None
        assert isinstance(result.output, ArbiterExport)
        assert len(result.output.rules) >= 1
        # Sorted by pattern
        patterns = [r.pattern for r in result.output.rules]
        assert patterns == sorted(patterns), f"Rules not sorted by pattern: {patterns}"
        assert not has_error_violation(result.violations)

    def test_empty_table_error(self):
        """Empty table produces ERROR violation."""
        result = export_arbiter([], default_backend="vault")
        assert has_error_violation(result.violations)
        assert result.output is None

    def test_missing_tier_property_error(self):
        """Rule without tier produces ERROR violation."""
        entry = make_propagation_entry(
            field_ref="user.email",
            annotation_key="PII",
            rule={"requires_masking": True},  # missing tier
        )
        result = export_arbiter([entry], default_backend="vault")
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("tier" in v.message.lower() for v in error_violations)

    def test_conflicting_tiers_error(self):
        """Different tier values on same field produces ERROR violation."""
        table = [
            make_arbiter_entry(field_ref="user.email", annotation_key="PII", tier="CONFIDENTIAL"),
            make_arbiter_entry(field_ref="user.email", annotation_key="FINANCIAL", tier="RESTRICTED"),
        ]
        result = export_arbiter(table, default_backend="vault")
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("tier" in v.message.lower() or "conflict" in v.message.lower() for v in error_violations)

    def test_merge_annotations_same_field(self):
        """Multiple annotations on same field merged into single rule."""
        table = [
            make_arbiter_entry(field_ref="user.email", annotation_key="PII", tier="CONFIDENTIAL"),
            make_arbiter_entry(field_ref="user.email", annotation_key="CONTACT", tier="CONFIDENTIAL"),
        ]
        result = export_arbiter(table, default_backend="vault")
        if result.output is not None:
            # Should be merged into one rule for the same pattern+backend
            email_rules = [r for r in result.output.rules if "email" in r.pattern]
            assert len(email_rules) == 1, "Multiple annotations should merge into single rule"
            # Both annotation keys should appear in annotations list
            assert len(email_rules[0].annotations) >= 2

    def test_taint_on_raw_value_from_requires_masking(self):
        """taint_on_raw_value is true when requires_masking=true."""
        entry = make_arbiter_entry(
            field_ref="user.ssn",
            tier="RESTRICTED",
            requires_masking=True,
            taint_on_raw_value=False,
        )
        result = export_arbiter([entry], default_backend="vault")
        if result.output is not None:
            ssn_rules = [r for r in result.output.rules if "ssn" in r.pattern]
            assert len(ssn_rules) == 1
            assert ssn_rules[0].taint_on_raw_value is True

    def test_mask_in_spans_from_requires_masking(self):
        """mask_in_spans is true when requires_masking=true."""
        entry = make_arbiter_entry(
            field_ref="user.ssn",
            tier="RESTRICTED",
            requires_masking=True,
            mask_in_spans=False,
        )
        result = export_arbiter([entry], default_backend="vault")
        if result.output is not None:
            ssn_rules = [r for r in result.output.rules if "ssn" in r.pattern]
            assert len(ssn_rules) == 1
            assert ssn_rules[0].mask_in_spans is True

    def test_default_backend_used(self):
        """default_backend is used when rule has no backend specified."""
        entry = make_arbiter_entry(field_ref="user.email", tier="CONFIDENTIAL", backend=None)
        result = export_arbiter([entry], default_backend="vault")
        if result.output is not None:
            assert any(r.backend == "vault" for r in result.output.rules)

    def test_output_none_iff_error(self):
        """output is None iff violations contain ERROR severity."""
        # Success case
        table = [make_arbiter_entry()]
        result = export_arbiter(table, default_backend="vault")
        if has_error_violation(result.violations):
            assert result.output is None
        else:
            assert result.output is not None

        # Error case
        result_err = export_arbiter([], default_backend="vault")
        assert has_error_violation(result_err.violations)
        assert result_err.output is None

    def test_unique_pattern_backend_combinations(self):
        """Each (pattern, backend) combination is unique in output."""
        table = [
            make_arbiter_entry(field_ref="user.email", tier="CONFIDENTIAL"),
            make_arbiter_entry(field_ref="user.name", tier="CONFIDENTIAL"),
        ]
        result = export_arbiter(table, default_backend="vault")
        if result.output is not None:
            combos = [(r.pattern, r.backend) for r in result.output.rules]
            assert len(combos) == len(set(combos)), "Duplicate (pattern, backend) combinations found"


# ============================================================================
# Tests for export_baton
# ============================================================================

class TestExportBaton:
    """Tests for Baton egress node export."""

    def test_happy_path(self):
        """Valid table produces BatonExport with egress_nodes sorted by id."""
        table = [
            make_baton_entry(field_ref="user.email", component_id="svc-a", owner="team-alpha"),
            make_baton_entry(field_ref="user.name", component_id="svc-b", owner="team-beta",
                           mock_generator="faker.name"),
        ]
        result = export_baton(table)
        assert isinstance(result, ExportResultBaton)
        assert result.output is not None
        assert isinstance(result.output, BatonExport)
        assert len(result.output.egress_nodes) >= 1
        # Sorted by id
        ids = [n.id for n in result.output.egress_nodes]
        assert ids == sorted(ids), f"Egress nodes not sorted by id: {ids}"
        assert not has_error_violation(result.violations)

    def test_empty_table_error(self):
        """Empty table produces ERROR violation."""
        result = export_baton([])
        assert has_error_violation(result.violations)
        assert result.output is None

    def test_missing_mock_generator_error(self):
        """Rule without mock_generator produces ERROR violation."""
        entry = make_propagation_entry(
            field_ref="user.email",
            annotation_key="PII",
            rule={"tier": "CONFIDENTIAL", "requires_masking": True, "owner": "team-alpha"},
            # missing mock_generator
        )
        result = export_baton([entry])
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("mock_generator" in v.message.lower() for v in error_violations)

    def test_non_string_canary_field_error(self):
        """canary_eligible=true on non-string field produces ERROR violation."""
        entry = make_baton_entry(
            field_ref="user.age",
            field_type="integer",
            canary_eligible=True,
        )
        result = export_baton([entry])
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any(
            "canary" in v.message.lower() or "string" in v.message.lower()
            for v in error_violations
        )

    def test_missing_owner_error(self):
        """Cannot determine owner produces ERROR violation."""
        entry = make_propagation_entry(
            field_ref="user.email",
            annotation_key="PII",
            rule={
                "tier": "CONFIDENTIAL",
                "requires_masking": True,
                "mock_generator": "faker.email",
                # missing owner
            },
        )
        result = export_baton([entry])
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("owner" in v.message.lower() for v in error_violations)

    def test_masked_fields_non_public_only(self):
        """masked_fields only contains non-PUBLIC tier fields."""
        table = [
            make_baton_entry(field_ref="user.email", tier="CONFIDENTIAL", requires_masking=True),
            make_baton_entry(field_ref="user.nickname", tier="PUBLIC", requires_masking=False,
                           mock_generator="stdlib.random"),
        ]
        result = export_baton(table)
        if result.output is not None:
            for node in result.output.egress_nodes:
                for masked_field in node.masked_fields:
                    assert "nickname" not in masked_field.lower() or "public" not in str(masked_field).lower(), (
                        "PUBLIC tier fields should not be in masked_fields"
                    )

    def test_canary_eligible_string_only(self):
        """canary_eligible_fields only contains string-typed fields."""
        table = [
            make_baton_entry(
                field_ref="user.email",
                field_type="string",
                canary_eligible=True,
            ),
        ]
        result = export_baton(table)
        if result.output is not None:
            for node in result.output.egress_nodes:
                # All canary eligible fields should be from string-typed entries
                assert isinstance(node.canary_eligible_fields, list)

    def test_output_none_iff_error(self):
        """output is None iff violations contain ERROR severity."""
        table = [make_baton_entry()]
        result = export_baton(table)
        if has_error_violation(result.violations):
            assert result.output is None
        else:
            assert result.output is not None

    def test_unique_egress_node_ids(self):
        """Each egress node id is unique."""
        table = [
            make_baton_entry(field_ref="user.email", component_id="svc-a"),
            make_baton_entry(field_ref="user.phone", component_id="svc-a",
                           mock_generator="faker.phone"),
        ]
        result = export_baton(table)
        if result.output is not None:
            ids = [n.id for n in result.output.egress_nodes]
            assert len(ids) == len(set(ids)), "Egress node IDs are not unique"


# ============================================================================
# Tests for export_sentinel
# ============================================================================

class TestExportSentinel:
    """Tests for Sentinel severity mapping export."""

    def test_happy_path(self):
        """Valid table produces SentinelExport with sorted severity_mappings."""
        table = [
            make_sentinel_entry(field_ref="user.email", severity="high"),
            make_sentinel_entry(field_ref="user.name", annotation_key="NAME", severity="medium"),
        ]
        result = export_sentinel(table)
        assert isinstance(result, ExportResultSentinel)
        assert result.output is not None
        assert isinstance(result.output, SentinelExport)
        assert len(result.output.severity_mappings) >= 1
        # Sorted by (severity, annotation_key, field_pattern)
        mappings = result.output.severity_mappings
        sort_keys = [(m.severity, m.annotation_key, m.field_pattern) for m in mappings]
        assert sort_keys == sorted(sort_keys), f"Mappings not sorted: {sort_keys}"
        assert not has_error_violation(result.violations)

    def test_empty_table_error(self):
        """Empty table produces ERROR violation."""
        result = export_sentinel([])
        assert has_error_violation(result.violations)
        assert result.output is None

    def test_missing_severity_property_warning(self):
        """Entry without severity rule property produces WARNING violation and is skipped."""
        entry = make_propagation_entry(
            field_ref="user.email",
            annotation_key="PII",
            rule={"tier": "CONFIDENTIAL"},  # no severity
        )
        result = export_sentinel([entry])
        warning_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.WARNING]
        assert len(warning_violations) >= 1
        assert any("severity" in v.message.lower() for v in warning_violations)

    def test_invalid_severity_value_error(self):
        """Unrecognized severity value produces ERROR violation."""
        entry = make_sentinel_entry(severity="BOGUS_SEVERITY")
        result = export_sentinel([entry])
        assert has_error_violation(result.violations)
        assert result.output is None
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert any("severity" in v.message.lower() for v in error_violations)

    def test_skip_no_severity_with_warning(self):
        """Entries without severity are skipped but produce WARNING."""
        table = [
            make_sentinel_entry(field_ref="user.email", severity="high"),
            make_propagation_entry(
                field_ref="user.name",
                annotation_key="NAME",
                rule={"tier": "PUBLIC"},  # no severity
            ),
        ]
        result = export_sentinel(table)
        # Should still have output from the valid entry
        if result.output is not None:
            # Only the entry with severity produces a mapping
            assert len(result.output.severity_mappings) >= 1
        # Should have warning for the missing severity
        warning_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.WARNING]
        assert len(warning_violations) >= 1

    def test_one_mapping_per_severity_entry(self):
        """Each entry with severity produces exactly one SentinelSeverityMapping."""
        table = [
            make_sentinel_entry(field_ref="user.email", annotation_key="PII", severity="high"),
            make_sentinel_entry(field_ref="user.ssn", annotation_key="SSN", severity="critical"),
            make_sentinel_entry(field_ref="user.name", annotation_key="NAME", severity="low"),
        ]
        result = export_sentinel(table)
        if result.output is not None:
            assert len(result.output.severity_mappings) == 3

    def test_output_none_iff_error(self):
        """output is None iff violations contain ERROR severity."""
        table = [make_sentinel_entry()]
        result = export_sentinel(table)
        if has_error_violation(result.violations):
            assert result.output is None
        else:
            assert result.output is not None

    def test_multiple_violations_collected(self):
        """Multiple problems produce ALL violations, not just the first."""
        table = [
            make_sentinel_entry(severity="BOGUS_1"),
            make_sentinel_entry(field_ref="user.name", annotation_key="NAME", severity="BOGUS_2"),
        ]
        result = export_sentinel(table)
        error_violations = [v for v in result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert len(error_violations) >= 2, (
            f"Expected at least 2 ERROR violations, got {len(error_violations)}"
        )


# ============================================================================
# Tests for yaml_dump
# ============================================================================

class TestYamlDump:
    """Tests for YAML serialization."""

    def test_happy_path_pact_export(self):
        """yaml_dump produces valid YAML string from PactExport."""
        model = PactExport(
            component_id="svc-a",
            assertions=[
                PactAssertion(
                    assertion_id="a1",
                    description="test assertion",
                    test_type="shape",
                    field_ref="user.email",
                    filter="required",
                    shape="object",
                    method="GET",
                )
            ],
        )
        result = yaml_dump(model)
        assert isinstance(result, str)
        assert len(result) > 0
        # Should be parseable YAML
        parsed = yaml.safe_load(result)
        assert parsed is not None
        assert "component_id" in parsed or "assertions" in parsed

    def test_roundtrip(self):
        """yaml_dump output can be loaded back via yaml.safe_load and matches model_dump."""
        model = PactExport(
            component_id="svc-a",
            assertions=[
                PactAssertion(
                    assertion_id="a1",
                    description="test",
                    test_type="shape",
                    field_ref="user.email",
                    filter="required",
                    shape="object",
                    method="GET",
                )
            ],
        )
        yaml_str = yaml_dump(model)
        parsed = yaml.safe_load(yaml_str)
        model_dict = model.model_dump(exclude_none=True)
        assert parsed == model_dict

    def test_not_pydantic_model_error(self):
        """Non-Pydantic input raises error."""
        with pytest.raises(Exception) as exc_info:
            yaml_dump({"not": "a pydantic model"})
        # Should indicate the problem
        err_msg = str(exc_info.value).lower()
        assert "pydantic" in err_msg or "model_dump" in err_msg or "model" in err_msg

    def test_excludes_none_values(self):
        """None values are excluded from output."""
        model = PactExport(
            component_id="svc-a",
            assertions=[],
        )
        yaml_str = yaml_dump(model)
        # Parse and verify no None values
        parsed = yaml.safe_load(yaml_str)
        if parsed is not None:
            _assert_no_none_values(parsed)

    def test_idempotent(self):
        """Calling yaml_dump twice on same model produces identical output."""
        model = ArbiterExport(
            rules=[
                ArbiterRule(
                    pattern="user.email",
                    backend="vault",
                    tier="CONFIDENTIAL",
                    annotations=["PII"],
                    taint_on_raw_value=True,
                    mask_in_spans=True,
                )
            ]
        )
        result1 = yaml_dump(model)
        result2 = yaml_dump(model)
        assert result1 == result2, "yaml_dump is not idempotent"

    def test_unicode_survival(self):
        """Unicode and special characters survive serialization."""
        model = PactExport(
            component_id="svc-ünïcödé",
            assertions=[
                PactAssertion(
                    assertion_id="a1",
                    description="Ñoño field — «special» chars: 日本語",
                    test_type="shape",
                    field_ref="user.名前",
                    filter="required",
                    shape="object",
                    method="GET",
                )
            ],
        )
        yaml_str = yaml_dump(model)
        parsed = yaml.safe_load(yaml_str)
        # Verify unicode survived
        assert "ünïcödé" in str(parsed) or "svc-ünïcödé" in yaml_str
        assert "日本語" in str(parsed) or "日本語" in yaml_str

    def test_preserves_field_order(self):
        """yaml_dump preserves model field definition order (sort_keys=False)."""
        model = BatonExport(
            egress_nodes=[
                BatonEgressNode(
                    id="node-1",
                    owner="team-alpha",
                    mock_generator="faker.email",
                    masked_fields=["user.email"],
                    canary_eligible_fields=["user.email"],
                )
            ]
        )
        yaml_str = yaml_dump(model)
        # In the YAML, 'id' should appear before 'owner', 'owner' before 'mock_generator'
        lines = yaml_str.split("\n")
        key_positions = {}
        for i, line in enumerate(lines):
            stripped = line.lstrip("- ").strip()
            if ":" in stripped:
                key = stripped.split(":")[0].strip()
                if key not in key_positions:
                    key_positions[key] = i
        # Verify id comes before owner if both present
        if "id" in key_positions and "owner" in key_positions:
            assert key_positions["id"] < key_positions["owner"], (
                "Field order not preserved: 'id' should come before 'owner'"
            )

    def test_all_export_types_accepted(self):
        """yaml_dump accepts all four export model types."""
        models = [
            PactExport(component_id="svc-a", assertions=[]),
            ArbiterExport(rules=[]),
            BatonExport(egress_nodes=[]),
            SentinelExport(severity_mappings=[]),
        ]
        for model in models:
            result = yaml_dump(model)
            assert isinstance(result, str)
            assert len(result) > 0


def _assert_no_none_values(obj, path="root"):
    """Recursively assert no None values in parsed YAML output."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            assert value is not None, f"None value found at {path}.{key}"
            _assert_no_none_values(value, f"{path}.{key}")
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            assert item is not None, f"None value found at {path}[{i}]"
            _assert_no_none_values(item, f"{path}[{i}]")


# ============================================================================
# Cross-cutting invariant tests
# ============================================================================

class TestCrossCuttingInvariants:
    """Tests for invariants that span multiple exporters."""

    def test_all_exporters_collect_all_violations(self):
        """All four exporters collect violations exhaustively, never short-circuit."""
        # Pact: two broken entries
        pact_table = [
            make_propagation_entry(
                field_ref="user.email", annotation_key="A",
                rule={"shape": "object"}, component_id="svc-a",
            ),
            make_propagation_entry(
                field_ref="user.name", annotation_key="B",
                rule={"test_type": "INVALID"}, component_id="svc-a",
            ),
        ]
        pact_result = export_pact("svc-a", pact_table)
        pact_errors = [v for v in pact_result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert len(pact_errors) >= 2, "Pact should collect all violations"

        # Sentinel: two broken entries
        sentinel_table = [
            make_sentinel_entry(severity="INVALID_1"),
            make_sentinel_entry(field_ref="b.field", annotation_key="B", severity="INVALID_2"),
        ]
        sentinel_result = export_sentinel(sentinel_table)
        sentinel_errors = [v for v in sentinel_result.violations if v.severity == ExportViolationSeverity.ERROR]
        assert len(sentinel_errors) >= 2, "Sentinel should collect all violations"

    def test_violations_always_well_formed(self):
        """All violations from any exporter are well-formed ExportViolation instances."""
        # Gather violations from various error scenarios
        results = [
            export_pact("svc-a", []),
            export_arbiter([], default_backend="vault"),
            export_baton([]),
            export_sentinel([]),
        ]
        for result in results:
            for v in result.violations:
                assert isinstance(v, ExportViolation)
                assert isinstance(v.severity, ExportViolationSeverity)
                assert isinstance(v.field_path, str)
                assert isinstance(v.message, str)
                assert len(v.message) > 0

    def test_output_none_iff_error_all_exporters(self):
        """For all exporters, output is None iff violations contain ERROR severity."""
        # Error cases — all should have output=None
        error_results = [
            export_pact("svc-a", []),
            export_arbiter([], default_backend="vault"),
            export_baton([]),
            export_sentinel([]),
        ]
        for result in error_results:
            assert has_error_violation(result.violations)
            assert result.output is None, f"Output should be None when ERROR violations exist: {type(result)}"

        # Happy cases
        pact_table = [make_pact_entry(component_id="svc-a")]
        pact_result = export_pact("svc-a", pact_table)
        if not has_error_violation(pact_result.violations):
            assert pact_result.output is not None

        arbiter_table = [make_arbiter_entry()]
        arbiter_result = export_arbiter(arbiter_table, default_backend="vault")
        if not has_error_violation(arbiter_result.violations):
            assert arbiter_result.output is not None

        baton_table = [make_baton_entry()]
        baton_result = export_baton(baton_table)
        if not has_error_violation(baton_result.violations):
            assert baton_result.output is not None

        sentinel_table = [make_sentinel_entry()]
        sentinel_result = export_sentinel(sentinel_table)
        if not has_error_violation(sentinel_result.violations):
            assert sentinel_result.output is not None

    def test_no_hardcoded_annotation_names(self):
        """Custom annotations with appropriate rule properties produce correct output."""
        # Use a completely custom annotation name that no exporter could hard-code
        custom_annotation = "TOTALLY_CUSTOM_ANNOTATION_XYZ"

        # Arbiter: custom annotation with tier property
        arbiter_entry = make_propagation_entry(
            field_ref="data.custom_field",
            annotation_key=custom_annotation,
            rule={"tier": "INTERNAL", "requires_masking": False},
        )
        arbiter_result = export_arbiter([arbiter_entry], default_backend="vault")
        if arbiter_result.output is not None:
            assert len(arbiter_result.output.rules) >= 1
            found = any(custom_annotation in r.annotations for r in arbiter_result.output.rules)
            assert found, f"Custom annotation '{custom_annotation}' not found in Arbiter output"

        # Sentinel: custom annotation with severity property
        sentinel_entry = make_propagation_entry(
            field_ref="data.custom_field",
            annotation_key=custom_annotation,
            rule={"severity": "high", "description": "custom rule"},
        )
        sentinel_result = export_sentinel([sentinel_entry])
        if sentinel_result.output is not None:
            assert len(sentinel_result.output.severity_mappings) >= 1
            found = any(
                m.annotation_key == custom_annotation
                for m in sentinel_result.output.severity_mappings
            )
            assert found, f"Custom annotation '{custom_annotation}' not found in Sentinel output"

    def test_deterministic_output_order(self):
        """All export output lists use deterministic sort keys."""
        # Pact: sorted by assertion_id
        pact_table = [
            make_pact_entry(field_ref="z.field", annotation_key="Z", component_id="svc-a"),
            make_pact_entry(field_ref="a.field", annotation_key="A", component_id="svc-a"),
        ]
        pact_result = export_pact("svc-a", pact_table)
        if pact_result.output is not None:
            ids = [a.assertion_id for a in pact_result.output.assertions]
            assert ids == sorted(ids)

        # Arbiter: sorted by pattern
        arbiter_table = [
            make_arbiter_entry(field_ref="z.field"),
            make_arbiter_entry(field_ref="a.field"),
        ]
        arbiter_result = export_arbiter(arbiter_table, default_backend="vault")
        if arbiter_result.output is not None:
            patterns = [r.pattern for r in arbiter_result.output.rules]
            assert patterns == sorted(patterns)

        # Baton: sorted by id
        baton_table = [
            make_baton_entry(field_ref="z.field", component_id="svc-z"),
            make_baton_entry(field_ref="a.field", component_id="svc-a"),
        ]
        baton_result = export_baton(baton_table)
        if baton_result.output is not None:
            ids = [n.id for n in baton_result.output.egress_nodes]
            assert ids == sorted(ids)

        # Sentinel: sorted by (severity, annotation_key, field_pattern)
        sentinel_table = [
            make_sentinel_entry(field_ref="z.field", annotation_key="Z", severity="low"),
            make_sentinel_entry(field_ref="a.field", annotation_key="A", severity="high"),
        ]
        sentinel_result = export_sentinel(sentinel_table)
        if sentinel_result.output is not None:
            sort_keys = [
                (m.severity, m.annotation_key, m.field_pattern)
                for m in sentinel_result.output.severity_mappings
            ]
            assert sort_keys == sorted(sort_keys)
