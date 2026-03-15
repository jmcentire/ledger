"""Export generators for Pact, Arbiter, Baton, and Sentinel."""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional

import yaml
from pydantic import BaseModel


# ── Enums ──────────────────────────────────────────────


class ExportViolationSeverity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


# ── Data Models ────────────────────────────────────────


class ExportViolation(BaseModel):
    severity: ExportViolationSeverity
    field_path: str
    message: str
    annotation_key: Optional[str] = None


class PropagationEntry(BaseModel):
    field_ref: str
    annotation_key: str
    rule: dict
    field_type: str
    component_id: str


class PropagationEntryTuple(BaseModel):
    field_ref: str
    annotation_key: str
    rule_properties: dict
    field_type: str
    component_id: str


class PactAssertion(BaseModel):
    assertion_id: str
    description: str
    test_type: str
    field_ref: str
    filter: Optional[str] = None
    shape: Optional[str] = None
    method: Optional[str] = None


class PactExport(BaseModel):
    component_id: str
    assertions: list[PactAssertion]


class ArbiterRule(BaseModel):
    pattern: str
    backend: str
    tier: str
    annotations: list[str]
    taint_on_raw_value: bool
    mask_in_spans: bool


class ArbiterExport(BaseModel):
    rules: list[ArbiterRule]


class BatonEgressNode(BaseModel):
    id: str
    owner: str
    mock_generator: str
    masked_fields: list[str]
    canary_eligible_fields: list[str]


class BatonExport(BaseModel):
    egress_nodes: list[BatonEgressNode]


class SentinelSeverityMapping(BaseModel):
    annotation_key: str
    severity: str
    field_pattern: str
    description: str


class SentinelExport(BaseModel):
    severity_mappings: list[SentinelSeverityMapping]


class ExportResultPact(BaseModel):
    output: Optional[PactExport] = None
    violations: list[ExportViolation]


class ExportResultArbiter(BaseModel):
    output: Optional[ArbiterExport] = None
    violations: list[ExportViolation]


class ExportResultBaton(BaseModel):
    output: Optional[BatonExport] = None
    violations: list[ExportViolation]


class ExportResultSentinel(BaseModel):
    output: Optional[SentinelExport] = None
    violations: list[ExportViolation]


# ── Shared Helper ──────────────────────────────────────


def iter_propagation_entries(
    propagation_table: list[PropagationEntry],
    property_filter: dict[str, Any] | None = None,
) -> list[PropagationEntryTuple]:
    if property_filter is None:
        property_filter = {}

    if not propagation_table:
        raise ValueError("Propagation table is empty; no entries to iterate.")

    results = []
    for entry in propagation_table:
        for field_name in ("field_ref", "annotation_key", "rule"):
            try:
                getattr(entry, field_name)
            except AttributeError:
                raise ValueError(
                    f"PropagationEntry missing required field: {field_name}"
                )

        rule = entry.rule
        if all(rule.get(k) == v for k, v in property_filter.items()):
            results.append(PropagationEntryTuple(
                field_ref=entry.field_ref,
                annotation_key=entry.annotation_key,
                rule_properties=dict(rule),
                field_type=getattr(entry, "field_type", "string"),
                component_id=getattr(entry, "component_id", ""),
            ))

    results.sort(key=lambda e: (e.field_ref, e.annotation_key))
    return results


# ── Helpers ────────────────────────────────────────────

_VALID_TEST_TYPES = {"shape", "filter", "method"}
_VALID_SEVERITIES = {"critical", "high", "medium", "low"}


def _has_errors(violations: list[ExportViolation]) -> bool:
    return any(v.severity == ExportViolationSeverity.ERROR for v in violations)


def _get_entries_or_error(propagation_table, tool_name, property_filter=None):
    """Call iter_propagation_entries; on ValueError return (None, violations)."""
    violations: list[ExportViolation] = []
    if property_filter is None:
        property_filter = {}
    try:
        entries = iter_propagation_entries(propagation_table, property_filter)
    except ValueError:
        violations.append(ExportViolation(
            severity=ExportViolationSeverity.ERROR,
            field_path="",
            message=f"No propagation entries found; empty propagation table for {tool_name}.",
        ))
        return None, violations
    return entries, violations


# ── Pact Exporter ──────────────────────────────────────


def export_pact(
    component_id: str,
    propagation_table: list[PropagationEntry],
) -> ExportResultPact:
    entries, violations = _get_entries_or_error(propagation_table, "Pact")
    if entries is None:
        return ExportResultPact(output=None, violations=violations)

    component_entries = [e for e in entries if e.component_id == component_id]
    if not component_entries:
        violations.append(ExportViolation(
            severity=ExportViolationSeverity.ERROR,
            field_path="",
            message=f"No matching propagation entries found for component '{component_id}'.",
        ))
        return ExportResultPact(output=None, violations=violations)

    assertions = []
    for entry in component_entries:
        rule = entry.rule_properties
        test_type = rule.get("test_type")

        if test_type is None:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=entry.field_ref,
                message=(f"Propagation rule for field '{entry.field_ref}' annotation "
                         f"'{entry.annotation_key}' missing required 'test_type' property."),
                annotation_key=entry.annotation_key,
            ))
            continue

        if test_type not in _VALID_TEST_TYPES:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=entry.field_ref,
                message=(f"Invalid test_type '{test_type}' for field '{entry.field_ref}'. "
                         "Must be 'shape', 'filter', or 'method'."),
                annotation_key=entry.annotation_key,
            ))
            continue

        if test_type not in rule:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.WARNING,
                field_path=entry.field_ref,
                message=(f"Propagation rule for field '{entry.field_ref}' annotation "
                         f"'{entry.annotation_key}' missing optional '{test_type}' property."),
                annotation_key=entry.annotation_key,
            ))

        assertions.append(PactAssertion(
            assertion_id=f"{entry.field_ref}:{entry.annotation_key}",
            description=f"Verify {test_type} for field '{entry.field_ref}' ({entry.annotation_key})",
            test_type=test_type,
            field_ref=entry.field_ref,
            filter=rule.get("filter"),
            shape=rule.get("shape"),
            method=rule.get("method"),
        ))

    if _has_errors(violations):
        return ExportResultPact(output=None, violations=violations)

    assertions.sort(key=lambda a: a.assertion_id)
    return ExportResultPact(
        output=PactExport(component_id=component_id, assertions=assertions),
        violations=violations,
    )


# ── Arbiter Exporter ───────────────────────────────────


def export_arbiter(
    propagation_table: list[PropagationEntry],
    default_backend: str = "primary",
) -> ExportResultArbiter:
    entries, violations = _get_entries_or_error(propagation_table, "Arbiter")
    if entries is None:
        return ExportResultArbiter(output=None, violations=violations)

    field_groups: dict[str, list[PropagationEntryTuple]] = {}
    for entry in entries:
        rule = entry.rule_properties
        if "tier" not in rule:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=entry.field_ref,
                message=(f"Propagation rule for field '{entry.field_ref}' annotation "
                         f"'{entry.annotation_key}' missing required 'tier' property."),
                annotation_key=entry.annotation_key,
            ))
            continue
        backend = rule.get("backend", default_backend)
        key = f"{entry.field_ref}:{backend}"
        field_groups.setdefault(key, []).append(entry)

    rules = []
    for _key, group in field_groups.items():
        tiers = {e.rule_properties["tier"] for e in group}
        field_ref = group[0].field_ref
        if len(tiers) > 1:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=field_ref,
                message=(f"Field '{field_ref}' has conflicting tiers: "
                         f"{sorted(tiers)}. Using highest classification."),
            ))
            continue

        tier = next(iter(tiers))
        backend = group[0].rule_properties.get("backend", default_backend)
        annotations = sorted({e.annotation_key for e in group})
        taint = any(
            r.get("requires_masking", False) or r.get("taint_on_raw_value", False)
            for r in (e.rule_properties for e in group)
        )
        mask = any(
            r.get("requires_masking", False) or r.get("mask_in_spans", False)
            for r in (e.rule_properties for e in group)
        )
        rules.append(ArbiterRule(
            pattern=field_ref, backend=backend, tier=tier,
            annotations=annotations, taint_on_raw_value=taint, mask_in_spans=mask,
        ))

    if _has_errors(violations):
        return ExportResultArbiter(output=None, violations=violations)

    rules.sort(key=lambda r: r.pattern)
    return ExportResultArbiter(
        output=ArbiterExport(rules=rules), violations=violations,
    )


# ── Baton Exporter ─────────────────────────────────────


def export_baton(
    propagation_table: list[PropagationEntry],
) -> ExportResultBaton:
    entries, violations = _get_entries_or_error(propagation_table, "Baton")
    if entries is None:
        return ExportResultBaton(output=None, violations=violations)

    nodes = []
    for entry in entries:
        rule = entry.rule_properties

        owner = rule.get("owner")
        if not owner:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=entry.field_ref,
                message=f"Cannot determine owner for egress node derived from field '{entry.field_ref}'.",
                annotation_key=entry.annotation_key,
            ))
            continue

        mock_gen = rule.get("mock_generator")
        if not mock_gen:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=entry.field_ref,
                message=f"No mock_generator specified for field '{entry.field_ref}'; required for Baton export.",
                annotation_key=entry.annotation_key,
            ))
            continue

        canary_eligible = rule.get("canary_eligible", False)
        if canary_eligible and entry.field_type != "string":
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=entry.field_ref,
                message=(f"Field '{entry.field_ref}' is canary_eligible but type is "
                         f"'{entry.field_type}' (not string); cannot be canary eligible."),
                annotation_key=entry.annotation_key,
            ))
            continue

        masked = [entry.field_ref] if rule.get("requires_masking", False) else []
        canary_fields = [entry.field_ref] if (canary_eligible and entry.field_type == "string") else []

        nodes.append(BatonEgressNode(
            id=f"{entry.field_ref}:{entry.annotation_key}",
            owner=owner, mock_generator=mock_gen,
            masked_fields=masked, canary_eligible_fields=canary_fields,
        ))

    if _has_errors(violations):
        return ExportResultBaton(output=None, violations=violations)

    nodes.sort(key=lambda n: n.id)
    return ExportResultBaton(
        output=BatonExport(egress_nodes=nodes), violations=violations,
    )


# ── Sentinel Exporter ──────────────────────────────────


def export_sentinel(
    propagation_table: list[PropagationEntry],
) -> ExportResultSentinel:
    entries, violations = _get_entries_or_error(propagation_table, "Sentinel")
    if entries is None:
        return ExportResultSentinel(output=None, violations=violations)

    mappings = []
    for entry in entries:
        rule = entry.rule_properties
        severity = rule.get("severity")

        if severity is None:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.WARNING,
                field_path=entry.field_ref,
                message=(f"Propagation rule for field '{entry.field_ref}' annotation "
                         f"'{entry.annotation_key}' has no 'severity' property; skipping."),
                annotation_key=entry.annotation_key,
            ))
            continue

        if severity.lower() not in _VALID_SEVERITIES:
            violations.append(ExportViolation(
                severity=ExportViolationSeverity.ERROR,
                field_path=entry.field_ref,
                message=(f"Invalid severity '{severity}' for field '{entry.field_ref}' "
                         f"annotation '{entry.annotation_key}'. Expected CRITICAL, HIGH, MEDIUM, or LOW."),
                annotation_key=entry.annotation_key,
            ))
            continue

        description = rule.get(
            "description",
            f"{entry.annotation_key} severity mapping for {entry.field_ref}",
        )
        mappings.append(SentinelSeverityMapping(
            annotation_key=entry.annotation_key, severity=severity,
            field_pattern=entry.field_ref, description=description,
        ))

    if _has_errors(violations):
        return ExportResultSentinel(output=None, violations=violations)

    mappings.sort(key=lambda m: (m.severity, m.annotation_key, m.field_pattern))
    return ExportResultSentinel(
        output=SentinelExport(severity_mappings=mappings), violations=violations,
    )


# ── YAML Serializer ───────────────────────────────────


def yaml_dump(export_model: Any) -> str:
    if not hasattr(export_model, "model_dump"):
        raise TypeError(
            f"export_model must be a Pydantic v2 BaseModel instance, got {type(export_model).__name__}."
        )
    try:
        data = export_model.model_dump(exclude_none=True)
        return yaml.dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to serialize model to YAML: {e}")
