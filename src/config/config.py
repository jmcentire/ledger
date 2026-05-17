"""Configuration & Data Models for the Ledger system."""
from __future__ import annotations

import enum
import os
from contextlib import contextmanager
from typing import final
from types import MappingProxyType

import yaml
from pydantic import BaseModel, ConfigDict


# ── Enums ──────────────────────────────────────────────


@final
class ClassificationTier(str, enum.Enum):
    PUBLIC = "PUBLIC"
    PII = "PII"
    FINANCIAL = "FINANCIAL"
    AUTH = "AUTH"
    COMPLIANCE = "COMPLIANCE"


# ── Exceptions ─────────────────────────────────────────


class PlatformError(Exception):
    pass


class LedgerValidationError(Exception):
    def __init__(
        self, file_path: str, violations: list[str], source_exception: str = ""
    ):
        self.file_path = file_path
        self.violations = violations
        self.source_exception = source_exception
        super().__init__(f"Validation errors in {file_path}: {violations}")


# Re-export builtin so contract __all__ is satisfied
BlockingIOError = BlockingIOError


# ── Pydantic Models ────────────────────────────────────


class PropagationRule(BaseModel):
    model_config = ConfigDict(frozen=True)
    annotation_name: str
    pact_assertion_type: str
    arbiter_tier_behavior: str
    baton_masking_rule: str
    sentinel_severity: str


class Annotation(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    params: dict = {}


class Field(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    field_type: str
    classification: ClassificationTier
    nullable: bool = False
    annotations: list[Annotation] = []


AnnotationList = list[Annotation]


class SchemaFile(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    version: int
    fields: list[Field]
    raw_yaml: str
    source_path: str


FieldList = list[Field]


class Backend(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    enabled: bool = True
    base_url: str = ""
    timeout_ms: int = 5000


class MigrationGate(BaseModel):
    model_config = ConfigDict(frozen=True)
    rule_name: str
    passed: bool
    severity: str
    message: str
    field_name: str = ""
    schema_name: str = ""


MigrationGateList = list[MigrationGate]


class MigrationPlan(BaseModel):
    model_config = ConfigDict(frozen=True)
    plan_id: str
    schema_name: str
    from_version: int
    to_version: int
    gates: list[MigrationGate]
    approved: bool
    created_at: str


class ChangelogEntry(BaseModel):
    model_config = ConfigDict(frozen=True)
    entry_id: str
    schema_name: str
    version: int
    change_type: str
    timestamp: str
    description: str = ""
    migration_plan_id: str = ""


class ConstraintViolation(BaseModel):
    model_config = ConfigDict(frozen=True)
    violation_type: str
    annotations: list[str]
    message: str


ConstraintViolationList = list[ConstraintViolation]
StringList = list[str]


class CustomAnnotationDef(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    pact_assertion_type: str
    arbiter_tier_behavior: str
    baton_masking_rule: str
    sentinel_severity: str


CustomAnnotationDefList = list[CustomAnnotationDef]
BackendList = list[Backend]
SchemaFileList = list[SchemaFile]


class LedgerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_name: str
    schemas_dir: str
    changelog_path: str
    plans_dir: str
    backends: list[Backend]
    custom_annotations: list[CustomAnnotationDef] = []
    propagation_table: dict


class PropagationRuleDict(BaseModel):
    key: str
    value: PropagationRule


class ConflictsPairs(BaseModel):
    pair_1: list[str]
    pair_2: list[str]
    pair_3: list[str]


class FileLockHandle:
    __slots__ = ("lock_path", "fd", "exclusive")

    def __init__(self, lock_path: str, fd: int, exclusive: bool):
        self.lock_path = lock_path
        self.fd = fd
        self.exclusive = exclusive


# ── Constants ──────────────────────────────────────────


CONFLICTS: frozenset[frozenset[str]] = frozenset({
    frozenset({"immutable", "gdpr_erasable"}),
    frozenset({"audit_field", "gdpr_erasable"}),
    frozenset({"soft_delete_marker", "immutable"}),
})

REQUIRES: dict[str, frozenset[str]] = {
    "encrypted_at_rest": frozenset({"not_null"}),
}

_BUILTIN_PROPAGATION_TABLE: dict[str, PropagationRule] = {
    "immutable": PropagationRule(
        annotation_name="immutable",
        pact_assertion_type="field_present",
        arbiter_tier_behavior="block_downgrade",
        baton_masking_rule="no_mask",
        sentinel_severity="critical",
    ),
    "gdpr_erasable": PropagationRule(
        annotation_name="gdpr_erasable",
        pact_assertion_type="field_present",
        arbiter_tier_behavior="enforce_tier",
        baton_masking_rule="full_mask",
        sentinel_severity="high",
    ),
    "audit_field": PropagationRule(
        annotation_name="audit_field",
        pact_assertion_type="not_null",
        arbiter_tier_behavior="audit_only",
        baton_masking_rule="no_mask",
        sentinel_severity="high",
    ),
    "soft_delete_marker": PropagationRule(
        annotation_name="soft_delete_marker",
        pact_assertion_type="field_present",
        arbiter_tier_behavior="enforce_tier",
        baton_masking_rule="no_mask",
        sentinel_severity="medium",
    ),
    "encrypted_at_rest": PropagationRule(
        annotation_name="encrypted_at_rest",
        pact_assertion_type="type_match",
        arbiter_tier_behavior="enforce_tier",
        baton_masking_rule="full_mask",
        sentinel_severity="critical",
    ),
    "not_null": PropagationRule(
        annotation_name="not_null",
        pact_assertion_type="not_null",
        arbiter_tier_behavior="audit_only",
        baton_masking_rule="no_mask",
        sentinel_severity="low",
    ),
    "pii_field": PropagationRule(
        annotation_name="pii_field",
        pact_assertion_type="field_present",
        arbiter_tier_behavior="enforce_tier",
        baton_masking_rule="partial_mask",
        sentinel_severity="high",
    ),
    "primary_key": PropagationRule(
        annotation_name="primary_key",
        pact_assertion_type="not_null",
        arbiter_tier_behavior="audit_only",
        baton_masking_rule="no_mask",
        sentinel_severity="info",
    ),
}

_BUILTIN_TABLE_PROXY = MappingProxyType(_BUILTIN_PROPAGATION_TABLE)


# Stripe-specific built-in annotations — card and customer field defaults
STRIPE_BUILTINS: dict[str, dict] = {
    "stripe_card_number": {
        "description": "Stripe card number fields",
        "field_pattern": "*.card.number",
        "classification": "FINANCIAL",
        "annotations": ["encrypted_at_rest", "tokenized"],
        "propagation": {
            "pact_assertion_type": "type_match",
            "arbiter_tier_behavior": "enforce_tier",
            "baton_masking_rule": "full_mask",
            "sentinel_severity": "critical",
        },
    },
    "stripe_card_cvc": {
        "description": "Stripe card CVC/CVV fields",
        "field_pattern": "*.card.cvc",
        "classification": "FINANCIAL",
        "annotations": ["encrypted_at_rest", "tokenized"],
        "propagation": {
            "pact_assertion_type": "type_match",
            "arbiter_tier_behavior": "enforce_tier",
            "baton_masking_rule": "full_mask",
            "sentinel_severity": "critical",
        },
    },
    "stripe_card_exp": {
        "description": "Stripe card expiration fields",
        "field_pattern": "*.card.exp_*",
        "classification": "FINANCIAL",
        "annotations": ["encrypted_at_rest"],
        "propagation": {
            "pact_assertion_type": "type_match",
            "arbiter_tier_behavior": "enforce_tier",
            "baton_masking_rule": "partial_mask",
            "sentinel_severity": "high",
        },
    },
    "stripe_customer_email": {
        "description": "Stripe customer email fields",
        "field_pattern": "*.customer.email",
        "classification": "PII",
        "annotations": ["pii_field", "gdpr_erasable"],
        "propagation": {
            "pact_assertion_type": "field_present",
            "arbiter_tier_behavior": "enforce_tier",
            "baton_masking_rule": "partial_mask",
            "sentinel_severity": "high",
        },
    },
    "stripe_customer_name": {
        "description": "Stripe customer name fields",
        "field_pattern": "*.customer.name",
        "classification": "PII",
        "annotations": ["pii_field", "gdpr_erasable"],
        "propagation": {
            "pact_assertion_type": "field_present",
            "arbiter_tier_behavior": "enforce_tier",
            "baton_masking_rule": "partial_mask",
            "sentinel_severity": "high",
        },
    },
    "stripe_customer_phone": {
        "description": "Stripe customer phone fields",
        "field_pattern": "*.customer.phone",
        "classification": "PII",
        "annotations": ["pii_field", "gdpr_erasable"],
        "propagation": {
            "pact_assertion_type": "field_present",
            "arbiter_tier_behavior": "enforce_tier",
            "baton_masking_rule": "partial_mask",
            "sentinel_severity": "high",
        },
    },
    "stripe_customer_address": {
        "description": "Stripe customer address fields",
        "field_pattern": "*.customer.address.*",
        "classification": "PII",
        "annotations": ["pii_field", "gdpr_erasable"],
        "propagation": {
            "pact_assertion_type": "field_present",
            "arbiter_tier_behavior": "enforce_tier",
            "baton_masking_rule": "partial_mask",
            "sentinel_severity": "high",
        },
    },
}


def get_stripe_builtins() -> dict[str, dict]:
    """Return the Stripe-specific built-in annotation definitions."""
    return dict(STRIPE_BUILTINS)


# ── Public Functions ───────────────────────────────────


def get_builtin_propagation_table() -> MappingProxyType:
    return _BUILTIN_TABLE_PROXY


def get_conflicts() -> frozenset:
    return CONFLICTS


def get_requires() -> dict:
    return REQUIRES


def build_propagation_table(
    custom_annotations: list[CustomAnnotationDef],
) -> MappingProxyType:
    seen: dict[str, int] = {}
    for ca in custom_annotations:
        seen[ca.name] = seen.get(ca.name, 0) + 1
    duplicates = [n for n, c in seen.items() if c > 1]
    if duplicates:
        raise ValueError(f"Duplicate custom annotation names: {duplicates}")

    collisions = [ca.name for ca in custom_annotations if ca.name in _BUILTIN_PROPAGATION_TABLE]
    if collisions:
        raise ValueError(
            f"Custom annotation names collide with builtin annotations: {collisions}"
        )

    merged = dict(_BUILTIN_PROPAGATION_TABLE)
    for ca in custom_annotations:
        merged[ca.name] = PropagationRule(
            annotation_name=ca.name,
            pact_assertion_type=ca.pact_assertion_type,
            arbiter_tier_behavior=ca.arbiter_tier_behavior,
            baton_masking_rule=ca.baton_masking_rule,
            sentinel_severity=ca.sentinel_severity,
        )
    return MappingProxyType(merged)


def validate_annotation_set(annotations: list[str]) -> list[ConstraintViolation]:
    for ann in annotations:
        if ann == "":
            raise ValueError("Annotation names must be non-empty")

    ann_set = set(annotations)
    violations: list[ConstraintViolation] = []

    for pair in CONFLICTS:
        if pair.issubset(ann_set):
            pair_list = sorted(pair)
            violations.append(ConstraintViolation(
                violation_type="conflict",
                annotations=pair_list,
                message=f"Annotations {pair_list} conflict with each other",
            ))

    for source, required in REQUIRES.items():
        if source in ann_set:
            for req in sorted(required):
                if req not in ann_set:
                    violations.append(ConstraintViolation(
                        violation_type="missing_required",
                        annotations=[source, req],
                        message=f"Annotation '{source}' requires '{req}' to be present",
                    ))
    return violations


def parse_schema_file(path: str, propagation_table: dict) -> SchemaFile:
    try:
        with open(path, "r") as f:
            raw_content = f.read()
    except FileNotFoundError:
        raise
    except PermissionError:
        raise

    try:
        data = yaml.safe_load(raw_content)
    except yaml.YAMLError as e:
        raise LedgerValidationError(
            file_path=path, violations=[f"YAML parse error: {e}"],
            source_exception=str(e),
        )

    if not isinstance(data, dict):
        raise LedgerValidationError(
            file_path=path, violations=["Schema file must be a YAML mapping"],
        )

    all_violations: list[str] = []
    for req in ("name", "version", "fields"):
        if req not in data:
            all_violations.append(f"Missing required field: {req}")
    if all_violations:
        raise LedgerValidationError(file_path=path, violations=all_violations)

    fields_data = data.get("fields", [])
    if not isinstance(fields_data, list) or len(fields_data) == 0:
        raise LedgerValidationError(
            file_path=path,
            violations=["'fields' must be a non-empty list"],
        )

    fields: list[Field] = []
    for i, fd in enumerate(fields_data):
        if not isinstance(fd, dict):
            all_violations.append(f"Field {i}: must be a mapping")
            continue
        ann_data_list = fd.get("annotations", [])
        annotations: list[Annotation] = []
        if isinstance(ann_data_list, list):
            for ad in ann_data_list:
                if isinstance(ad, dict):
                    aname = ad.get("name", "")
                    if aname and aname not in propagation_table:
                        all_violations.append(
                            f"Field '{fd.get('name', i)}': unknown annotation '{aname}'"
                        )
                    annotations.append(Annotation(name=aname, params=ad.get("params", {})))

        ann_names = [a.name for a in annotations if a.name]
        for cv in validate_annotation_set(ann_names):
            all_violations.append(f"Field '{fd.get('name', i)}': {cv.message}")

        try:
            fields.append(Field(
                name=fd.get("name", ""),
                field_type=fd.get("field_type", ""),
                classification=ClassificationTier(fd.get("classification", "")),
                nullable=fd.get("nullable", False),
                annotations=annotations,
            ))
        except Exception as e:
            all_violations.append(f"Field {i}: {e}")

    if all_violations:
        raise LedgerValidationError(file_path=path, violations=all_violations)

    return SchemaFile(
        name=data["name"], version=data["version"], fields=fields,
        raw_yaml=raw_content, source_path=path,
    )


@contextmanager
def file_lock(path: str, exclusive: bool = True, blocking: bool = True):
    try:
        import fcntl
    except ImportError:
        raise PlatformError(
            "fcntl-based file locking is only supported on Unix platforms"
        )
    if fcntl is None:
        raise PlatformError(
            "fcntl-based file locking is only supported on Unix platforms"
        )

    lock_path = path + ".lock"
    parent = os.path.dirname(lock_path)
    if parent and not os.path.isdir(parent):
        raise FileNotFoundError(f"Parent directory does not exist: {parent}")

    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
    lock_flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    if not blocking:
        lock_flags |= fcntl.LOCK_NB
    try:
        fcntl.flock(fd, lock_flags)
    except BlockingIOError:
        os.close(fd)
        raise

    handle = FileLockHandle(lock_path=lock_path, fd=fd, exclusive=exclusive)
    try:
        yield handle
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def load_config(path: str) -> LedgerConfig:
    try:
        with open(path, "r") as f:
            raw = f.read()
    except FileNotFoundError:
        raise
    except PermissionError:
        raise

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise LedgerValidationError(
            file_path=path, violations=[f"YAML parse error: {e}"],
            source_exception=str(e),
        )

    if not isinstance(data, dict):
        raise LedgerValidationError(
            file_path=path, violations=["ledger.yaml must be a YAML mapping"],
        )

    violations: list[str] = []
    for rf in ("project_name", "schemas_dir", "changelog_path", "plans_dir"):
        if rf not in data:
            violations.append(f"Missing required field: {rf}")
    if violations:
        raise LedgerValidationError(file_path=path, violations=violations)

    custom_annotations: list[CustomAnnotationDef] = []
    for ca_data in data.get("custom_annotations", []) or []:
        if isinstance(ca_data, dict):
            try:
                custom_annotations.append(CustomAnnotationDef(**ca_data))
            except Exception as e:
                violations.append(f"Invalid custom annotation: {e}")
    if violations:
        raise LedgerValidationError(file_path=path, violations=violations)

    try:
        prop_table = build_propagation_table(custom_annotations)
    except ValueError as e:
        raise LedgerValidationError(file_path=path, violations=[str(e)])

    backends: list[Backend] = []
    for b_data in data.get("backends", []) or []:
        if isinstance(b_data, dict):
            try:
                backends.append(Backend(**b_data))
            except Exception as e:
                violations.append(f"Invalid backend: {e}")
    if violations:
        raise LedgerValidationError(file_path=path, violations=violations)

    schemas_dir = data["schemas_dir"]
    if os.path.isdir(schemas_dir):
        for fname in sorted(os.listdir(schemas_dir)):
            if fname.endswith((".yaml", ".yml")):
                sp = os.path.join(schemas_dir, fname)
                try:
                    parse_schema_file(sp, prop_table)
                except LedgerValidationError as e:
                    violations.extend(e.violations)
                except Exception as e:
                    violations.append(f"Error parsing {sp}: {e}")
    if violations:
        raise LedgerValidationError(file_path=path, violations=violations)

    return LedgerConfig(
        project_name=data["project_name"],
        schemas_dir=data["schemas_dir"],
        changelog_path=data["changelog_path"],
        plans_dir=data["plans_dir"],
        backends=backends,
        custom_annotations=custom_annotations,
        propagation_table=dict(prop_table),
    )


def init_config(config_path: str) -> None:
    """Write a minimal ledger.yaml scaffold to config_path.

    Creates the schemas/, plans/ directories and an empty changelog.yaml
    alongside the config file if they don't already exist.
    """
    base = os.path.dirname(os.path.abspath(config_path))
    schemas_dir = os.path.join(base, "schemas")
    plans_dir = os.path.join(base, "plans")
    changelog_path = os.path.join(base, "changelog.yaml")

    os.makedirs(schemas_dir, exist_ok=True)
    os.makedirs(plans_dir, exist_ok=True)
    if not os.path.exists(changelog_path):
        with open(changelog_path, "w") as f:
            f.write("")

    project_name = os.path.basename(base) or "my-project"
    scaffold = (
        f"project_name: {project_name}\n"
        f"schemas_dir: {schemas_dir}\n"
        f"changelog_path: {changelog_path}\n"
        f"plans_dir: {plans_dir}\n"
        "backends: []\n"
        "custom_annotations: []\n"
    )
    with open(config_path, "w") as f:
        f.write(scaffold)
