"""
Adversarial hidden acceptance tests for the Root component.
These tests detect implementations that "teach to the test" by hardcoding
return values or taking shortcuts that only satisfy the visible tests.
"""
import os
import sys
import enum
import typing
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock
from types import MappingProxyType

import pytest


# ---------------------------------------------------------------------------
# Import the component under test
# ---------------------------------------------------------------------------
from src.root import *

# Also import the package itself for attribute/module-level inspection
try:
    import src.root as ledger_pkg
except ImportError:
    ledger_pkg = None

# Try importing types and protocols modules directly
try:
    from src.root.types import (
        Severity, BackendType, ExportFormat, PlanStatus, ClassificationTier,
        Violation, LedgerError, BootstrapError, VersionInfo,
    )
except ImportError:
    pass  # Already imported via star import

try:
    from src.root.protocols import (
        RegistryProtocol, MigrationProtocol, ExportProtocol,
        MockProtocol, ConfigProtocol, ApiProtocol,
    )
except ImportError:
    pass  # Already imported via star import


# ===========================================================================
# Enum behavioral tests
# ===========================================================================

class TestGoodhartEnumStringEquality:
    """StrEnum members should be directly comparable to their string values."""

    def test_goodhart_severity_all_members_equal_strings(self):
        for member in Severity:
            assert member == member.value, f"Severity.{member.name} != '{member.value}'"
            assert isinstance(member, str), f"Severity.{member.name} is not a str instance"

    def test_goodhart_backend_type_all_members_equal_strings(self):
        for member in BackendType:
            assert member == member.value
            assert isinstance(member, str)

    def test_goodhart_export_format_all_members_equal_strings(self):
        for member in ExportFormat:
            assert member == member.value
            assert isinstance(member, str)

    def test_goodhart_plan_status_all_members_equal_strings(self):
        for member in PlanStatus:
            assert member == member.value
            assert isinstance(member, str)


class TestGoodhartClassificationTier:
    """ClassificationTier enum correctness."""

    def test_goodhart_classification_tier_uppercase_values(self):
        assert ClassificationTier.PUBLIC == "PUBLIC"
        assert ClassificationTier.PII == "PII"
        assert ClassificationTier.FINANCIAL == "FINANCIAL"
        assert ClassificationTier.AUTH == "AUTH"
        assert ClassificationTier.COMPLIANCE == "COMPLIANCE"

    def test_goodhart_classification_tier_is_str_subclass(self):
        """ClassificationTier members should be str instances (StrEnum behavior)."""
        for member in ClassificationTier:
            assert isinstance(member, str), (
                f"ClassificationTier.{member.name} is not a str instance"
            )

    def test_goodhart_classification_tier_values_are_uppercase(self):
        """All ClassificationTier values must be uppercase."""
        for member in ClassificationTier:
            assert member.value == member.value.upper(), (
                f"ClassificationTier.{member.name} value '{member.value}' is not uppercase"
            )


class TestGoodhartEnumConstruction:
    """StrEnums should be constructible from string values."""

    def test_goodhart_severity_constructible_from_string(self):
        assert Severity("info") == Severity.info
        assert Severity("warning") == Severity.warning
        assert Severity("error") == Severity.error
        assert Severity("critical") == Severity.critical

    def test_goodhart_backend_type_constructible_from_string(self):
        assert BackendType("postgres") == BackendType.postgres
        assert BackendType("kafka") == BackendType.kafka
        assert BackendType("custom") == BackendType.custom

    def test_goodhart_export_format_constructible_from_string(self):
        assert ExportFormat("pact") == ExportFormat.pact
        assert ExportFormat("sentinel") == ExportFormat.sentinel

    def test_goodhart_plan_status_constructible_from_string(self):
        assert PlanStatus("pending") == PlanStatus.pending
        assert PlanStatus("approved") == PlanStatus.approved
        assert PlanStatus("rejected") == PlanStatus.rejected


class TestGoodhartEnumInvalidValues:
    """StrEnum types should reject invalid string values."""

    def test_goodhart_severity_invalid_value_raises(self):
        with pytest.raises(ValueError):
            Severity("nonexistent")

    def test_goodhart_severity_case_sensitive(self):
        with pytest.raises(ValueError):
            Severity("INFO")  # lowercase only per contract

    def test_goodhart_backend_type_invalid_value_raises(self):
        with pytest.raises(ValueError):
            BackendType("oracle")

    def test_goodhart_export_format_invalid_value_raises(self):
        with pytest.raises(ValueError):
            ExportFormat("json")

    def test_goodhart_plan_status_invalid_value_raises(self):
        with pytest.raises(ValueError):
            PlanStatus("cancelled")


class TestGoodhartEnumIterationAndLength:
    """Enum iteration order and length."""

    def test_goodhart_severity_iteration_order(self):
        members = [m.value for m in Severity]
        assert members == ["info", "warning", "error", "critical"]

    def test_goodhart_plan_status_iteration_order(self):
        members = [m.value for m in PlanStatus]
        assert members == ["pending", "approved", "rejected"]

    def test_goodhart_severity_len(self):
        assert len(Severity) == 4

    def test_goodhart_backend_type_len(self):
        assert len(BackendType) == 8

    def test_goodhart_export_format_len(self):
        assert len(ExportFormat) == 4

    def test_goodhart_plan_status_len(self):
        assert len(PlanStatus) == 3

    def test_goodhart_classification_tier_len(self):
        assert len(ClassificationTier) == 5


# ===========================================================================
# Violation tests
# ===========================================================================

class TestGoodhartViolation:
    """Violation struct behavioral correctness."""

    def test_goodhart_violation_nested_context(self):
        """Violation.context must handle arbitrary nested dicts, not just flat ones."""
        ctx = {"nested": {"key": [1, 2, 3]}, "flag": True, "count": 42}
        v = Violation(
            severity=Severity.info,
            message="test",
            code="TEST",
            path="/test",
            context=ctx,
        )
        assert v.context["nested"]["key"] == [1, 2, 3]
        assert v.context["flag"] is True
        assert v.context["count"] == 42

    def test_goodhart_violation_empty_context(self):
        """Violation should accept empty dict for context."""
        v = Violation(
            severity=Severity.critical,
            message="msg",
            code="C001",
            path="/p",
            context={},
        )
        assert v.context == {}

    def test_goodhart_violation_all_severity_levels(self):
        """Violation should accept every Severity level."""
        for sev in Severity:
            v = Violation(
                severity=sev,
                message=f"msg for {sev}",
                code="X",
                path="/x",
                context={},
            )
            assert v.severity == sev

    def test_goodhart_violation_fields_accessible(self):
        """All Violation fields should be individually accessible as attributes."""
        v = Violation(
            severity=Severity.warning,
            message="field deprecated",
            code="DEPRECATION_001",
            path="schemas/users.yaml:line:5",
            context={"field": "ssn", "replacement": "ssn_hash"},
        )
        assert v.severity == Severity.warning
        assert v.message == "field deprecated"
        assert v.code == "DEPRECATION_001"
        assert v.path == "schemas/users.yaml:line:5"
        assert v.context["field"] == "ssn"
        assert v.context["replacement"] == "ssn_hash"


# ===========================================================================
# Error type tests
# ===========================================================================

class TestGoodhartLedgerError:
    """LedgerError must be a proper exception."""

    def test_goodhart_ledger_error_is_exception(self):
        assert issubclass(LedgerError, Exception)

    def test_goodhart_ledger_error_raisable(self):
        v = Violation(
            severity=Severity.error,
            message="test",
            code="E001",
            path="/",
            context={},
        )
        err = LedgerError(message="boom", violations=[v], exit_code=1)
        with pytest.raises(LedgerError):
            raise err

    def test_goodhart_ledger_error_violations_are_violation_instances(self):
        v1 = Violation(severity=Severity.error, message="a", code="A", path="/a", context={})
        v2 = Violation(severity=Severity.warning, message="b", code="B", path="/b", context={})
        err = LedgerError(message="multi", violations=[v1, v2], exit_code=2)
        assert isinstance(err.violations, list)
        for v in err.violations:
            assert isinstance(v, Violation)


class TestGoodhartBootstrapError:
    """BootstrapError inheritance and structure."""

    def test_goodhart_bootstrap_error_inherits_ledger_error(self):
        assert issubclass(BootstrapError, LedgerError)

    def test_goodhart_bootstrap_error_is_exception(self):
        assert issubclass(BootstrapError, Exception)

    def test_goodhart_bootstrap_error_catchable_as_ledger_error(self):
        v = Violation(severity=Severity.error, message="x", code="X", path="/", context={})
        err = BootstrapError(
            message="bootstrap failed",
            violations=[v],
            config_path="/some/path.yaml",
            exit_code=1,
        )
        with pytest.raises(LedgerError):
            raise err

    def test_goodhart_bootstrap_error_raisable(self):
        err = BootstrapError(
            message="fail",
            violations=[],
            config_path="/cfg.yaml",
            exit_code=1,
        )
        with pytest.raises(BootstrapError):
            raise err


# ===========================================================================
# ViolationList tests
# ===========================================================================

class TestGoodhartViolationList:
    """ViolationList should support standard list operations."""

    def test_goodhart_violation_list_append_and_extend(self):
        vl = []  # ViolationList is just a list alias
        v1 = Violation(severity=Severity.info, message="a", code="A", path="/", context={})
        v2 = Violation(severity=Severity.warning, message="b", code="B", path="/", context={})
        v3 = Violation(severity=Severity.error, message="c", code="C", path="/", context={})
        vl.append(v1)
        assert len(vl) == 1
        vl.extend([v2, v3])
        assert len(vl) == 3
        assert all(isinstance(v, Violation) for v in vl)


# ===========================================================================
# resolve_config_path tests
# ===========================================================================

class TestGoodhartResolveConfigPath:
    """Behavioral tests for resolve_config_path."""

    def test_goodhart_none_explicit_falls_to_default(self):
        """None as explicit_path should be treated as 'not provided'."""
        env_key = "GOODHART_TEST_CFG_NONE"
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(env_key, None)
            result = resolve_config_path(None, env_key)
        assert os.path.isabs(result)
        assert result.endswith("ledger.yaml")

    def test_goodhart_whitespace_only_env_var_ignored(self):
        """A whitespace-only env var should be treated as empty/unset."""
        env_key = "GOODHART_TEST_CFG_WS"
        with patch.dict(os.environ, {env_key: "   "}):
            result = resolve_config_path("", env_key)
        # Should fall through to default since whitespace-only is effectively empty
        assert os.path.isabs(result)
        assert result.endswith("ledger.yaml")

    def test_goodhart_custom_env_var_name(self):
        """resolve_config_path should use the provided env_var_name, not a hardcoded name."""
        custom_key = "MY_TOTALLY_CUSTOM_VAR_12345"
        with patch.dict(os.environ, {custom_key: "/from/custom/var.yaml"}):
            result = resolve_config_path("", custom_key)
        assert result == "/from/custom/var.yaml"

    def test_goodhart_absolute_explicit_unchanged(self):
        """An already-absolute explicit path should be returned as-is."""
        result = resolve_config_path("/absolute/path/to/config.yaml", "UNUSED_VAR")
        assert result == "/absolute/path/to/config.yaml"

    def test_goodhart_relative_path_with_parent_refs(self):
        """Paths with .. components should be properly resolved."""
        result = resolve_config_path("relative/../other/ledger.yaml", "UNUSED_VAR")
        assert os.path.isabs(result)
        # The .. should be resolved
        assert ".." not in os.path.normpath(result)

    def test_goodhart_env_var_with_spaces_in_path(self):
        """Paths with spaces should be handled correctly."""
        env_key = "GOODHART_TEST_CFG_SPACES"
        with patch.dict(os.environ, {env_key: "/path/with spaces/ledger.yaml"}):
            result = resolve_config_path("", env_key)
        assert os.path.isabs(result)
        assert "with spaces" in result

    def test_goodhart_env_var_overrides_default_not_default(self):
        """When env var is set and explicit is empty, env var wins over default."""
        env_key = "GOODHART_TEST_CFG_OVERRIDE"
        with patch.dict(os.environ, {env_key: "/custom/from/env.yaml"}):
            result = resolve_config_path("", env_key)
        assert result == "/custom/from/env.yaml"
        assert not result.endswith("ledger.yaml") or "custom" in result

    def test_goodhart_explicit_always_wins_over_env(self):
        """Even if env var is set, explicit_path should take priority."""
        env_key = "GOODHART_TEST_CFG_EXPLICIT_WINS"
        with patch.dict(os.environ, {env_key: "/from/env.yaml"}):
            result = resolve_config_path("/from/explicit.yaml", env_key)
        assert result == "/from/explicit.yaml"


# ===========================================================================
# get_version tests
# ===========================================================================

class TestGoodhartGetVersion:
    """Behavioral tests for get_version."""

    def test_goodhart_get_version_returns_actual_str(self):
        """get_version must return a real str, not a proxy object."""
        result = get_version()
        assert type(result) is str
        assert len(result) > 0

    def test_goodhart_get_version_not_none(self):
        result = get_version()
        assert result is not None

    def test_goodhart_get_version_no_leading_trailing_whitespace(self):
        result = get_version()
        assert result == result.strip()


# ===========================================================================
# get_version_info tests
# ===========================================================================

class TestGoodhartGetVersionInfo:
    """Behavioral tests for get_version_info."""

    def test_goodhart_pydantic_version_looks_like_version(self):
        """pydantic_version should contain digits and dots like a real version."""
        info = get_version_info()
        assert len(info.pydantic_version) > 0
        assert any(c.isdigit() for c in info.pydantic_version)
        assert "." in info.pydantic_version

    def test_goodhart_version_info_consistent_with_get_version(self):
        """get_version_info().version should match get_version()."""
        info = get_version_info()
        version = get_version()
        assert info.version == version

    def test_goodhart_python_version_format(self):
        """python_version should be formatted like 'X.Y.Z'."""
        info = get_version_info()
        parts = info.python_version.split(".")
        assert len(parts) >= 2  # At least major.minor
        assert parts[0].isdigit()

    def test_goodhart_version_info_is_correct_type(self):
        """get_version_info should return a VersionInfo instance."""
        info = get_version_info()
        assert isinstance(info, VersionInfo)
        assert hasattr(info, "version")
        assert hasattr(info, "python_version")
        assert hasattr(info, "pydantic_version")

    def test_goodhart_python_version_matches_sys(self):
        """python_version must match the actual running interpreter."""
        info = get_version_info()
        expected = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        assert info.python_version == expected


# ===========================================================================
# __all__ / Public Exports tests
# ===========================================================================

class TestGoodhartPublicExports:
    """Public exports correctness."""

    def test_goodhart_all_contains_required_names(self):
        """__all__ must contain all documented public names."""
        all_names = ledger_pkg.__all__
        required = [
            "__version__", "Severity", "BackendType", "ExportFormat",
            "PlanStatus", "ClassificationTier", "Violation", "LedgerError",
            "RegistryProtocol", "MigrationProtocol", "ExportProtocol",
            "MockProtocol", "ConfigProtocol", "ApiProtocol",
            "Ledger", "create_ledger", "get_version_info",
        ]
        for name in required:
            assert name in all_names, f"'{name}' missing from __all__"

    def test_goodhart_all_no_private_names_except_version(self):
        """__all__ should not export private names except __version__."""
        all_names = ledger_pkg.__all__
        for name in all_names:
            if name.startswith("_") and name != "__version__":
                pytest.fail(f"Private name '{name}' found in __all__")

    def test_goodhart_dunder_version_matches_get_version(self):
        """__version__ attribute should match get_version() return value."""
        assert ledger_pkg.__version__ == get_version()

    def test_goodhart_all_names_are_importable(self):
        """Every name in __all__ should be an actual attribute of the package."""
        for name in ledger_pkg.__all__:
            assert hasattr(ledger_pkg, name), f"'{name}' in __all__ but not an attribute"


# ===========================================================================
# Protocol tests
# ===========================================================================

class TestGoodhartProtocols:
    """Protocol types should be structural typing protocols."""

    def test_goodhart_registry_protocol_is_protocol(self):
        """RegistryProtocol should be based on typing.Protocol."""
        # Check that it's a Protocol subclass
        assert hasattr(RegistryProtocol, '__protocol_attrs__') or \
               issubclass(type(RegistryProtocol), type(typing.Protocol)) or \
               typing.Protocol in getattr(RegistryProtocol, '__mro__', []), \
               "RegistryProtocol should be a typing.Protocol"

    def test_goodhart_migration_protocol_is_protocol(self):
        assert typing.Protocol in getattr(MigrationProtocol, '__mro__', []) or \
               hasattr(MigrationProtocol, '__protocol_attrs__')

    def test_goodhart_export_protocol_is_protocol(self):
        assert typing.Protocol in getattr(ExportProtocol, '__mro__', []) or \
               hasattr(ExportProtocol, '__protocol_attrs__')

    def test_goodhart_mock_protocol_is_protocol(self):
        assert typing.Protocol in getattr(MockProtocol, '__mro__', []) or \
               hasattr(MockProtocol, '__protocol_attrs__')

    def test_goodhart_config_protocol_is_protocol(self):
        assert typing.Protocol in getattr(ConfigProtocol, '__mro__', []) or \
               hasattr(ConfigProtocol, '__protocol_attrs__')

    def test_goodhart_api_protocol_is_protocol(self):
        assert typing.Protocol in getattr(ApiProtocol, '__mro__', []) or \
               hasattr(ApiProtocol, '__protocol_attrs__')


# ===========================================================================
# create_ledger tests (with mocked dependencies)
# ===========================================================================

class TestGoodhartCreateLedger:
    """Behavioral tests for create_ledger with mocked dependencies."""

    def _make_mock_config(self):
        """Create a mock LedgerConfig-like object."""
        config = MagicMock()
        config.root = Path(tempfile.mkdtemp())
        config.schemas_dir = config.root / "schemas"
        config.custom_annotations = []
        # Make propagation_table an immutable mapping
        config.propagation_table = MappingProxyType({"pii": {"mask": True}})
        return config

    @patch("src.root.registry")
    @patch("src.root.config")
    def test_goodhart_create_ledger_frozen_rejects_setattr(self, mock_config_mod, mock_registry):
        """The Ledger container must reject attribute modification after construction."""
        cfg = self._make_mock_config()
        mock_config_mod.load_config.return_value = cfg
        mock_config_mod.build_propagation_table.return_value = MappingProxyType({})
        mock_config_mod.get_builtin_propagation_table.return_value = {}

        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            f.write("component_id: test\n")
            f.flush()
            try:
                ledger = create_ledger(f.name)
                with pytest.raises((AttributeError, TypeError, Exception)):
                    ledger.version = "hacked"
                with pytest.raises((AttributeError, TypeError, Exception)):
                    ledger.config = None
            except BootstrapError:
                pytest.skip("Could not bootstrap with mocks - skipping freeze test")
            finally:
                os.unlink(f.name)

    @patch("src.root.registry")
    @patch("src.root.config")
    def test_goodhart_create_ledger_frozen_rejects_delattr(self, mock_config_mod, mock_registry):
        """The Ledger container must reject attribute deletion."""
        cfg = self._make_mock_config()
        mock_config_mod.load_config.return_value = cfg
        mock_config_mod.build_propagation_table.return_value = MappingProxyType({})
        mock_config_mod.get_builtin_propagation_table.return_value = {}

        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            f.write("component_id: test\n")
            f.flush()
            try:
                ledger = create_ledger(f.name)
                with pytest.raises((AttributeError, TypeError, Exception)):
                    del ledger.version
            except BootstrapError:
                pytest.skip("Could not bootstrap with mocks - skipping freeze test")
            finally:
                os.unlink(f.name)

    def test_goodhart_create_ledger_config_not_found_has_config_path(self):
        """BootstrapError for missing config should carry the config_path."""
        nonexistent = "/absolutely/nonexistent/path/config_12345.yaml"
        with pytest.raises(BootstrapError) as exc_info:
            create_ledger(nonexistent)
        assert exc_info.value.config_path is not None
        assert len(exc_info.value.config_path) > 0

    def test_goodhart_create_ledger_config_not_found_has_nonzero_exit(self):
        """BootstrapError should have a non-zero exit_code."""
        nonexistent = "/absolutely/nonexistent/path/config_67890.yaml"
        with pytest.raises(BootstrapError) as exc_info:
            create_ledger(nonexistent)
        assert isinstance(exc_info.value.exit_code, int)
        assert exc_info.value.exit_code != 0

    def test_goodhart_create_ledger_config_not_found_has_violations(self):
        """BootstrapError should carry a violations list (may be empty or populated)."""
        nonexistent = "/absolutely/nonexistent/path/config_violations.yaml"
        with pytest.raises(BootstrapError) as exc_info:
            create_ledger(nonexistent)
        assert hasattr(exc_info.value, "violations")
        assert isinstance(exc_info.value.violations, list)


# ===========================================================================
# validate_import_graph tests
# ===========================================================================

class TestGoodhartValidateImportGraph:
    """Behavioral tests for validate_import_graph."""

    def _create_source_tree(self, tmpdir, files):
        """Helper to create a mock source tree."""
        ledger_dir = tmpdir / "ledger"
        ledger_dir.mkdir(parents=True)
        for relpath, content in files.items():
            fpath = ledger_dir / relpath
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(textwrap.dedent(content))
        return str(ledger_dir)

    def test_goodhart_types_importing_subpackage_is_violation(self, tmp_path):
        """types.py importing from a ledger subpackage should be flagged."""
        src = self._create_source_tree(tmp_path, {
            "__init__.py": "",
            "types.py": "from ledger.config import load_config\n",
            "protocols.py": "",
            "config/__init__.py": "",
        })
        violations = validate_import_graph(src)
        assert len(violations) > 0
        assert any("types" in str(getattr(v, 'path', v)) for v in violations)

    def test_goodhart_protocols_importing_subpackage_is_violation(self, tmp_path):
        """protocols.py importing from a ledger subpackage should be flagged."""
        src = self._create_source_tree(tmp_path, {
            "__init__.py": "",
            "types.py": "",
            "protocols.py": "from ledger.registry import init\n",
            "registry/__init__.py": "",
        })
        violations = validate_import_graph(src)
        assert len(violations) > 0
        assert any("protocols" in str(getattr(v, 'path', v)) for v in violations)

    def test_goodhart_violation_code_is_import_violation(self, tmp_path):
        """All violations from validate_import_graph must have code='IMPORT_VIOLATION'."""
        src = self._create_source_tree(tmp_path, {
            "__init__.py": "",
            "types.py": "from ledger.config import load_config\n",
            "protocols.py": "from ledger.registry import init\n",
            "config/__init__.py": "from ledger.registry import something\n",
            "registry/__init__.py": "",
        })
        violations = validate_import_graph(src)
        assert len(violations) > 0
        for v in violations:
            assert v.code == "IMPORT_VIOLATION", (
                f"Violation code should be 'IMPORT_VIOLATION', got '{v.code}'"
            )

    def test_goodhart_violation_path_nonempty(self, tmp_path):
        """Each violation must have a non-empty path referencing a .py file."""
        src = self._create_source_tree(tmp_path, {
            "__init__.py": "",
            "types.py": "from ledger.config import x\n",
            "protocols.py": "",
            "config/__init__.py": "",
        })
        violations = validate_import_graph(src)
        assert len(violations) > 0
        for v in violations:
            assert v.path, "Violation path must be non-empty"
            assert ".py" in v.path, f"Violation path should reference a .py file, got: {v.path}"

    def test_goodhart_sibling_import_detected(self, tmp_path):
        """Subpackage importing from sibling subpackage should be flagged."""
        src = self._create_source_tree(tmp_path, {
            "__init__.py": "",
            "types.py": "",
            "protocols.py": "",
            "config/__init__.py": "from ledger.registry import init\n",
            "registry/__init__.py": "",
        })
        violations = validate_import_graph(src)
        assert len(violations) > 0

    def test_goodhart_subpackage_importing_from_types_is_clean(self, tmp_path):
        """Subpackage importing from ledger.types should NOT be a violation."""
        src = self._create_source_tree(tmp_path, {
            "__init__.py": "",
            "types.py": "",
            "protocols.py": "",
            "config/__init__.py": "from ledger.types import Severity\n",
            "registry/__init__.py": "from ledger.protocols import RegistryProtocol\n",
        })
        violations = validate_import_graph(src)
        assert len(violations) == 0, f"Imports from ledger.types/protocols should be allowed, got {len(violations)} violations"

    def test_goodhart_source_root_not_found_raises(self):
        """validate_import_graph should raise when source_root doesn't exist."""
        with pytest.raises(Exception):
            validate_import_graph("/nonexistent/path/that/does/not/exist/12345")
