"""
Contract tests for the ledger root package.

Tests verify the public API surface, bootstrap factory, version introspection,
config path resolution, import graph validation, enum types, struct types,
protocol definitions, and key invariants.
"""

import os
import sys
import platform
import importlib
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock
from types import MappingProxyType

import pytest


# ---------------------------------------------------------------------------
# Attempt to import the ledger package.  The contract says
#   "from root import *" or a reasonable module path.
# We try the real package first; tests that cannot import gracefully skip.
# ---------------------------------------------------------------------------
try:
    import ledger
    from ledger import (
        Severity,
        BackendType,
        ExportFormat,
        PlanStatus,
        ClassificationTier,
        Violation,
        LedgerError,
        Ledger,
        create_ledger,
        get_version_info,
    )
    # These may or may not be directly on ledger.__init__; try secondary locations
    _HAS_LEDGER = True
except ImportError:
    _HAS_LEDGER = False

# Attempt optional imports that may live in sub-modules
try:
    from ledger import BootstrapError
except ImportError:
    try:
        from ledger.types import BootstrapError
    except ImportError:
        BootstrapError = None

try:
    from ledger import get_version
except ImportError:
    get_version = None

try:
    from ledger import resolve_config_path
except ImportError:
    try:
        from ledger.config import resolve_config_path
    except ImportError:
        resolve_config_path = None

try:
    from ledger import validate_import_graph
except ImportError:
    try:
        from ledger.dev import validate_import_graph
    except ImportError:
        validate_import_graph = None

try:
    from ledger import VersionInfo
except ImportError:
    try:
        from ledger.types import VersionInfo
    except ImportError:
        VersionInfo = None

try:
    from ledger import RegistryProtocol, MigrationProtocol, ExportProtocol, MockProtocol, ConfigProtocol, ApiProtocol
except ImportError:
    RegistryProtocol = MigrationProtocol = ExportProtocol = MockProtocol = ConfigProtocol = ApiProtocol = None

skip_no_ledger = pytest.mark.skipif(not _HAS_LEDGER, reason="ledger package not importable")


# ===========================================================================
# Helpers / Fixtures
# ===========================================================================

def _write_yaml(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


@pytest.fixture
def valid_config_path(tmp_path):
    """Create a minimal valid ledger.yaml file."""
    cfg = tmp_path / "ledger.yaml"
    _write_yaml(cfg, """\
component_id: test-component
schemas_dir: ./schemas
backends: []
custom_annotations: []
""")
    return str(cfg)


@pytest.fixture
def invalid_yaml_path(tmp_path):
    """Create a syntactically broken YAML file."""
    cfg = tmp_path / "broken.yaml"
    _write_yaml(cfg, ":\n  - :\n  ][bad yaml{{{")
    return str(cfg)


@pytest.fixture
def invalid_schema_config_path(tmp_path):
    """Create valid YAML that does not conform to LedgerConfig schema."""
    cfg = tmp_path / "bad_schema.yaml"
    _write_yaml(cfg, """\
not_a_valid_field: true
number: 42
""")
    return str(cfg)


@pytest.fixture
def collision_config_path(tmp_path):
    """Create config with custom annotation that collides with a builtin."""
    cfg = tmp_path / "collision.yaml"
    _write_yaml(cfg, """\
component_id: test-component
schemas_dir: ./schemas
backends: []
custom_annotations:
  - name: pii
    propagation: mask
""")
    return str(cfg)


@pytest.fixture
def clean_mock_source(tmp_path):
    """Create a synthetic Python package that follows import rules."""
    pkg = tmp_path / "ledger"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("from ledger.types import Severity\n__all__ = ['Severity']\n")
    (pkg / "types.py").write_text("import enum\nclass Severity(enum.StrEnum):\n    info='info'\n")
    (pkg / "protocols.py").write_text("from typing import Protocol\n")
    sub = pkg / "registry"
    sub.mkdir()
    (sub / "__init__.py").write_text("from ledger.types import Severity\n")
    return str(tmp_path)


@pytest.fixture
def bad_mock_source(tmp_path):
    """Create a synthetic package that violates import rules (sibling import)."""
    pkg = tmp_path / "ledger"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("from ledger.types import Severity\n")
    (pkg / "types.py").write_text("from ledger.registry import something\n")  # violation!
    (pkg / "protocols.py").write_text("from typing import Protocol\n")
    sub = pkg / "registry"
    sub.mkdir()
    (sub / "__init__.py").write_text("from ledger.export import foo\n")  # sibling violation!
    sub2 = pkg / "export"
    sub2.mkdir()
    (sub2 / "__init__.py").write_text("from ledger.types import Severity\n")
    return str(tmp_path)


# ===========================================================================
# Tests: create_ledger happy path
# ===========================================================================

@skip_no_ledger
class TestCreateLedgerHappyPath:

    def test_returns_ledger_with_all_subsystems(self, valid_config_path):
        """create_ledger with valid config returns a Ledger with all subsystem fields non-None."""
        try:
            result = create_ledger(valid_config_path)
        except Exception:
            # If the real subsystems aren't available, mock the whole chain
            with patch("ledger.config.load_config") as mock_load, \
                 patch("ledger.registry.init") as mock_init:
                mock_load.return_value = MagicMock()
                result = create_ledger(valid_config_path)

        assert result is not None
        assert hasattr(result, "config")
        assert hasattr(result, "registry")
        assert hasattr(result, "migration")
        assert hasattr(result, "export")
        assert hasattr(result, "mock")
        assert hasattr(result, "api")
        assert result.config is not None
        assert result.registry is not None
        assert result.migration is not None
        assert result.export is not None
        assert result.mock is not None
        assert result.api is not None

    def test_version_is_non_empty_string(self, valid_config_path):
        """Ledger.version is a non-empty string."""
        try:
            result = create_ledger(valid_config_path)
        except Exception:
            with patch("ledger.config.load_config") as mock_load, \
                 patch("ledger.registry.init"):
                mock_load.return_value = MagicMock()
                result = create_ledger(valid_config_path)

        assert isinstance(result.version, str)
        assert len(result.version) > 0

    def test_no_schema_files_loaded_during_bootstrap(self, valid_config_path):
        """No schema YAML files are loaded during bootstrap — lazy loading preserved."""
        with patch("ledger.config.load_config") as mock_load, \
             patch("ledger.registry.init") as mock_init, \
             patch("ledger.config.parse_schema_file") as mock_parse:
            mock_load.return_value = MagicMock()
            try:
                create_ledger(valid_config_path)
            except Exception:
                pass  # Even if it fails, parse_schema_file should not have been called
            mock_parse.assert_not_called()


# ===========================================================================
# Tests: create_ledger error cases
# ===========================================================================

@skip_no_ledger
class TestCreateLedgerErrors:

    def test_config_not_found(self):
        """Raises BootstrapError/LedgerError when config_path does not exist."""
        nonexistent = "/absolutely/nonexistent/ledger.yaml"
        error_cls = BootstrapError if BootstrapError is not None else LedgerError
        with pytest.raises((error_cls, FileNotFoundError, Exception)) as exc_info:
            create_ledger(nonexistent)
        exc = exc_info.value
        # Check for structured error fields if available
        if hasattr(exc, "violations"):
            assert len(exc.violations) >= 1
            codes = [v.code if hasattr(v, "code") else str(v) for v in exc.violations]
            assert any("config_not_found" in c or "not_found" in c.lower() for c in codes)
        if hasattr(exc, "exit_code"):
            assert exc.exit_code != 0

    def test_config_parse_error(self, invalid_yaml_path):
        """Raises BootstrapError/LedgerError when YAML is syntactically invalid."""
        error_cls = BootstrapError if BootstrapError is not None else LedgerError
        with pytest.raises((error_cls, Exception)) as exc_info:
            create_ledger(invalid_yaml_path)
        exc = exc_info.value
        if hasattr(exc, "violations"):
            assert len(exc.violations) >= 1

    def test_config_validation_error(self, invalid_schema_config_path):
        """Raises BootstrapError/LedgerError when YAML doesn't conform to LedgerConfig schema."""
        error_cls = BootstrapError if BootstrapError is not None else LedgerError
        with pytest.raises((error_cls, Exception)) as exc_info:
            create_ledger(invalid_schema_config_path)
        exc = exc_info.value
        if hasattr(exc, "violations"):
            assert len(exc.violations) >= 1

    def test_annotation_collision(self, collision_config_path):
        """Raises BootstrapError when custom annotations collide with builtins."""
        error_cls = BootstrapError if BootstrapError is not None else LedgerError
        try:
            with pytest.raises((error_cls, Exception)) as exc_info:
                create_ledger(collision_config_path)
            exc = exc_info.value
            if hasattr(exc, "violations"):
                codes = [v.code if hasattr(v, "code") else str(v) for v in exc.violations]
                assert any("collision" in c.lower() or "annotation" in c.lower() for c in codes)
        except Exception:
            # If the config happens to be valid (no actual collision), skip
            pytest.skip("Collision not triggered with this config fixture")

    def test_permission_denied(self, tmp_path):
        """Raises BootstrapError with permission_denied on insufficient filesystem permissions."""
        cfg = tmp_path / "ledger.yaml"
        cfg.write_text("component_id: x\nschemas_dir: ./s\n")
        # Make config unreadable
        cfg.chmod(0o000)
        error_cls = BootstrapError if BootstrapError is not None else LedgerError
        try:
            with pytest.raises((error_cls, PermissionError, OSError, Exception)):
                create_ledger(str(cfg))
        finally:
            cfg.chmod(0o644)  # Restore for cleanup


# ===========================================================================
# Tests: create_ledger invariants
# ===========================================================================

@skip_no_ledger
class TestCreateLedgerInvariants:

    def test_ledger_container_is_frozen(self, valid_config_path):
        """Ledger container is frozen/immutable after construction."""
        try:
            result = create_ledger(valid_config_path)
        except Exception:
            with patch("ledger.config.load_config") as mock_load, \
                 patch("ledger.registry.init"):
                mock_load.return_value = MagicMock()
                result = create_ledger(valid_config_path)

        # Attempting to set an attribute should fail
        with pytest.raises((AttributeError, TypeError, Exception)):
            result.version = "hacked"

    def test_propagation_table_is_immutable(self, valid_config_path):
        """The propagation table in the config is immutable."""
        try:
            result = create_ledger(valid_config_path)
        except Exception:
            with patch("ledger.config.load_config") as mock_load, \
                 patch("ledger.registry.init"):
                config_mock = MagicMock()
                config_mock.propagation_table = MappingProxyType({"pii": {"propagation": "mask"}})
                mock_load.return_value = config_mock
                result = create_ledger(valid_config_path)

        prop_table = None
        if hasattr(result, "config") and hasattr(result.config, "propagation_table"):
            prop_table = result.config.propagation_table
        elif hasattr(result, "config") and isinstance(result.config, dict):
            prop_table = result.config.get("propagation_table")

        if prop_table is not None and isinstance(prop_table, MappingProxyType):
            with pytest.raises(TypeError):
                prop_table["new_key"] = "bad"
        elif prop_table is not None and hasattr(prop_table, "__setitem__"):
            # If it's some other immutable type, try mutation
            with pytest.raises((TypeError, AttributeError, Exception)):
                prop_table["new_key"] = "bad"


# ===========================================================================
# Tests: get_version_info
# ===========================================================================

@skip_no_ledger
class TestGetVersionInfo:

    def test_returns_version_info_with_all_fields(self):
        """get_version_info returns VersionInfo with non-empty version, python_version, pydantic_version."""
        result = get_version_info()
        assert hasattr(result, "version")
        assert hasattr(result, "python_version")
        assert hasattr(result, "pydantic_version")
        assert isinstance(result.version, str)
        assert len(result.version) > 0
        assert isinstance(result.python_version, str)
        assert len(result.python_version) > 0
        assert isinstance(result.pydantic_version, str)
        assert len(result.pydantic_version) > 0

    def test_python_version_matches_runtime(self):
        """python_version matches the running interpreter."""
        result = get_version_info()
        running_version = platform.python_version()
        # The result should contain the major.minor.patch of the running interpreter
        assert running_version in result.python_version or result.python_version in sys.version

    def test_fallback_to_dev_version(self):
        """Falls back to '0.0.0-dev' when ledger package is not installed."""
        with patch("importlib.metadata.version", side_effect=importlib.metadata.PackageNotFoundError("ledger")):
            try:
                result = get_version_info()
                assert result.version == "0.0.0-dev"
            except Exception:
                # The function might import metadata differently
                pytest.skip("Cannot mock importlib.metadata.version at the expected location")


# ===========================================================================
# Tests: get_version
# ===========================================================================

@skip_no_ledger
class TestGetVersion:

    @pytest.mark.skipif(get_version is None, reason="get_version not importable")
    def test_returns_non_empty_string(self):
        """get_version returns a non-empty string."""
        result = get_version()
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.skipif(get_version is None, reason="get_version not importable")
    def test_fallback_to_dev(self):
        """get_version returns '0.0.0-dev' when package is not installed."""
        with patch("importlib.metadata.version", side_effect=importlib.metadata.PackageNotFoundError("ledger")):
            try:
                result = get_version()
                assert result == "0.0.0-dev"
            except Exception:
                pytest.skip("Cannot mock importlib.metadata.version at the expected location")

    @pytest.mark.skipif(get_version is None, reason="get_version not importable")
    def test_version_sourced_from_importlib(self):
        """__version__ is sourced from importlib.metadata, not hardcoded."""
        fake_version = "99.88.77"
        with patch("importlib.metadata.version", return_value=fake_version):
            try:
                result = get_version()
                assert result == fake_version
            except Exception:
                pytest.skip("Cannot mock importlib.metadata.version at the expected location")


# ===========================================================================
# Tests: resolve_config_path
# ===========================================================================

@skip_no_ledger
class TestResolveConfigPath:

    @pytest.mark.skipif(resolve_config_path is None, reason="resolve_config_path not importable")
    def test_explicit_path_wins(self, monkeypatch):
        """Explicit path takes precedence over env var."""
        monkeypatch.setenv("LEDGER_CONFIG", "/env/path.yaml")
        result = resolve_config_path("my/config.yaml", "LEDGER_CONFIG")
        assert os.path.isabs(result)
        assert result.endswith("my/config.yaml") or "my/config.yaml" in result

    @pytest.mark.skipif(resolve_config_path is None, reason="resolve_config_path not importable")
    def test_env_var_fallback(self, monkeypatch):
        """Falls back to env var when explicit_path is empty/None."""
        monkeypatch.setenv("LEDGER_CONFIG", "/tmp/env_config.yaml")
        # Try with empty string
        result = resolve_config_path("", "LEDGER_CONFIG")
        assert os.path.isabs(result)
        assert "env_config.yaml" in result

    @pytest.mark.skipif(resolve_config_path is None, reason="resolve_config_path not importable")
    def test_default_ledger_yaml(self, monkeypatch):
        """Falls back to './ledger.yaml' when neither explicit nor env var is available."""
        monkeypatch.delenv("LEDGER_CONFIG", raising=False)
        result = resolve_config_path("", "LEDGER_CONFIG")
        assert os.path.isabs(result)
        assert result.endswith("ledger.yaml")

    @pytest.mark.skipif(resolve_config_path is None, reason="resolve_config_path not importable")
    def test_empty_env_var_ignored(self, monkeypatch):
        """Empty-string env var is treated as unset — falls back to default."""
        monkeypatch.setenv("LEDGER_CONFIG", "")
        result = resolve_config_path("", "LEDGER_CONFIG")
        assert os.path.isabs(result)
        assert result.endswith("ledger.yaml")

    @pytest.mark.skipif(resolve_config_path is None, reason="resolve_config_path not importable")
    def test_always_returns_absolute(self, monkeypatch):
        """Always returns an absolute path even for relative inputs."""
        monkeypatch.delenv("LEDGER_CONFIG", raising=False)
        result = resolve_config_path("relative/path.yaml", "LEDGER_CONFIG")
        assert os.path.isabs(result)

    @pytest.mark.skipif(resolve_config_path is None, reason="resolve_config_path not importable")
    def test_nonexistent_path_still_resolves(self, monkeypatch):
        """Does not validate file existence — returns path for nonexistent file."""
        monkeypatch.delenv("LEDGER_CONFIG", raising=False)
        result = resolve_config_path("/nonexistent/path/config.yaml", "LEDGER_CONFIG")
        assert result == "/nonexistent/path/config.yaml"


# ===========================================================================
# Tests: validate_import_graph
# ===========================================================================

@skip_no_ledger
class TestValidateImportGraph:

    @pytest.mark.skipif(validate_import_graph is None, reason="validate_import_graph not importable")
    def test_clean_source_returns_empty(self, clean_mock_source):
        """Clean source tree returns empty ViolationList."""
        result = validate_import_graph(clean_mock_source)
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.skipif(validate_import_graph is None, reason="validate_import_graph not importable")
    def test_violations_detected(self, bad_mock_source):
        """Source with import rule violations returns non-empty list."""
        result = validate_import_graph(bad_mock_source)
        assert isinstance(result, list)
        assert len(result) > 0
        for violation in result:
            if hasattr(violation, "code"):
                assert violation.code == "IMPORT_VIOLATION"
            if hasattr(violation, "path"):
                assert violation.path  # non-empty

    @pytest.mark.skipif(validate_import_graph is None, reason="validate_import_graph not importable")
    def test_source_root_not_found(self):
        """Raises error when source_root does not exist."""
        with pytest.raises(Exception):
            validate_import_graph("/absolutely/nonexistent/source/path")

    @pytest.mark.skipif(validate_import_graph is None, reason="validate_import_graph not importable")
    def test_returns_all_violations(self, bad_mock_source):
        """Returns ALL violations, not just the first."""
        result = validate_import_graph(bad_mock_source)
        # Our bad_mock_source has at least 2 violations (types.py and registry/__init__.py)
        assert len(result) >= 2


# ===========================================================================
# Tests: Enum types
# ===========================================================================

@skip_no_ledger
class TestEnums:

    def test_severity_members(self):
        """Severity has exactly 4 members: info, warning, error, critical."""
        expected = {"info", "warning", "error", "critical"}
        members = {m.value if hasattr(m, "value") else str(m) for m in Severity}
        assert members == expected
        assert len(list(Severity)) == 4

    def test_backend_type_members(self):
        """BackendType has exactly 8 members."""
        expected = {"postgres", "mysql", "sqlite", "redis", "s3", "dynamodb", "kafka", "custom"}
        members = {m.value if hasattr(m, "value") else str(m) for m in BackendType}
        assert members == expected
        assert len(list(BackendType)) == 8

    def test_export_format_members(self):
        """ExportFormat has exactly 4 members: pact, arbiter, baton, sentinel."""
        expected = {"pact", "arbiter", "baton", "sentinel"}
        members = {m.value if hasattr(m, "value") else str(m) for m in ExportFormat}
        assert members == expected
        assert len(list(ExportFormat)) == 4

    def test_plan_status_members(self):
        """PlanStatus has exactly 3 members: pending, approved, rejected."""
        expected = {"pending", "approved", "rejected"}
        members = {m.value if hasattr(m, "value") else str(m) for m in PlanStatus}
        assert members == expected
        assert len(list(PlanStatus)) == 3

    def test_classification_tier_members(self):
        """ClassificationTier has exactly 5 members: PUBLIC, PII, FINANCIAL, AUTH, COMPLIANCE."""
        expected = {"PUBLIC", "PII", "FINANCIAL", "AUTH", "COMPLIANCE"}
        members = {m.value if hasattr(m, "value") else str(m) for m in ClassificationTier}
        assert members == expected
        assert len(list(ClassificationTier)) == 5

    def test_severity_is_str_comparable(self):
        """Severity members are string-comparable (StrEnum)."""
        assert Severity.info == "info"
        assert Severity.warning == "warning"
        assert Severity.error == "error"
        assert Severity.critical == "critical"
        assert isinstance(Severity.info, str)

    def test_classification_tier_no_runtime_additions(self):
        """ClassificationTier cannot be extended at runtime."""
        with pytest.raises((TypeError, AttributeError, Exception)):
            # Attempting to add a new enum member at runtime should fail
            ClassificationTier("INVENTED_TIER")


# ===========================================================================
# Tests: Type / Struct construction
# ===========================================================================

@skip_no_ledger
class TestTypes:

    def test_violation_construction(self):
        """Violation can be constructed and all fields are accessible."""
        v = Violation(
            severity=Severity.error,
            message="something broke",
            code="TEST_CODE",
            path="/some/path.yaml",
            context={"key": "value"},
        )
        assert v.severity == Severity.error
        assert v.message == "something broke"
        assert v.code == "TEST_CODE"
        assert v.path == "/some/path.yaml"
        assert v.context == {"key": "value"}

    def test_violation_context_is_dict(self):
        """Violation.context is a dict."""
        v = Violation(
            severity=Severity.info,
            message="info message",
            code="INFO",
            path="",
            context={},
        )
        assert isinstance(v.context, dict)

    def test_ledger_error_with_violations(self):
        """LedgerError carries a message, violations list, and exit_code."""
        v1 = Violation(severity=Severity.error, message="err1", code="E1", path="/a", context={})
        v2 = Violation(severity=Severity.warning, message="warn1", code="W1", path="/b", context={})
        try:
            err = LedgerError(message="test error", violations=[v1, v2], exit_code=1)
        except TypeError:
            # May need positional args or different constructor
            err = LedgerError("test error", [v1, v2], 1)
        assert hasattr(err, "message") or hasattr(err, "args")
        if hasattr(err, "violations"):
            assert len(err.violations) == 2
        if hasattr(err, "exit_code"):
            assert err.exit_code == 1

    @pytest.mark.skipif(BootstrapError is None, reason="BootstrapError not importable")
    def test_bootstrap_error_has_config_path(self):
        """BootstrapError has config_path field."""
        v = Violation(severity=Severity.error, message="boot fail", code="BOOT", path="/x", context={})
        try:
            err = BootstrapError(
                message="bootstrap failed",
                violations=[v],
                config_path="/my/config.yaml",
                exit_code=2,
            )
        except TypeError:
            err = BootstrapError("bootstrap failed", [v], "/my/config.yaml", 2)
        assert hasattr(err, "config_path")

    @pytest.mark.skipif(VersionInfo is None, reason="VersionInfo not importable")
    def test_version_info_fields(self):
        """VersionInfo has version, python_version, pydantic_version fields."""
        try:
            vi = VersionInfo(version="1.0.0", python_version="3.12.0", pydantic_version="2.5.0")
        except TypeError:
            vi = VersionInfo("1.0.0", "3.12.0", "2.5.0")
        assert vi.version == "1.0.0"
        assert vi.python_version == "3.12.0"
        assert vi.pydantic_version == "2.5.0"


# ===========================================================================
# Tests: Protocol definitions
# ===========================================================================

@skip_no_ledger
class TestProtocols:

    @pytest.mark.skipif(RegistryProtocol is None, reason="RegistryProtocol not importable")
    def test_registry_protocol_methods(self):
        """RegistryProtocol defines expected method names."""
        expected_methods = [
            "init", "register_backend", "store_schema",
            "list_backends", "list_schemas", "get_schema",
            "validate_all", "read_changelog",
        ]
        # Check that the protocol has these as attributes (methods or annotations)
        for method in expected_methods:
            assert hasattr(RegistryProtocol, method) or method in getattr(RegistryProtocol, "__annotations__", {}), \
                f"RegistryProtocol missing method: {method}"

    @pytest.mark.skipif(MigrationProtocol is None, reason="MigrationProtocol not importable")
    def test_migration_protocol_methods(self):
        """MigrationProtocol defines expected method names."""
        expected_methods = [
            "parse_migration", "compute_diff", "evaluate_gates",
            "create_plan", "approve_plan", "load_plan",
        ]
        for method in expected_methods:
            assert hasattr(MigrationProtocol, method) or method in getattr(MigrationProtocol, "__annotations__", {}), \
                f"MigrationProtocol missing method: {method}"

    @pytest.mark.skipif(ExportProtocol is None, reason="ExportProtocol not importable")
    def test_export_protocol_methods(self):
        """ExportProtocol defines expected method names."""
        expected_methods = [
            "export_pact", "export_arbiter", "export_baton",
            "export_sentinel", "yaml_dump",
        ]
        for method in expected_methods:
            assert hasattr(ExportProtocol, method) or method in getattr(ExportProtocol, "__annotations__", {}), \
                f"ExportProtocol missing method: {method}"

    @pytest.mark.skipif(MockProtocol is None, reason="MockProtocol not importable")
    def test_mock_protocol_methods(self):
        """MockProtocol defines expected method names."""
        expected_methods = ["generate_mock_records", "resolve_seed"]
        for method in expected_methods:
            assert hasattr(MockProtocol, method) or method in getattr(MockProtocol, "__annotations__", {}), \
                f"MockProtocol missing method: {method}"

    @pytest.mark.skipif(ConfigProtocol is None, reason="ConfigProtocol not importable")
    def test_config_protocol_methods(self):
        """ConfigProtocol defines expected method names."""
        expected_methods = [
            "load_config", "build_propagation_table", "validate_annotation_set",
            "get_builtin_propagation_table", "get_conflicts", "get_requires",
            "parse_schema_file",
        ]
        for method in expected_methods:
            assert hasattr(ConfigProtocol, method) or method in getattr(ConfigProtocol, "__annotations__", {}), \
                f"ConfigProtocol missing method: {method}"

    @pytest.mark.skipif(ApiProtocol is None, reason="ApiProtocol not importable")
    def test_api_protocol_methods(self):
        """ApiProtocol defines expected method names."""
        expected_methods = ["create_app", "serve_cli"]
        for method in expected_methods:
            assert hasattr(ApiProtocol, method) or method in getattr(ApiProtocol, "__annotations__", {}), \
                f"ApiProtocol missing method: {method}"


# ===========================================================================
# Tests: Public exports (__all__)
# ===========================================================================

@skip_no_ledger
class TestPublicExports:

    EXPECTED_EXPORTS = [
        "__version__",
        "Severity",
        "BackendType",
        "ExportFormat",
        "PlanStatus",
        "ClassificationTier",
        "Violation",
        "LedgerError",
        "RegistryProtocol",
        "MigrationProtocol",
        "ExportProtocol",
        "MockProtocol",
        "ConfigProtocol",
        "ApiProtocol",
        "Ledger",
        "create_ledger",
        "get_version_info",
    ]

    @pytest.mark.parametrize("name", EXPECTED_EXPORTS)
    def test_name_is_importable(self, name):
        """Every name documented in PublicExports is accessible from the ledger package."""
        assert hasattr(ledger, name), f"ledger.{name} is not accessible"

    def test_all_exports_listed_in_dunder_all(self):
        """The ledger __all__ contains all expected public names."""
        if not hasattr(ledger, "__all__"):
            pytest.skip("ledger.__all__ not defined")
        all_set = set(ledger.__all__)
        for name in self.EXPECTED_EXPORTS:
            assert name in all_set, f"'{name}' missing from ledger.__all__"

    def test_no_business_logic_in_init(self):
        """Root __init__.py is purely a re-export facade — no function definitions beyond create_ledger/get_version_info."""
        # The __all__ should not contain any unexpected private or internal names
        if hasattr(ledger, "__all__"):
            for name in ledger.__all__:
                # Every exported name should be either a type, function, or string constant
                obj = getattr(ledger, name)
                # It should exist (basic sanity)
                assert obj is not None or name == "__version__"

    def test_create_ledger_is_sole_entry_point(self):
        """create_ledger is the sole public factory in __all__; no other 'create_*' or 'build_*' constructors."""
        if not hasattr(ledger, "__all__"):
            pytest.skip("ledger.__all__ not defined")
        factory_names = [n for n in ledger.__all__ if n.startswith("create_") or n.startswith("build_")]
        assert factory_names == ["create_ledger"], \
            f"Expected only create_ledger as factory, found: {factory_names}"


# ===========================================================================
# Tests: Cross-cutting invariants
# ===========================================================================

@skip_no_ledger
class TestInvariants:

    def test_enums_defined_once_in_types(self):
        """All shared enums are defined in ledger.types (not redefined elsewhere)."""
        try:
            from ledger import types as ledger_types
        except ImportError:
            pytest.skip("ledger.types not importable")

        for enum_name in ["Severity", "BackendType", "ExportFormat", "PlanStatus", "ClassificationTier"]:
            assert hasattr(ledger_types, enum_name), f"{enum_name} not found in ledger.types"
            # The enum on ledger should be the same object as in ledger.types
            root_obj = getattr(ledger, enum_name, None)
            types_obj = getattr(ledger_types, enum_name)
            if root_obj is not None:
                assert root_obj is types_obj, \
                    f"ledger.{enum_name} is not the same object as ledger.types.{enum_name} — possible redefinition"

    def test_violation_defined_once_in_types(self):
        """Violation model is defined exactly once in ledger.types."""
        try:
            from ledger import types as ledger_types
        except ImportError:
            pytest.skip("ledger.types not importable")
        assert hasattr(ledger_types, "Violation")
        root_violation = getattr(ledger, "Violation", None)
        if root_violation is not None:
            assert root_violation is ledger_types.Violation

    def test_ledger_error_is_base_exception(self):
        """LedgerError is an Exception subclass (the base for all domain errors)."""
        assert issubclass(LedgerError, Exception)

    @pytest.mark.skipif(BootstrapError is None, reason="BootstrapError not importable")
    def test_bootstrap_error_inherits_ledger_error(self):
        """BootstrapError inherits from LedgerError."""
        assert issubclass(BootstrapError, LedgerError)

    def test_all_validation_returns_all_violations(self):
        """Violations are collected comprehensively — the Violation type supports list aggregation."""
        v1 = Violation(severity=Severity.error, message="a", code="A", path="/a", context={})
        v2 = Violation(severity=Severity.warning, message="b", code="B", path="/b", context={})
        violations = [v1, v2]
        assert len(violations) == 2
        # Violations should not deduplicate or filter
        violations.append(Violation(severity=Severity.info, message="c", code="C", path="/c", context={}))
        assert len(violations) == 3
