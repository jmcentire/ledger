"""Tests for the retention export format."""

import os
import pytest

from export import (
    RetentionFieldRule,
    RetentionTableRule,
    RetentionExport,
    export_retention,
    export_retention_from_config,
)


# ===========================================================================
#  export_retention
# ===========================================================================


class TestExportRetention:
    """Tests for the export_retention function."""

    def test_empty_schemas_returns_empty(self):
        result = export_retention([])
        assert result.retention_rules == []

    def test_schema_without_retention_annotations_skipped(self):
        schemas = [
            {
                "backend_id": "db1",
                "table_name": "users",
                "fields": [
                    {"name": "id", "annotations": ["primary_key", "immutable"]},
                    {"name": "name", "annotations": ["pii_field"]},
                ],
            }
        ]
        result = export_retention(schemas)
        assert result.retention_rules == []

    def test_gdpr_erasable_generates_retention_rule(self):
        schemas = [
            {
                "backend_id": "db1",
                "table_name": "users",
                "fields": [
                    {"name": "email", "annotations": ["gdpr_erasable", "pii_field"]},
                ],
            }
        ]
        result = export_retention(schemas)
        assert len(result.retention_rules) == 1
        table_rule = result.retention_rules[0]
        assert table_rule.backend_id == "db1"
        assert table_rule.table_name == "users"
        assert len(table_rule.field_rules) == 1
        field_rule = table_rule.field_rules[0]
        assert field_rule.field_name == "email"
        assert field_rule.annotation == "gdpr_erasable"
        assert field_rule.retention_days == 90
        assert field_rule.erasure_method == "hard_delete_or_anonymize"

    def test_audit_field_generates_long_retention(self):
        schemas = [
            {
                "backend_id": "db1",
                "table_name": "events",
                "fields": [
                    {"name": "created_at", "annotations": ["audit_field"]},
                ],
            }
        ]
        result = export_retention(schemas)
        assert len(result.retention_rules) == 1
        field_rule = result.retention_rules[0].field_rules[0]
        assert field_rule.annotation == "audit_field"
        assert field_rule.retention_days == 2555  # ~7 years
        assert "archive" in field_rule.erasure_method

    def test_soft_delete_marker_generates_rule(self):
        schemas = [
            {
                "backend_id": "db1",
                "table_name": "users",
                "fields": [
                    {"name": "deleted_at", "annotations": ["soft_delete_marker"]},
                ],
            }
        ]
        result = export_retention(schemas)
        assert len(result.retention_rules) == 1
        field_rule = result.retention_rules[0].field_rules[0]
        assert field_rule.annotation == "soft_delete_marker"
        assert field_rule.retention_days == 30

    def test_multiple_annotations_on_same_field(self):
        schemas = [
            {
                "backend_id": "db1",
                "table_name": "users",
                "fields": [
                    {
                        "name": "deleted_at",
                        "annotations": ["soft_delete_marker", "audit_field"],
                    },
                ],
            }
        ]
        result = export_retention(schemas)
        assert len(result.retention_rules) == 1
        # Should have two rules for the same field
        assert len(result.retention_rules[0].field_rules) == 2
        annotations = {r.annotation for r in result.retention_rules[0].field_rules}
        assert annotations == {"soft_delete_marker", "audit_field"}

    def test_multiple_tables_from_same_backend(self):
        schemas = [
            {
                "backend_id": "db1",
                "table_name": "users",
                "fields": [
                    {"name": "email", "annotations": ["gdpr_erasable"]},
                ],
            },
            {
                "backend_id": "db1",
                "table_name": "orders",
                "fields": [
                    {"name": "created_at", "annotations": ["audit_field"]},
                ],
            },
        ]
        result = export_retention(schemas)
        assert len(result.retention_rules) == 2

    def test_non_list_annotations_skipped(self):
        schemas = [
            {
                "backend_id": "db1",
                "table_name": "users",
                "fields": [
                    {"name": "email", "annotations": "gdpr_erasable"},  # string, not list
                ],
            }
        ]
        result = export_retention(schemas)
        assert result.retention_rules == []

    def test_default_backend_id_and_table_name(self):
        schemas = [
            {
                "fields": [
                    {"name": "email", "annotations": ["gdpr_erasable"]},
                ],
            }
        ]
        result = export_retention(schemas)
        assert len(result.retention_rules) == 1
        assert result.retention_rules[0].backend_id == "unknown"
        assert result.retention_rules[0].table_name == "unknown"


# ===========================================================================
#  RetentionExport model
# ===========================================================================


class TestRetentionModels:
    """Tests for retention data models."""

    def test_retention_field_rule_creation(self):
        rule = RetentionFieldRule(
            field_name="email",
            annotation="gdpr_erasable",
            retention_days=90,
            erasure_method="hard_delete",
        )
        assert rule.field_name == "email"
        assert rule.retention_days == 90

    def test_retention_table_rule_creation(self):
        field_rule = RetentionFieldRule(
            field_name="email",
            annotation="gdpr_erasable",
            retention_days=90,
        )
        table_rule = RetentionTableRule(
            backend_id="db1",
            table_name="users",
            field_rules=[field_rule],
        )
        assert table_rule.backend_id == "db1"
        assert len(table_rule.field_rules) == 1

    def test_retention_export_serializes(self):
        export = RetentionExport(retention_rules=[])
        data = export.model_dump()
        assert "retention_rules" in data
        assert data["retention_rules"] == []

    def test_retention_export_full_roundtrip(self):
        export = RetentionExport(
            retention_rules=[
                RetentionTableRule(
                    backend_id="db1",
                    table_name="users",
                    field_rules=[
                        RetentionFieldRule(
                            field_name="email",
                            annotation="gdpr_erasable",
                            retention_days=90,
                            erasure_method="hard_delete_or_anonymize",
                            notes="GDPR compliance",
                        ),
                    ],
                ),
            ]
        )
        data = export.model_dump(exclude_none=True)
        assert data["retention_rules"][0]["backend_id"] == "db1"
        assert data["retention_rules"][0]["field_rules"][0]["retention_days"] == 90


# ===========================================================================
#  export_retention_from_config
# ===========================================================================


class TestExportRetentionFromConfig:
    """Tests for the config-aware retention export wrapper."""

    def test_with_schemas_dir(self, tmp_path):
        """Test that it reads YAML files from schemas_dir."""
        import yaml

        sd = tmp_path / "schemas"
        sd.mkdir()

        schema_data = {
            "backend_id": "users_db",
            "table_name": "users",
            "fields": [
                {"name": "email", "annotations": ["gdpr_erasable", "pii_field"]},
                {"name": "created_at", "annotations": ["audit_field", "immutable"]},
            ],
        }
        (sd / "users.yaml").write_text(yaml.dump(schema_data))

        sd_str = str(sd)

        class FakeConfig:
            schemas_dir = sd_str

        result = export_retention_from_config(FakeConfig())
        assert "retention_rules" in result
        assert len(result["retention_rules"]) == 1
        rules = result["retention_rules"][0]
        assert rules["backend_id"] == "users_db"
        assert len(rules["field_rules"]) == 2

    def test_with_nonexistent_schemas_dir(self):
        """Should return empty when schemas_dir doesn't exist."""
        class FakeConfig:
            schemas_dir = "/nonexistent/path"

        result = export_retention_from_config(FakeConfig())
        assert result["retention_rules"] == []

    def test_with_no_schemas_dir_attr(self):
        """Should return empty when config has no schemas_dir."""
        class FakeConfig:
            pass

        result = export_retention_from_config(FakeConfig())
        assert result["retention_rules"] == []

    def test_component_filter(self, tmp_path):
        """Test filtering by component."""
        import yaml

        sd = tmp_path / "schemas"
        sd.mkdir()

        (sd / "users.yaml").write_text(yaml.dump({
            "backend_id": "users_db",
            "table_name": "users",
            "owner": "user_service",
            "fields": [
                {"name": "email", "annotations": ["gdpr_erasable"]},
            ],
        }))
        (sd / "orders.yaml").write_text(yaml.dump({
            "backend_id": "orders_db",
            "table_name": "orders",
            "owner": "order_service",
            "fields": [
                {"name": "created_at", "annotations": ["audit_field"]},
            ],
        }))

        sd_str = str(sd)

        class FakeConfig:
            schemas_dir = sd_str

        result = export_retention_from_config(FakeConfig(), component="user_service")
        assert len(result["retention_rules"]) == 1
        assert result["retention_rules"][0]["backend_id"] == "users_db"
