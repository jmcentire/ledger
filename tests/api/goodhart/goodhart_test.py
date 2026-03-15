"""
Adversarial hidden acceptance tests for the HTTP API Server component.
These tests catch implementations that pass visible tests through shortcuts
(hardcoded returns, incomplete validation, etc.) rather than truly satisfying the contract.
"""

import pytest
import time
import uuid
import json
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from src.api import *


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(**overrides):
    """Create a LedgerConfig with sensible defaults, overridable per-test."""
    import tempfile, os
    defaults = {
        "port": 8080,
        "schema_dir": overrides.pop("schema_dir", tempfile.mkdtemp()),
        "plan_ttl_seconds": 3600,
        "arbiter_url": "",
    }
    defaults.update(overrides)
    return LedgerConfig(**defaults)


def make_app(**config_overrides):
    """Create a FastAPI app with a test client."""
    config = make_config(**config_overrides)
    app = create_app(config)
    client = TestClient(app)
    return app, client, config


def register_backend(client, backend_id="test-be", display_name="Test Backend", description="A test backend"):
    resp = client.post("/backends", json={
        "backend_id": backend_id,
        "display_name": display_name,
        "description": description,
    })
    return resp


SIMPLE_SCHEMA_YAML = """
table:
  name: users
  columns:
    - name: id
      data_type: integer
      nullable: false
      primary_key: true
    - name: email
      data_type: varchar
      nullable: false
      primary_key: false
""".strip()

SIMPLE_SCHEMA_YAML_V2 = """
table:
  name: users
  columns:
    - name: id
      data_type: integer
      nullable: false
      primary_key: true
    - name: email
      data_type: varchar
      nullable: false
      primary_key: false
    - name: age
      data_type: integer
      nullable: true
      primary_key: false
""".strip()


def register_schema(client, backend_id="test-be", table_name="users", yaml_content=None, version="1.0.0"):
    if yaml_content is None:
        yaml_content = SIMPLE_SCHEMA_YAML
    resp = client.post("/schemas", json={
        "backend_id": backend_id,
        "table_name": table_name,
        "yaml_content": yaml_content,
        "version": version,
    })
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGoodhartHealth:
    def test_goodhart_health_response_fields_correct_types(self):
        """Health response must contain all three fields (status, version, port) with correct types for any valid config."""
        _, client, _ = make_app(port=9999)
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert isinstance(body["version"], str) and len(body["version"]) > 0
        assert isinstance(body["port"], int)
        assert body["port"] == 9999

    def test_goodhart_health_port_reflects_config(self):
        """Health endpoint must dynamically reflect the configured port, not return a hardcoded value."""
        _, client1, _ = make_app(port=7542)
        _, client2, _ = make_app(port=3333)
        resp1 = client1.get("/health")
        resp2 = client2.get("/health")
        assert resp1.json()["port"] == 7542
        assert resp2.json()["port"] == 3333

    def test_goodhart_health_no_registry_field_access(self):
        """Health endpoint must work correctly as the very first request to a fresh app."""
        _, client, _ = make_app()
        # This is the absolute first request - no setup
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestGoodhartBackendRegistration:
    def test_goodhart_backend_conflict_different_description(self):
        """Backend conflict detection must check description field too, not just display_name."""
        _, client, _ = make_app()
        r1 = register_backend(client, "be-1", "Same Name", "Original desc")
        assert r1.status_code == 201

        r2 = client.post("/backends", json={
            "backend_id": "be-1",
            "display_name": "Same Name",
            "description": "Changed desc",
        })
        assert r2.status_code == 409
        body = r2.json()
        assert "violations" in body
        assert isinstance(body["violations"], list)

    def test_goodhart_backend_identical_rereg_returns_created_false(self):
        """Identical backend re-registration must return 200 with created=false."""
        _, client, _ = make_app()
        register_backend(client, "be-2", "My Backend", "Desc")
        r2 = register_backend(client, "be-2", "My Backend", "Desc")
        assert r2.status_code == 200
        assert r2.json()["created"] is False

    def test_goodhart_backend_201_created_true(self):
        """New backend registration must return created=true in the response body."""
        _, client, _ = make_app()
        r = register_backend(client, "fresh-be", "Fresh", "Desc")
        assert r.status_code == 201
        body = r.json()
        assert body["created"] is True
        assert body["backend_id"] == "fresh-be"
        assert body["display_name"] == "Fresh"

    def test_goodhart_backend_location_header_format(self):
        """Backend registration Location header must follow /backends/{backend_id} format exactly."""
        _, client, _ = make_app()
        r = register_backend(client, "test-backend-xyz", "Display", "Desc")
        assert r.status_code == 201
        location = r.headers.get("location", "")
        assert location == "/backends/test-backend-xyz"

    def test_goodhart_backend_409_response_has_violations(self):
        """Backend conflict (409) response must include a non-empty violations list."""
        _, client, _ = make_app()
        register_backend(client, "be-conflict", "Name1", "Desc1")
        r = register_backend(client, "be-conflict", "Name2", "Desc1")
        assert r.status_code == 409
        body = r.json()
        assert "violations" in body
        assert isinstance(body["violations"], list)
        assert len(body["violations"]) > 0

    def test_goodhart_error_response_violations_is_list(self):
        """All error responses must include violations as a list, not a single object or string."""
        _, client, _ = make_app()
        register_backend(client, "err-be", "Original", "Desc")
        r = register_backend(client, "err-be", "Different", "Desc")
        assert r.status_code == 409
        body = r.json()
        assert isinstance(body.get("violations"), list)
        for v in body["violations"]:
            assert "field" in v or "message" in v or "severity" in v or "code" in v


class TestGoodhartSchemaRegistration:
    def test_goodhart_schema_location_header_format(self):
        """Schema registration Location header must follow /schemas/{backend_id}/{table_name} format."""
        _, client, _ = make_app()
        register_backend(client, "my-backend", "My Backend", "Desc")
        r = register_schema(client, backend_id="my-backend", table_name="users_table")
        assert r.status_code == 201
        location = r.headers.get("location", "")
        assert location == "/schemas/my-backend/users_table"

    def test_goodhart_schema_verbatim_preserves_trailing_whitespace(self):
        """Schema YAML storage must preserve trailing whitespace and unusual spacing."""
        _, client, _ = make_app()
        register_backend(client, "wb-be", "WB", "Desc")

        weird_yaml = "table:  \n  name: test_table  \n  columns:  \n    - name: id  \n      data_type: integer  \n      nullable: false  \n      primary_key: true  \n"
        r = register_schema(client, backend_id="wb-be", table_name="test_table", yaml_content=weird_yaml)
        assert r.status_code == 201

        detail = client.get("/schemas/wb-be/test_table")
        assert detail.status_code == 200
        assert detail.json()["yaml_content"] == weird_yaml

    def test_goodhart_schema_verbatim_preserves_key_order(self):
        """Schema YAML storage must not reorder YAML keys."""
        _, client, _ = make_app()
        register_backend(client, "order-be", "Order", "Desc")

        ordered_yaml = "table:\n  name: ordered_tbl\n  columns:\n    - name: z_col\n      data_type: varchar\n      nullable: true\n      primary_key: false\n    - name: a_col\n      data_type: integer\n      nullable: false\n      primary_key: true\n"
        r = register_schema(client, backend_id="order-be", table_name="ordered_tbl", yaml_content=ordered_yaml)
        assert r.status_code == 201

        detail = client.get("/schemas/order-be/ordered_tbl")
        assert detail.status_code == 200
        assert detail.json()["yaml_content"] == ordered_yaml

    def test_goodhart_schema_registration_created_field(self):
        """Schema registration response must have created=true for new and created=false for re-registration."""
        _, client, _ = make_app()
        register_backend(client, "cr-be", "CR", "Desc")

        r1 = register_schema(client, backend_id="cr-be", table_name="users", version="1.0.0")
        assert r1.status_code == 201
        assert r1.json()["created"] is True

        r2 = register_schema(client, backend_id="cr-be", table_name="users", version="1.0.0")
        assert r2.status_code == 200
        assert r2.json()["created"] is False

    def test_goodhart_conflict_409_has_error_response_model(self):
        """409 Conflict responses must use the ErrorResponse model."""
        _, client, _ = make_app()
        register_backend(client, "cf-be", "CF", "Desc")
        register_schema(client, backend_id="cf-be", table_name="users", yaml_content=SIMPLE_SCHEMA_YAML, version="1.0.0")

        r = register_schema(client, backend_id="cf-be", table_name="users", yaml_content=SIMPLE_SCHEMA_YAML_V2, version="1.0.0")
        assert r.status_code == 409
        body = r.json()
        assert "error" in body
        assert "detail" in body
        assert "violations" in body
        assert isinstance(body["violations"], list)


class TestGoodhartSchemaRetrieval:
    def test_goodhart_schema_list_multiple_schemas(self):
        """Schema list endpoint must return all registered schemas, not just one."""
        _, client, _ = make_app()
        register_backend(client, "multi-be", "Multi", "Desc")

        tables = ["alpha", "beta", "gamma"]
        for t in tables:
            yaml_c = f"table:\n  name: {t}\n  columns:\n    - name: id\n      data_type: integer\n      nullable: false\n      primary_key: true\n"
            register_schema(client, backend_id="multi-be", table_name=t, yaml_content=yaml_c)

        resp = client.get("/schemas/multi-be")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["schemas"]) == 3
        returned_names = {s["table_name"] for s in body["schemas"]}
        assert returned_names == set(tables)

    def test_goodhart_schema_list_summary_fields(self):
        """Schema list entries must include summary fields, not full YAML."""
        _, client, _ = make_app()
        register_backend(client, "summ-be", "Summ", "Desc")
        register_schema(client, backend_id="summ-be")

        resp = client.get("/schemas/summ-be")
        assert resp.status_code == 200
        for s in resp.json()["schemas"]:
            assert "table_name" in s
            assert "version" in s
            assert "column_count" in s
            assert "annotation_count" in s

    def test_goodhart_schema_detail_backend_not_found(self):
        """Schema detail endpoint must return 404 for non-existent backend."""
        _, client, _ = make_app()
        resp = client.get("/schemas/nonexistent-backend/some-table")
        assert resp.status_code == 404


class TestGoodhartSchemaValidation:
    def test_goodhart_validate_schema_info_severity_still_valid(self):
        """Schema validation with only info-severity violations should return valid=true."""
        _, client, _ = make_app()
        # This depends on the implementation producing info-level violations.
        # We test the contract: valid=true iff no error-severity violations.
        # Use a schema that is structurally valid but may have info notes.
        resp = client.post("/schemas/validate", json={"yaml_content": SIMPLE_SCHEMA_YAML})
        assert resp.status_code == 200
        body = resp.json()
        # If there are violations, none should be 'error' if valid is true
        if body["valid"]:
            for v in body.get("violations", []):
                assert v.get("severity") != "error"

    def test_goodhart_validate_schema_does_not_affect_registry(self):
        """Validating a schema must not make it appear in registry queries or exports."""
        _, client, _ = make_app()
        register_backend(client, "val-be", "Val", "Desc")

        # Validate without registering
        client.post("/schemas/validate", json={"yaml_content": SIMPLE_SCHEMA_YAML})

        # Schemas list should be empty
        resp = client.get("/schemas/val-be")
        assert resp.status_code == 200
        assert len(resp.json()["schemas"]) == 0


class TestGoodhartMigrationPlan:
    def test_goodhart_migration_plan_unique_ids(self):
        """Each migration plan must receive a unique UUID4."""
        _, client, _ = make_app()
        register_backend(client, "mig-be", "Mig", "Desc")
        register_schema(client, backend_id="mig-be")

        r1 = client.post("/migrations/plan", json={
            "backend_id": "mig-be",
            "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        r2 = client.post("/migrations/plan", json={
            "backend_id": "mig-be",
            "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN name VARCHAR;",
        })
        assert r1.status_code == 201
        assert r2.status_code == 201

        id1 = r1.json()["plan_id"]
        id2 = r2.json()["plan_id"]
        assert id1 != id2

        # Both must be valid UUID4
        u1 = uuid.UUID(id1, version=4)
        u2 = uuid.UUID(id2, version=4)
        assert str(u1) == id1
        assert str(u2) == id2

    def test_goodhart_migration_plan_expires_at_future(self):
        """Migration plan expires_at must be a future timestamp based on config TTL."""
        _, client, _ = make_app(plan_ttl_seconds=600)
        register_backend(client, "ttl-be", "TTL", "Desc")
        register_schema(client, backend_id="ttl-be")

        before = datetime.now(timezone.utc)
        r = client.post("/migrations/plan", json={
            "backend_id": "ttl-be",
            "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        assert r.status_code == 201
        body = r.json()
        expires_at = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00"))
        # expires_at should be at least ~590 seconds in the future
        assert expires_at > before
        diff = (expires_at - before).total_seconds()
        assert 550 < diff < 700  # roughly 600 seconds ± tolerance

    def test_goodhart_migration_plan_stores_backend_and_table(self):
        """Migration plan response must echo back correct backend_id and table_name."""
        _, client, _ = make_app()
        register_backend(client, "alpha-db", "Alpha DB", "Desc")
        yaml_c = "table:\n  name: orders\n  columns:\n    - name: id\n      data_type: integer\n      nullable: false\n      primary_key: true\n"
        register_schema(client, backend_id="alpha-db", table_name="orders", yaml_content=yaml_c)

        r = client.post("/migrations/plan", json={
            "backend_id": "alpha-db",
            "table_name": "orders",
            "sql_content": "ALTER TABLE orders ADD COLUMN status VARCHAR;",
        })
        assert r.status_code == 201
        body = r.json()
        assert body["backend_id"] == "alpha-db"
        assert body["table_name"] == "orders"

    def test_goodhart_migration_plan_pending_status_value(self):
        """New migration plan status must be literally 'pending'."""
        _, client, _ = make_app()
        register_backend(client, "ps-be", "PS", "Desc")
        register_schema(client, backend_id="ps-be")

        r = client.post("/migrations/plan", json={
            "backend_id": "ps-be",
            "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        assert r.status_code == 201
        assert r.json()["status"] == "pending"

    def test_goodhart_multiple_plans_coexist(self):
        """Multiple migration plans must coexist independently in memory."""
        _, client, _ = make_app()
        register_backend(client, "co-be", "Co", "Desc")
        register_schema(client, backend_id="co-be")

        r1 = client.post("/migrations/plan", json={
            "backend_id": "co-be", "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN a INTEGER;",
        })
        r2 = client.post("/migrations/plan", json={
            "backend_id": "co-be", "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN b VARCHAR;",
        })
        assert r1.status_code == 201
        assert r2.status_code == 201
        assert r1.json()["plan_id"] != r2.json()["plan_id"]


class TestGoodhartApproval:
    def test_goodhart_approve_changes_status_to_approved(self):
        """After approval, the plan status must be 'approved'."""
        _, client, _ = make_app()
        register_backend(client, "appr-be", "Appr", "Desc")
        register_schema(client, backend_id="appr-be")

        plan = client.post("/migrations/plan", json={
            "backend_id": "appr-be", "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        assert plan.status_code == 201
        plan_body = plan.json()
        plan_id = plan_body["plan_id"]

        if plan_body["gate_result"]["passed"]:
            r = client.post(f"/migrations/{plan_id}/approve")
            assert r.status_code == 200
            body = r.json()
            assert body["status"] == "approved"
            assert body["plan_id"] == plan_id

    def test_goodhart_approve_double_approve_rejected(self):
        """Approving an already-approved plan must fail."""
        _, client, _ = make_app()
        register_backend(client, "dbl-be", "Dbl", "Desc")
        register_schema(client, backend_id="dbl-be")

        plan = client.post("/migrations/plan", json={
            "backend_id": "dbl-be", "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        plan_body = plan.json()
        plan_id = plan_body["plan_id"]

        if plan_body["gate_result"]["passed"]:
            r1 = client.post(f"/migrations/{plan_id}/approve")
            assert r1.status_code == 200

            r2 = client.post(f"/migrations/{plan_id}/approve")
            assert r2.status_code not in (200, 201)

    def test_goodhart_expired_plan_not_approvable(self):
        """Expired plans must be treated as non-existent during approval."""
        _, client, _ = make_app(plan_ttl_seconds=1)
        register_backend(client, "exp-be", "Exp", "Desc")
        register_schema(client, backend_id="exp-be")

        plan = client.post("/migrations/plan", json={
            "backend_id": "exp-be", "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        plan_id = plan.json()["plan_id"]

        time.sleep(2)

        r = client.post(f"/migrations/{plan_id}/approve")
        assert r.status_code == 404


class TestGoodhartExport:
    def test_goodhart_export_json_contains_registered_schemas(self):
        """JSON export must include data from actually registered schemas."""
        _, client, _ = make_app()
        register_backend(client, "exp-be1", "Exp1", "Desc1")
        register_backend(client, "exp-be2", "Exp2", "Desc2")
        register_schema(client, backend_id="exp-be1", table_name="tbl1")

        yaml2 = "table:\n  name: tbl2\n  columns:\n    - name: id\n      data_type: integer\n      nullable: false\n      primary_key: true\n"
        register_schema(client, backend_id="exp-be2", table_name="tbl2", yaml_content=yaml2)

        resp = client.get("/export/json")
        assert resp.status_code == 200
        # The content should reference the registered schemas
        content = resp.text
        assert "exp-be1" in content or "tbl1" in content
        assert "exp-be2" in content or "tbl2" in content

    def test_goodhart_export_csv_contains_schema_data(self):
        """CSV export must contain actual schema information."""
        _, client, _ = make_app()
        register_backend(client, "csv-be", "CSV", "Desc")
        register_schema(client, backend_id="csv-be", table_name="users")

        resp = client.get("/export/csv")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers.get("content-type", "")
        assert "csv-be" in resp.text or "users" in resp.text

    def test_goodhart_export_no_state_mutation(self):
        """Export endpoint must not mutate any state."""
        _, client, _ = make_app()
        register_backend(client, "mut-be", "Mut", "Desc")
        register_schema(client, backend_id="mut-be")

        before = client.get("/schemas/mut-be").json()
        client.get("/export/json")
        client.get("/export/csv")
        client.get("/export/yaml")
        after = client.get("/schemas/mut-be").json()

        assert len(before["schemas"]) == len(after["schemas"])

    def test_goodhart_export_schema_count_accurate(self):
        """Export response schema_count must reflect actual registered schemas count."""
        _, client, _ = make_app()

        # With no schemas, export JSON and check
        resp0 = client.get("/export/json")
        assert resp0.status_code == 200
        # schema_count should be 0 (either in JSON body or in wrapper)
        body0 = resp0.json()
        if "schema_count" in body0:
            assert body0["schema_count"] == 0

        register_backend(client, "sc-be1", "SC1", "Desc")
        register_backend(client, "sc-be2", "SC2", "Desc")
        register_schema(client, backend_id="sc-be1", table_name="t1")
        yaml2 = "table:\n  name: t2\n  columns:\n    - name: id\n      data_type: integer\n      nullable: false\n      primary_key: true\n"
        register_schema(client, backend_id="sc-be2", table_name="t2", yaml_content=yaml2)

        resp2 = client.get("/export/json")
        body2 = resp2.json()
        if "schema_count" in body2:
            assert body2["schema_count"] == 2


class TestGoodhartMock:
    def test_goodhart_mock_row_count_exact_boundary_10000(self):
        """Mock generation must support exactly 10000 rows (upper boundary)."""
        _, client, _ = make_app()
        register_backend(client, "bnd-be", "Bnd", "Desc")
        register_schema(client, backend_id="bnd-be")

        resp = client.post("/mock/bnd-be/users", json={"row_count": 10000, "seed": 1})
        assert resp.status_code == 200
        body = resp.json()
        assert body["row_count"] == 10000
        assert len(body["rows"]) == 10000

    def test_goodhart_mock_row_count_10001_rejected(self):
        """Mock generation must reject row_count=10001."""
        _, client, _ = make_app()
        register_backend(client, "rej-be", "Rej", "Desc")
        register_schema(client, backend_id="rej-be")

        resp = client.post("/mock/rej-be/users", json={"row_count": 10001, "seed": 1})
        assert resp.status_code == 422

    def test_goodhart_mock_negative_row_count_rejected(self):
        """Mock generation must reject negative row_count values."""
        _, client, _ = make_app()
        register_backend(client, "neg-be", "Neg", "Desc")
        register_schema(client, backend_id="neg-be")

        resp = client.post("/mock/neg-be/users", json={"row_count": -1, "seed": 1})
        assert resp.status_code == 422

    def test_goodhart_mock_different_seeds_different_output(self):
        """Different seed values must produce different mock data."""
        _, client, _ = make_app()
        register_backend(client, "seed-be", "Seed", "Desc")
        register_schema(client, backend_id="seed-be")

        r1 = client.post("/mock/seed-be/users", json={"row_count": 5, "seed": 1})
        r2 = client.post("/mock/seed-be/users", json={"row_count": 5, "seed": 2})
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r1.json()["rows"] != r2.json()["rows"]

    def test_goodhart_mock_columns_match_schema(self):
        """Mock generation response columns must match the registered schema's column definitions."""
        _, client, _ = make_app()
        register_backend(client, "col-be", "Col", "Desc")
        register_schema(client, backend_id="col-be")

        resp = client.post("/mock/col-be/users", json={"row_count": 3, "seed": 42})
        assert resp.status_code == 200
        body = resp.json()
        columns = body["columns"]
        assert len(columns) >= 2  # at least id and email from our schema
        for row in body["rows"]:
            assert len(row) == len(columns)

    def test_goodhart_mock_seed_echoed_in_response(self):
        """Mock generation response must echo back the seed value from the request."""
        _, client, _ = make_app()
        register_backend(client, "echo-be", "Echo", "Desc")
        register_schema(client, backend_id="echo-be")

        resp = client.post("/mock/echo-be/users", json={"row_count": 1, "seed": 42})
        assert resp.status_code == 200
        assert resp.json()["seed"] == 42


class TestGoodhartAnnotations:
    def test_goodhart_annotations_empty_when_no_schemas(self):
        """Annotations endpoint must return empty list and total_count=0 when no schemas registered."""
        _, client, _ = make_app()
        resp = client.get("/annotations")
        assert resp.status_code == 200
        body = resp.json()
        assert body["annotations"] == []
        assert body["total_count"] == 0


class TestGoodhartCreateApp:
    def test_goodhart_create_app_config_stored_in_state(self):
        """App state must contain the config reference so dependencies can access it."""
        config = make_config(port=4444)
        app = create_app(config)
        # Config should be accessible from app state
        assert hasattr(app.state, "config") or hasattr(app, "state")
        # Verify via health endpoint that config is used
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.json()["port"] == 4444

    def test_goodhart_create_app_all_routes_exist(self):
        """App must have routes for all 7 router groups with expected path patterns."""
        _, client, _ = make_app()
        # Check that key routes exist by making requests and getting non-404/405 or expected responses
        # Health
        assert client.get("/health").status_code == 200
        # Backends - POST should work (or 422 for empty body, but not 404)
        r = client.post("/backends", json={})
        assert r.status_code != 404
        # Schemas validate
        r = client.post("/schemas/validate", json={})
        assert r.status_code != 404
        # Export
        r = client.get("/export/json")
        assert r.status_code != 404
        # Annotations
        r = client.get("/annotations")
        assert r.status_code != 404
        # Migrations plan
        r = client.post("/migrations/plan", json={})
        assert r.status_code != 404


class TestGoodhartMigrationGateViolations:
    def test_goodhart_migration_gate_result_structure(self):
        """Gate result must have 'passed' boolean and 'violations' list structure."""
        _, client, _ = make_app()
        register_backend(client, "gate-be", "Gate", "Desc")
        register_schema(client, backend_id="gate-be")

        r = client.post("/migrations/plan", json={
            "backend_id": "gate-be",
            "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        assert r.status_code == 201
        body = r.json()
        gate = body["gate_result"]
        assert isinstance(gate["passed"], bool)
        assert isinstance(gate["violations"], list)

    def test_goodhart_migration_plan_diffs_is_list(self):
        """Migration plan diffs must be a list of diff entries."""
        _, client, _ = make_app()
        register_backend(client, "diff-be", "Diff", "Desc")
        register_schema(client, backend_id="diff-be")

        r = client.post("/migrations/plan", json={
            "backend_id": "diff-be",
            "table_name": "users",
            "sql_content": "ALTER TABLE users ADD COLUMN age INTEGER;",
        })
        assert r.status_code == 201
        assert isinstance(r.json()["diffs"], list)
