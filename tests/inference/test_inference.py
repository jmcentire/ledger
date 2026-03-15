"""Tests for the inference module — schema introspection and field classification."""

import pytest
from unittest.mock import MagicMock, patch

from inference import (
    InferredField,
    InferredTable,
    InferredSchema,
    InferenceError,
    MissingDependencyError,
    infer_schema,
    classify_field_name,
    guess_classification,
)
from inference.inference import schema_to_yaml, infer_postgres_schema


# ===========================================================================
#  classify_field_name
# ===========================================================================


class TestClassifyFieldName:
    """Tests for classify_field_name heuristic."""

    def test_email_classified_as_pii(self):
        classification, annotations = classify_field_name("email")
        assert classification == "PII"
        assert "pii_field" in annotations
        assert "gdpr_erasable" in annotations

    def test_phone_classified_as_pii(self):
        classification, _ = classify_field_name("phone")
        assert classification == "PII"

    def test_password_classified_as_auth(self):
        classification, annotations = classify_field_name("password")
        assert classification == "AUTH"
        assert "encrypted_at_rest" in annotations

    def test_password_hash_classified_as_auth(self):
        classification, annotations = classify_field_name("password_hash")
        assert classification == "AUTH"

    def test_api_key_classified_as_auth(self):
        classification, annotations = classify_field_name("api_key")
        assert classification == "AUTH"
        assert "encrypted_at_rest" in annotations

    def test_payment_token_classified_as_financial(self):
        classification, annotations = classify_field_name("payment_token")
        assert classification == "FINANCIAL"
        assert "tokenized" in annotations

    def test_card_number_classified_as_financial(self):
        classification, annotations = classify_field_name("card_number")
        assert classification == "FINANCIAL"
        assert "encrypted_at_rest" in annotations

    def test_created_at_has_audit_field(self):
        _, annotations = classify_field_name("created_at")
        assert "audit_field" in annotations
        assert "immutable" in annotations

    def test_deleted_at_has_soft_delete_marker(self):
        _, annotations = classify_field_name("deleted_at")
        assert "soft_delete_marker" in annotations

    def test_generic_field_classified_as_public(self):
        classification, annotations = classify_field_name("description")
        assert classification == "PUBLIC"

    def test_id_field_gets_primary_key(self):
        classification, annotations = classify_field_name("id")
        assert "primary_key" in annotations
        assert "immutable" in annotations

    def test_ssn_classified_as_pii(self):
        classification, _ = classify_field_name("ssn")
        assert classification == "PII"

    def test_balance_classified_as_financial(self):
        classification, _ = classify_field_name("balance")
        assert classification == "FINANCIAL"

    def test_session_token_classified_as_auth(self):
        classification, _ = classify_field_name("session_token")
        assert classification == "AUTH"


# ===========================================================================
#  guess_classification
# ===========================================================================


class TestGuessClassification:
    """Tests for guess_classification which uses both name and type."""

    def test_bytea_type_gets_encrypted(self):
        classification, annotations = guess_classification("data_blob", "bytea")
        assert "encrypted_at_rest" in annotations

    def test_blob_type_gets_encrypted(self):
        classification, annotations = guess_classification("some_field", "blob")
        assert "encrypted_at_rest" in annotations

    def test_varchar_email_still_pii(self):
        classification, _ = guess_classification("email", "varchar(255)")
        assert classification == "PII"

    def test_integer_generic_is_public(self):
        classification, _ = guess_classification("count", "integer")
        assert classification == "PUBLIC"


# ===========================================================================
#  InferredSchema data models
# ===========================================================================


class TestInferredModels:
    """Tests for the data model classes."""

    def test_inferred_field_defaults(self):
        f = InferredField(name="test", field_type="text")
        assert f.nullable is True
        assert f.classification == "PUBLIC"
        assert f.annotations == []
        assert f._confidence == "draft"

    def test_inferred_table_defaults(self):
        t = InferredTable(name="users")
        assert t.fields == []
        assert t._confidence == "draft"

    def test_inferred_schema_defaults(self):
        s = InferredSchema(backend_id="db1", backend_type="postgres")
        assert s.tables == []
        assert s._confidence == "draft"


# ===========================================================================
#  schema_to_yaml
# ===========================================================================


class TestSchemaToYaml:
    """Tests for schema_to_yaml serialization."""

    def test_basic_serialization(self):
        schema = InferredSchema(
            backend_id="test_db",
            backend_type="postgres",
            tables=[
                InferredTable(
                    name="users",
                    fields=[
                        InferredField(name="id", field_type="uuid",
                                      classification="PUBLIC",
                                      annotations=["primary_key"]),
                    ],
                ),
            ],
        )
        output = schema_to_yaml(schema)
        assert "backend_id: test_db" in output
        assert "backend_type: postgres" in output
        assert "name: users" in output
        assert "name: id" in output
        assert "primary_key" in output

    def test_confidence_markers_included_when_requested(self):
        schema = InferredSchema(
            backend_id="db1",
            backend_type="postgres",
            tables=[
                InferredTable(
                    name="t1",
                    fields=[
                        InferredField(name="col1", field_type="text"),
                    ],
                ),
            ],
        )
        output = schema_to_yaml(schema, show_confidence=True)
        assert "_confidence: draft" in output

    def test_confidence_markers_excluded_by_default(self):
        schema = InferredSchema(
            backend_id="db1",
            backend_type="postgres",
            tables=[
                InferredTable(
                    name="t1",
                    fields=[
                        InferredField(name="col1", field_type="text"),
                    ],
                ),
            ],
        )
        output = schema_to_yaml(schema, show_confidence=False)
        assert "_confidence" not in output

    def test_empty_annotations_excluded(self):
        schema = InferredSchema(
            backend_id="db1",
            backend_type="postgres",
            tables=[
                InferredTable(
                    name="t1",
                    fields=[
                        InferredField(name="col1", field_type="text", annotations=[]),
                    ],
                ),
            ],
        )
        output = schema_to_yaml(schema)
        assert "annotations" not in output


# ===========================================================================
#  Exceptions
# ===========================================================================


class TestInferenceExceptions:
    """Tests for inference exception types."""

    def test_inference_error_has_message(self):
        err = InferenceError("test error")
        assert err.message == "test error"
        assert str(err) == "test error"

    def test_missing_dependency_error_has_details(self):
        err = MissingDependencyError("psycopg2-binary", "postgres")
        assert err.package == "psycopg2-binary"
        assert err.backend_type == "postgres"
        assert "psycopg2-binary" in err.message
        assert "postgres" in err.message


# ===========================================================================
#  infer_schema router
# ===========================================================================


class TestInferSchema:
    """Tests for the infer_schema dispatch function."""

    def test_postgres_without_connection_string_raises(self):
        with pytest.raises(InferenceError, match="connection_string"):
            infer_schema("db1", "postgres", {})

    def test_unsupported_backend_type_raises(self):
        with pytest.raises(InferenceError, match="not supported"):
            infer_schema("db1", "unknown_backend", {})

    def test_mysql_raises_missing_dep(self):
        with pytest.raises(MissingDependencyError) as exc_info:
            infer_schema("db1", "mysql", {"connection_string": "mysql://..."})
        assert exc_info.value.package == "pymysql"

    def test_redis_raises_missing_dep(self):
        with pytest.raises(MissingDependencyError) as exc_info:
            infer_schema("db1", "redis", {})
        assert exc_info.value.package == "redis"

    def test_mongo_raises_missing_dep(self):
        with pytest.raises(MissingDependencyError) as exc_info:
            infer_schema("db1", "mongo", {})
        assert exc_info.value.package == "pymongo"

    def test_dynamodb_raises_missing_dep(self):
        with pytest.raises(MissingDependencyError) as exc_info:
            infer_schema("db1", "dynamodb", {})
        assert exc_info.value.package == "boto3"

    def test_case_insensitive_backend_type(self):
        """Backend type should be case-insensitive."""
        with pytest.raises(InferenceError, match="connection_string"):
            infer_schema("db1", "POSTGRES", {})

    @patch("inference.inference.infer_postgres_schema")
    def test_postgres_delegates_correctly(self, mock_pg):
        mock_pg.return_value = InferredSchema(
            backend_id="db1", backend_type="postgres"
        )
        result = infer_schema(
            "db1", "postgres",
            {"connection_string": "postgresql://localhost/test", "schema": "myschema"},
        )
        mock_pg.assert_called_once_with("db1", "postgresql://localhost/test", "myschema", False)
        assert result.backend_id == "db1"


# ===========================================================================
#  infer_postgres_schema (with mocked psycopg2)
# ===========================================================================


class TestInferPostgresSchema:
    """Tests for PostgreSQL introspection with mocked database connection."""

    def test_raises_when_psycopg2_not_installed(self):
        """Without psycopg2 installed, should raise MissingDependencyError."""
        with patch.dict("sys.modules", {"psycopg2": None}):
            with pytest.raises(MissingDependencyError):
                infer_postgres_schema("db1", "postgresql://localhost/test")

    @patch("inference.inference.psycopg2", create=True)
    def test_introspects_tables_and_columns(self, mock_psycopg2):
        """Verify that tables and columns are read from information_schema."""
        import sys

        # Create mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock psycopg2.connect
        mock_psycopg2_module = MagicMock()
        mock_psycopg2_module.connect.return_value = mock_conn

        # Set up cursor.fetchall to return tables then columns
        call_count = [0]
        def side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                # Tables query
                return [("users",), ("orders",)]
            elif call_count[0] == 2:
                # Columns for 'users'
                return [
                    ("id", "uuid", "NO", None),
                    ("email", "character varying", "NO", 255),
                    ("created_at", "timestamp with time zone", "NO", None),
                ]
            elif call_count[0] == 3:
                # Columns for 'orders'
                return [
                    ("id", "uuid", "NO", None),
                    ("amount", "numeric", "NO", None),
                ]
            return []

        mock_cursor.fetchall = side_effect

        with patch.dict("sys.modules", {"psycopg2": mock_psycopg2_module}):
            # Need to reimport since we're patching the module
            from importlib import reload
            import inference.inference as mod
            reload(mod)

            result = mod.infer_postgres_schema("db1", "postgresql://localhost/test")

            assert result.backend_id == "db1"
            assert result.backend_type == "postgres"
            assert len(result.tables) == 2
            assert result.tables[0].name == "users"
            assert len(result.tables[0].fields) == 3

            # Check email field classification
            email_field = result.tables[0].fields[1]
            assert email_field.name == "email"
            assert email_field.field_type == "varchar(255)"
            assert email_field.classification == "PII"

            # Restore module
            reload(mod)
