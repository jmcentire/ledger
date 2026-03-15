"""Schema inference — live backend introspection to generate draft schema YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import yaml


# ── Exceptions ────────────────────────────────────────


class InferenceError(Exception):
    """Base error for inference operations."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class MissingDependencyError(InferenceError):
    """Raised when an optional dependency is not installed."""

    def __init__(self, package: str, backend_type: str):
        self.package = package
        self.backend_type = backend_type
        super().__init__(
            f"Live introspection for {backend_type} requires the '{package}' package. "
            f"Install it with: pip install ledger-registry[{backend_type}]"
        )


# ── Data Models ───────────────────────────────────────


@dataclass
class InferredField:
    name: str
    field_type: str
    nullable: bool = True
    classification: str = "PUBLIC"
    annotations: list[str] = field(default_factory=list)
    _confidence: str = "draft"


@dataclass
class InferredTable:
    name: str
    fields: list[InferredField] = field(default_factory=list)
    _confidence: str = "draft"


@dataclass
class InferredSchema:
    backend_id: str
    backend_type: str
    tables: list[InferredTable] = field(default_factory=list)
    _confidence: str = "draft"


# ── Classification Heuristics ─────────────────────────


# Field name patterns that suggest specific classifications and annotations.
_PII_PATTERNS = {
    "email", "phone", "address", "name", "first_name", "last_name",
    "full_name", "username", "ssn", "social_security", "date_of_birth",
    "dob", "zip", "zipcode", "zip_code", "postal_code", "city", "state",
    "country", "street", "ip_address", "user_agent",
}

_FINANCIAL_PATTERNS = {
    "payment", "card", "credit_card", "card_number", "cvv", "cvc",
    "account_number", "routing_number", "iban", "swift", "balance",
    "amount", "price", "total", "subtotal", "tax", "payment_token",
    "stripe_customer_id", "bank_account",
}

_AUTH_PATTERNS = {
    "password", "password_hash", "secret", "api_key", "token",
    "access_token", "refresh_token", "session_id", "session_token",
    "auth_token", "private_key", "signing_key",
}

_AUDIT_PATTERNS = {
    "created_at", "updated_at", "modified_at", "deleted_at",
    "created_by", "updated_by", "modified_by",
}

_PK_PATTERNS = {"id", "pk", "uuid"}

_SOFT_DELETE_PATTERNS = {"deleted_at", "is_deleted", "soft_deleted"}

_IMMUTABLE_PATTERNS = {"id", "pk", "uuid", "created_at", "created_by"}


def classify_field_name(name: str) -> tuple[str, list[str]]:
    """Guess classification tier and annotations from a field name.

    Returns (classification, annotations_list).
    """
    lower = name.lower()
    classification = "PUBLIC"
    annotations: list[str] = []

    # Check FINANCIAL before AUTH because some fields like "payment_token"
    # match both "payment" (FINANCIAL) and "token" (AUTH) — FINANCIAL wins.
    if lower in _FINANCIAL_PATTERNS or any(p in lower for p in ("payment", "card_number", "account_number", "cvv")):
        classification = "FINANCIAL"
        if any(p in lower for p in ("card", "cvv", "cvc", "account_number")):
            annotations.append("encrypted_at_rest")
        if "token" in lower:
            annotations.append("tokenized")
    elif lower in _AUTH_PATTERNS or any(p in lower for p in ("password", "secret", "api_key", "token", "private_key")):
        classification = "AUTH"
        annotations.append("encrypted_at_rest")
    elif lower in _PII_PATTERNS or any(p in lower for p in ("email", "phone", "address", "ssn")):
        classification = "PII"
        annotations.append("pii_field")
        annotations.append("gdpr_erasable")

    # Annotation heuristics (independent of classification)
    if lower in _PK_PATTERNS or lower.endswith("_id") and lower == "id":
        annotations.append("primary_key")
        annotations.append("immutable")
    elif lower in _IMMUTABLE_PATTERNS:
        annotations.append("immutable")

    if lower in _SOFT_DELETE_PATTERNS:
        annotations.append("soft_delete_marker")

    if lower in _AUDIT_PATTERNS:
        annotations.append("audit_field")

    return classification, annotations


def guess_classification(field_name: str, field_type: str) -> tuple[str, list[str]]:
    """Higher-level classification using both name and type.

    Returns (classification, annotations_list).
    """
    classification, annotations = classify_field_name(field_name)

    # Type-based refinements
    type_lower = field_type.lower() if field_type else ""
    if "json" in type_lower and classification == "PUBLIC":
        # JSON blobs could contain anything -- flag as needing review
        pass
    if "bytea" in type_lower or "blob" in type_lower:
        if "encrypted" not in " ".join(annotations):
            annotations.append("encrypted_at_rest")

    return classification, annotations


# ── Postgres Introspection ────────────────────────────


_PG_TYPE_MAP = {
    "integer": "integer",
    "bigint": "bigint",
    "smallint": "smallint",
    "boolean": "boolean",
    "text": "text",
    "character varying": "varchar",
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
    "date": "date",
    "uuid": "uuid",
    "jsonb": "jsonb",
    "json": "json",
    "bytea": "bytea",
    "numeric": "numeric",
    "double precision": "float8",
    "real": "float4",
}


def infer_postgres_schema(
    backend_id: str,
    connection_string: str,
    schema_name: str = "public",
    show_confidence: bool = False,
) -> InferredSchema:
    """Introspect a PostgreSQL database and return an InferredSchema.

    Requires psycopg2.
    """
    try:
        import psycopg2
    except ImportError:
        raise MissingDependencyError("psycopg2-binary", "postgres")

    result = InferredSchema(
        backend_id=backend_id,
        backend_type="postgres",
    )

    conn = psycopg2.connect(connection_string)
    try:
        cur = conn.cursor()

        # Get all tables in the schema
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
            (schema_name,),
        )
        tables = [row[0] for row in cur.fetchall()]

        for table_name in tables:
            cur.execute(
                "SELECT column_name, data_type, is_nullable, "
                "character_maximum_length "
                "FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema_name, table_name),
            )
            columns = cur.fetchall()

            inferred_table = InferredTable(name=table_name)
            for col_name, data_type, is_nullable, char_max_len in columns:
                mapped_type = _PG_TYPE_MAP.get(data_type, data_type)
                if mapped_type == "varchar" and char_max_len:
                    mapped_type = f"varchar({char_max_len})"

                classification, annotations = guess_classification(col_name, mapped_type)
                nullable = is_nullable == "YES"

                inferred_field = InferredField(
                    name=col_name,
                    field_type=mapped_type,
                    nullable=nullable,
                    classification=classification,
                    annotations=annotations,
                )
                inferred_table.fields.append(inferred_field)

            result.tables.append(inferred_table)

        cur.close()
    finally:
        conn.close()

    return result


# ── Backend Router ────────────────────────────────────

_BACKEND_DEPS = {
    "postgres": "psycopg2-binary",
    "mysql": "pymysql",
    "sqlite": "sqlite3 (stdlib)",
    "redis": "redis",
    "mongo": "pymongo",
    "dynamodb": "boto3",
    "s3": "boto3",
    "kafka": "confluent-kafka",
    "cassandra": "cassandra-driver",
    "rabbitmq": "pika",
    "sqs": "boto3",
}


def infer_schema(
    backend_id: str,
    backend_type: str,
    connection_config: dict[str, Any],
    show_confidence: bool = False,
) -> InferredSchema:
    """Main entry point: infer schema from a registered backend.

    Args:
        backend_id: The registered backend identifier.
        backend_type: Type of backend (postgres, mysql, redis, etc.).
        connection_config: Backend-specific connection parameters.
        show_confidence: Whether to include confidence markers.

    Returns:
        InferredSchema with draft field mappings.

    Raises:
        MissingDependencyError: If required package is not installed.
        InferenceError: On connection or introspection failure.
    """
    bt = backend_type.lower()

    if bt == "postgres":
        conn_str = connection_config.get("connection_string", "")
        pg_schema = connection_config.get("schema", "public")
        if not conn_str:
            raise InferenceError(
                "PostgreSQL introspection requires 'connection_string' in backend config."
            )
        return infer_postgres_schema(
            backend_id, conn_str, pg_schema, show_confidence
        )

    # For unsupported backends, raise a helpful error about the required dep
    dep = _BACKEND_DEPS.get(bt)
    if dep:
        raise MissingDependencyError(dep, bt)

    raise InferenceError(
        f"Schema inference is not supported for backend type '{backend_type}'. "
        f"Supported types: {', '.join(sorted(_BACKEND_DEPS.keys()))}"
    )


# ── YAML Serialization ───────────────────────────────


def schema_to_yaml(schema: InferredSchema, show_confidence: bool = False) -> str:
    """Serialize an InferredSchema to draft YAML format."""
    data: dict[str, Any] = {
        "backend_id": schema.backend_id,
        "backend_type": schema.backend_type,
    }
    if show_confidence:
        data["_confidence"] = schema._confidence

    tables_data = []
    for table in schema.tables:
        table_dict: dict[str, Any] = {"name": table.name}
        if show_confidence:
            table_dict["_confidence"] = table._confidence

        fields_data = []
        for f in table.fields:
            field_dict: dict[str, Any] = {
                "name": f.name,
                "field_type": f.field_type,
                "classification": f.classification,
                "nullable": f.nullable,
            }
            if f.annotations:
                field_dict["annotations"] = f.annotations
            if show_confidence:
                field_dict["_confidence"] = f._confidence
            fields_data.append(field_dict)

        table_dict["fields"] = fields_data
        tables_data.append(table_dict)

    data["tables"] = tables_data
    return yaml.dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False)
