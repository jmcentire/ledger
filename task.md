# Ledger — Schema Registry and Data Obligation Manager

Build the schema registry and data obligation manager for the distributed stack
(Pact, Arbiter, Baton, Sentinel, Constrain). Ledger is the authoritative answer to
"what does the data look like and what are its rules?" — it maintains a unified
model of every storage backend and external data surface in the system.

## Context

Data obligations today are scattered across tools — Arbiter has its own idea of what
"PII" means, Pact generates stubs without knowing which fields are encrypted, Baton
masks fields without a central registry to consult. This drift creates compliance gaps.

Ledger centralizes this. It tracks all backend types through a unified abstraction:
Backend -> Storage Units -> Fields -> Annotations. The vocabulary differs by backend
(table vs collection vs topic vs key pattern vs API resource) but the model is identical.

Engineers annotate fields in schema YAML files, and those annotations automatically
propagate into Pact contract assertions, Arbiter classification rules, Baton egress
masking config, Sentinel severity mappings, and infrastructure retention requirements.

Ledger also gates migrations for mutable backends, parses migration files, computes
schema diffs, and returns BLOCKED / HUMAN_GATE / AUTO_PROCEED. External APIs are
excluded from migration gating since Ledger cannot control external schema changes.

## Constraints

- Python 3.12+. Single package, no microservices.
- All backend types use the unit/unit_type abstraction — no backend-specific
  terminology in the core schema model.
- Schema YAML files stored verbatim — no normalization on ingestion.
- Schema change log is append-only.
- No two components may own the same storage backend.
- Annotation conflict pairs are hard errors at validation time.
- Annotation propagation rules are data-driven (table-defined).
- Mock data for encrypted_at_rest fields must never contain raw plaintext.
- Canary fingerprint format: `ledger-canary-{tier}-{hex8}` shaped to field type.
- Ledger must work without Arbiter configured.
- Schema inference produces drafts only — never writes to registry automatically.
- Redis inference uses SCAN (not KEYS), must accept --key-pattern filter.
- S3/R2 body sampling gated behind --sample-bodies flag, off by default.
- SQS/RabbitMQ inference warns against production queues.
- Built-in annotation maps (Stripe) are YAML data files, not hardcoded.
- HTTP API starts on configured port (default 7701) in under 3 seconds.

## Supported Backend Types

| Type         | Storage Unit         | Migration Format Support                    |
|--------------|----------------------|---------------------------------------------|
| postgres     | table                | SQL, Alembic, Flyway, Liquibase             |
| mysql        | table                | SQL, Flyway, Liquibase                      |
| sqlite       | table                | SQL                                         |
| mongodb      | collection           | JSON patch, custom scripts                  |
| redis        | key_pattern          | Manual schema update only                   |
| cassandra    | table                | CQL migration files                         |
| kafka        | topic                | Schema Registry evolution (Avro/Protobuf)   |
| rabbitmq     | queue                | Queue/exchange config changes               |
| sqs          | queue                | Queue attribute changes                     |
| s3 / r2      | object_pattern       | Bucket policy/lifecycle changes             |
| stripe       | api_resource         | Not applicable (external, read-only)        |
| generic_http | api_endpoint         | Not applicable (external, read-only)        |

## Requirements

### Registry (v1 core)
- `ledger init` creates `.ledger/` directory with empty registry.
- `ledger backend add` registers backend with owner component. Duplicate id fails.
- `ledger schema add` accepts and stores schema YAML verbatim.
- `ledger schema validate` checks annotation conflicts, REQUIRES satisfaction,
  backend ownership exclusivity. Returns all violations, not just first.

### Multi-Backend Schemas (v2)
- MongoDB schemas support dot-notation sub-fields to arbitrary depth.
- Redis schemas describe key patterns with value_format.
- Kafka schemas describe topics with message_format and optional schema_ref.
- S3/R2 schemas describe object patterns with _metadata and _body fields.
- External API schemas have direction (request/response) on fields.
- Cassandra schemas include partition_keys and clustering_keys.

### Schema Inference (v2)
- `ledger schema infer --backend <id> --unit <unit>` introspects live backend.
- Produces draft schema for human review — not written to registry automatically.
- Classification is never inferred (except partial for Stripe built-in).
- Graceful failure when backend unreachable.
- Backend-specific inference: information_schema for postgres, document sampling
  for mongodb, SCAN for redis, Schema Registry for kafka, Stripe API spec for stripe.

### Migration Analysis
- Covers all mutable backend types. External APIs excluded.
- SQL parser (v1): extracts ADD/DROP/ALTER COLUMN from PostgreSQL/ANSI SQL.
- Kafka evolution (v2): validates against Schema Registry compatibility + Ledger annotations.
- Gate logic: audit_field drop -> BLOCKED. immutable modify -> BLOCKED.
  Encryption removal -> HUMAN_GATE. Tier not in data_access -> HUMAN_GATE.
  PUBLIC-only change to declared component -> AUTO_PROCEED.
- Foreign key annotations include referenced table in blast radius.

### Export
- Pact: contract assertion YAML per component.
- Arbiter: classification rules for all non-PUBLIC fields.
- Baton: egress node config for all backends.
- Sentinel: severity mappings.
- Retention (v2): infrastructure config hints (Kafka retention.ms, S3 lifecycle rules)
  for backends with audit_field or retention_days annotations.

### Built-in Annotation Maps (v2)
- Stripe: ships curated YAML mapping known PII and FINANCIAL fields.
- `ledger builtins apply stripe` applies to registered Stripe backend.
- Per-field override without affecting other fields.

### Mock Data
- Records match field types and classification tiers.
- encrypted_at_rest -> token-shaped, never raw. tokenized -> token-shaped.
- Canary values: fingerprinted, registered with Arbiter if configured.
- Deterministic: same seed + schema = identical output.

### HTTP API
- All endpoints from v1 plus: /schemas/infer, /export/retention,
  /builtins, /builtins/<service>, /builtins/<service>/override.

## Functional Assertions

FA-L-001 through FA-L-025: all v1 assertions (updated for unit/unit_type model).
FA-L-026 through FA-L-044: v2 assertions covering multi-backend validation,
inference, Kafka/Cassandra/MongoDB migration gating, built-ins, and retention export.
See constraints.yaml (C001-C038) and prompt.md for full details.
