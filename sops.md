# Operating Procedures

## Tech Stack
- Language: Python 3.12+
- Data models: Pydantic >= 2.0
- HTTP: FastAPI + uvicorn
- Testing: pytest
- CLI: click
- YAML: pyyaml
- Mock data: faker (PII generation)
- Config: ledger.yaml (YAML)

## Project Structure
- Source: `src/ledger/`
- Tests: `tests/`
- Entry point: `ledger = "ledger.cli:main"`
- Subpackages mirror the component map: cli, registry, migration, propagation, api, mock, config, export

## Standards
- Type annotations on all public functions
- Pydantic models for all structured data (schemas, annotations, migration diffs, gate results)
- Composition over inheritance
- snake_case functions, PascalCase classes
- Annotation propagation rules defined as data (dict/table), not code branches
- Schema YAML stored verbatim — never normalize, reformat, or reorder on ingestion

## Verification
- All public functions must have at least one test
- Tests run without external services — no database, no Arbiter API, no network
- Mock Arbiter integration with httpx fixtures when testing canary registration
- Migration parser tested against known SQL inputs with expected diffs

## Error Handling
- Schema validation errors: return all violations, not just the first
- Migration gate errors: return full violation list with severity levels
- Missing Arbiter: warn and skip canary registration, never crash
- Invalid YAML: fail with clear parse error and file path

## Preferences
- Prefer stdlib over third-party where equivalent
- Keep files under 300 lines
- Faker only for PII mock generation — stdlib random for everything else
- No ORM — Ledger reads schemas, it does not connect to databases
