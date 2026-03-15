# TASKS — ledger
Progress: 32/62 completed (52%)

## Phase: Setup

- [ ] T001 Initialize project directory structure
- [ ] T002 Verify environment and dependencies

## Phase: Foundational

- [ ] T003 [P] Define shared type: ApiProtocol
- [ ] T004 [P] Define shared type: BackendType
- [ ] T005 [P] Define shared type: ChangelogEntry
- [ ] T006 [P] Define shared type: ClassificationTier
- [ ] T007 [P] Define shared type: ExportFormat
- [ ] T008 [P] Define shared type: ExportProtocol
- [ ] T009 [P] Define shared type: LedgerConfig
- [ ] T010 [P] Define shared type: LedgerError
- [ ] T011 [P] Define shared type: MigrationPlan
- [ ] T012 [P] Define shared type: MigrationProtocol
- [ ] T013 [P] Define shared type: MockGenerationRequest
- [ ] T014 [P] Define shared type: MockProtocol
- [ ] T015 [P] Define shared type: MockPurpose
- [ ] T016 [P] Define shared type: PlanStatus
- [ ] T017 [P] Define shared type: RegistryProtocol
- [ ] T018 [P] Define shared type: Severity
- [ ] T019 [P] Define shared type: Violation
- [ ] T020 [P] Define shared type: ViolationSeverity

## Phase: Component

- [x] T021 [P] [config] Review contract for Configuration & Data Models (contracts/config/interface.json)
- [x] T022 [config] Set up test harness for Configuration & Data Models
- [x] T023 [config] Write contract tests for Configuration & Data Models
- [x] T024 [config] Implement Configuration & Data Models (implementations/config/src/)
- [x] T025 [config] Run tests and verify Configuration & Data Models
- [x] T026 [registry] Review contract for Registry & Schema Store (contracts/registry/interface.json)
- [x] T027 [registry] Set up test harness for Registry & Schema Store
- [x] T028 [registry] Write contract tests for Registry & Schema Store
- [x] T029 [registry] Implement Registry & Schema Store (implementations/registry/src/)
- [x] T030 [registry] Run tests and verify Registry & Schema Store
- [x] T031 [migration] Review contract for Migration Parser & Planner (contracts/migration/interface.json)
- [x] T032 [migration] Set up test harness for Migration Parser & Planner
- [x] T033 [migration] Write contract tests for Migration Parser & Planner
- [x] T034 [migration] Implement Migration Parser & Planner (implementations/migration/src/)
- [x] T035 [migration] Run tests and verify Migration Parser & Planner
- [x] T036 [export] Review contract for Export Generators (contracts/export/interface.json)
- [x] T037 [export] Set up test harness for Export Generators
- [x] T038 [export] Write contract tests for Export Generators
- [x] T039 [export] Implement Export Generators (implementations/export/src/)
- [x] T040 [export] Run tests and verify Export Generators
- [x] T041 [mock] Review contract for Mock Data Generator (contracts/mock/interface.json)
- [x] T042 [mock] Set up test harness for Mock Data Generator
- [x] T043 [mock] Write contract tests for Mock Data Generator
- [x] T044 [mock] Implement Mock Data Generator (implementations/mock/src/)
- [ ] T045 [mock] Run tests and verify Mock Data Generator
- [x] T046 [cli] Review contract for CLI Entry Point (contracts/cli/interface.json)
- [x] T047 [cli] Set up test harness for CLI Entry Point
- [x] T048 [cli] Write contract tests for CLI Entry Point
- [x] T049 [cli] Implement CLI Entry Point (implementations/cli/src/)
- [ ] T050 [cli] Run tests and verify CLI Entry Point
- [x] T051 [api] Review contract for HTTP API Server (contracts/api/interface.json)
- [x] T052 [api] Set up test harness for HTTP API Server
- [x] T053 [api] Write contract tests for HTTP API Server
- [x] T054 [api] Implement HTTP API Server (implementations/api/src/)
- [ ] T055 [api] Run tests and verify HTTP API Server

---
CHECKPOINT: All leaf components verified

## Phase: Integration

- [ ] T056 [root] Review integration contract for Root
- [ ] T057 [P] [root] Write integration tests for Root
- [ ] T058 [root] Wire children for Root
- [ ] T059 [root] Run integration tests for Root

---
CHECKPOINT: All integrations verified

## Phase: Polish

- [ ] T060 Run full contract validation gate
- [ ] T061 Cross-artifact analysis
- [ ] T062 Update design document
