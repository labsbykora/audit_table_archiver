# Phase 1: Foundation & MVP - Detailed Task Breakdown

**Duration**: 4-6 weeks  
**Goal**: Working archiver for single database, single table with basic safety guarantees

---

## Week 1: Project Setup & Core Infrastructure

### Day 1-2: Repository Setup

#### Task 1.1: Initialize Repository Structure
**Owner**: Lead Engineer  
**Duration**: 2 hours  
**Dependencies**: None

**Actions**:
- [ ] Create GitHub repository
- [ ] Set up project structure (src/, tests/, docs/, etc.)
- [ ] Create `pyproject.toml` with project metadata
- [ ] Add `.gitignore` for Python
- [ ] Create initial `README.md`

**Acceptance Criteria**:
- Repository structure matches plan
- `pyproject.toml` includes project name, version, description
- README has basic project description

---

#### Task 1.2: Set Up Development Dependencies
**Owner**: Backend Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Add development dependencies to `pyproject.toml`:
  - pytest, pytest-asyncio, pytest-mock
  - black, ruff, mypy
  - pre-commit hooks
- [ ] Create `requirements-dev.txt`
- [ ] Configure `ruff.toml` and `mypy.ini`
- [ ] Set up pre-commit hooks

**Acceptance Criteria**:
- All dev dependencies installable
- Pre-commit hooks run on commit
- Linting and type checking configured

---

#### Task 1.3: Set Up CI/CD Pipeline
**Owner**: DevOps Engineer  
**Duration**: 3 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Create `.github/workflows/ci.yml`
- [ ] Configure Python matrix (3.9, 3.10, 3.11)
- [ ] Add linting step (ruff, mypy)
- [ ] Add testing step (pytest)
- [ ] Add coverage reporting
- [ ] Configure test badges

**Acceptance Criteria**:
- CI runs on every push/PR
- All checks pass
- Coverage report generated

---

#### Task 1.4: Create Docker Compose for Local Development
**Owner**: Backend Engineer  
**Duration**: 3 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Create `docker/docker-compose.yml`
- [ ] Add PostgreSQL 14 service
- [ ] Add MinIO service (S3-compatible)
- [ ] Configure networking
- [ ] Add initialization scripts
- [ ] Document usage in README

**Acceptance Criteria**:
- `docker-compose up` starts PostgreSQL and MinIO
- Services accessible from host
- Can connect to both services

---

#### Task 1.5: Write Initial README
**Owner**: Lead Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Add project description
- [ ] Add quick start guide
- [ ] Add installation instructions
- [ ] Add development setup
- [ ] Add contribution guidelines

**Acceptance Criteria**:
- README is clear and complete
- Quick start works for new developers

---

### Day 3-4: Configuration System

#### Task 1.6: Implement YAML Configuration Parser
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Install PyYAML
- [ ] Create `archiver/config.py`
- [ ] Implement YAML file reading
- [ ] Add basic structure validation
- [ ] Write unit tests

**Acceptance Criteria**:
- Can parse valid YAML files
- Throws clear errors for invalid YAML
- Unit tests pass

---

#### Task 1.7: Create Pydantic Models for Configuration
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 1.6

**Actions**:
- [ ] Install Pydantic v2
- [ ] Create `DatabaseConfig` model
- [ ] Create `TableConfig` model
- [ ] Create `S3Config` model
- [ ] Create `ArchiverConfig` root model
- [ ] Add field validators
- [ ] Write unit tests

**Acceptance Criteria**:
- All configuration fields validated
- Clear error messages for invalid config
- Unit tests cover all validation paths

---

#### Task 1.8: Implement Environment Variable Substitution
**Owner**: Backend Engineer  
**Duration**: 3 hours  
**Dependencies**: Task 1.7

**Actions**:
- [ ] Add `${VAR}` syntax support
- [ ] Implement variable resolution
- [ ] Add default value support `${VAR:-default}`
- [ ] Handle missing variables (error or warning)
- [ ] Write unit tests

**Acceptance Criteria**:
- Environment variables substituted correctly
- Missing variables handled appropriately
- Unit tests pass

---

#### Task 1.9: Add Configuration File Validation
**Owner**: Backend Engineer  
**Duration**: 3 hours  
**Dependencies**: Task 1.7

**Actions**:
- [ ] Add `validate()` method to config models
- [ ] Validate required fields
- [ ] Validate value ranges (e.g., batch_size > 0)
- [ ] Validate relationships (e.g., table exists in database)
- [ ] Generate clear error messages
- [ ] Write unit tests

**Acceptance Criteria**:
- Invalid configurations rejected with clear errors
- All validation rules tested

---

#### Task 1.10: Create Example Configuration Files
**Owner**: Backend Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 1.7

**Actions**:
- [ ] Create `docs/examples/config-simple.yaml`
- [ ] Create `docs/examples/config-multi-db.yaml`
- [ ] Add comments explaining options
- [ ] Validate examples parse correctly

**Acceptance Criteria**:
- Example configs are valid
- Examples are well-documented

---

### Day 5: Logging & Error Handling

#### Task 1.11: Set Up Structured Logging
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Install structlog
- [ ] Create `archiver/utils/logging.py`
- [ ] Configure JSON output format
- [ ] Add log level configuration
- [ ] Add correlation ID support
- [ ] Write unit tests

**Acceptance Criteria**:
- Logs output as structured JSON
- Correlation IDs included in all logs
- Log levels configurable

---

#### Task 1.12: Create Custom Exception Hierarchy
**Owner**: Backend Engineer  
**Duration**: 3 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Create `archiver/exceptions.py`
- [ ] Define base `ArchiverError`
- [ ] Create error categories:
  - `ConfigurationError`
  - `DatabaseError`
  - `S3Error`
  - `VerificationError`
- [ ] Add error context support
- [ ] Write unit tests

**Acceptance Criteria**:
- All exceptions inherit from base
- Exceptions include context
- Unit tests cover all exception types

---

#### Task 1.13: Implement Correlation ID Tracking
**Owner**: Backend Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 1.11

**Actions**:
- [ ] Generate correlation ID per run
- [ ] Add to logging context
- [ ] Include in all log entries
- [ ] Add to error messages

**Acceptance Criteria**:
- Correlation ID generated on startup
- Included in all logs
- Can trace operations by ID

---

## Week 2: Database & S3 Integration

### Day 1-2: Database Module

#### Task 2.1: Implement AsyncPG Connection Pooling
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Install asyncpg
- [ ] Create `archiver/database.py`
- [ ] Implement `DatabaseManager` class
- [ ] Add connection pool creation
- [ ] Configure pool size (default: 5)
- [ ] Add connection health checks
- [ ] Write unit tests with mocking

**Acceptance Criteria**:
- Connection pool created successfully
- Health checks work
- Unit tests pass

---

#### Task 2.2: Create Database Connection Manager
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 2.1

**Actions**:
- [ ] Implement context manager for connections
- [ ] Add connection acquisition/release
- [ ] Handle connection errors gracefully
- [ ] Add connection retry logic
- [ ] Write unit tests

**Acceptance Criteria**:
- Connections acquired/released correctly
- Retry logic works for transient errors
- Unit tests pass

---

#### Task 2.3: Implement Transaction Context Manager
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 2.2

**Actions**:
- [ ] Create transaction context manager
- [ ] Support BEGIN/COMMIT/ROLLBACK
- [ ] Add transaction timeout
- [ ] Handle transaction errors
- [ ] Write unit tests

**Acceptance Criteria**:
- Transactions commit/rollback correctly
- Timeout enforced
- Unit tests pass

---

### Day 3-4: S3 Client Module

#### Task 2.4: Implement Boto3 S3 Client Wrapper
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 1.1

**Actions**:
- [ ] Install boto3
- [ ] Create `archiver/s3_client.py`
- [ ] Implement `S3Client` class
- [ ] Add S3-compatible endpoint support
- [ ] Add credential configuration
- [ ] Write unit tests with moto

**Acceptance Criteria**:
- Can connect to S3 and S3-compatible storage
- Credentials configured correctly
- Unit tests pass

---

#### Task 2.5: Implement Basic Upload with Retry
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 2.4

**Actions**:
- [ ] Implement `upload_file()` method
- [ ] Add retry logic (3 attempts)
- [ ] Implement exponential backoff
- [ ] Add upload verification (exists + size)
- [ ] Write unit tests

**Acceptance Criteria**:
- Files upload successfully
- Retry works on transient errors
- Verification confirms upload
- Unit tests pass

---

#### Task 2.6: Add S3 Bucket Validation
**Owner**: Backend Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 2.4

**Actions**:
- [ ] Implement bucket existence check
- [ ] Test write permissions (upload small test file)
- [ ] Test read permissions
- [ ] Add validation to startup

**Acceptance Criteria**:
- Bucket validation runs on startup
- Clear errors if bucket invalid
- Tests pass

---

### Day 5: Integration Testing

#### Task 2.7: Set Up Integration Test Environment
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 1.4, Task 2.1, Task 2.4

**Actions**:
- [ ] Create `tests/integration/` directory
- [ ] Create `tests/integration/conftest.py`
- [ ] Add fixtures for PostgreSQL connection
- [ ] Add fixtures for S3 client (MinIO)
- [ ] Create test database setup/teardown

**Acceptance Criteria**:
- Integration tests can connect to real services
- Fixtures work correctly

---

#### Task 2.8: Test Database Connection
**Owner**: Backend Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 2.7

**Actions**:
- [ ] Write test to connect to PostgreSQL
- [ ] Test connection pooling
- [ ] Test transaction management
- [ ] Verify health checks

**Acceptance Criteria**:
- All database operations work
- Tests pass

---

#### Task 2.9: Test S3 Upload
**Owner**: Backend Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 2.7

**Actions**:
- [ ] Write test to upload to MinIO
- [ ] Test upload verification
- [ ] Test retry logic
- [ ] Verify file exists and size matches

**Acceptance Criteria**:
- S3 upload works
- Verification works
- Tests pass

---

## Week 3: Core Archival Logic

### Day 1-2: Batch Selection & Cursor Pagination

#### Task 3.1: Implement Cursor-Based Pagination
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 2.1

**Actions**:
- [ ] Create `archiver/batch_processor.py`
- [ ] Implement cursor-based query (ORDER BY timestamp, PK)
- [ ] Add WHERE clause for retention period
- [ ] Implement cursor tracking (last timestamp + PK)
- [ ] Write unit tests

**Acceptance Criteria**:
- Pagination works correctly
- No duplicate records
- Unit tests pass

---

#### Task 3.2: Create Batch Selection Query with SKIP LOCKED
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 3.1

**Actions**:
- [ ] Add `FOR UPDATE SKIP LOCKED` to query
- [ ] Test with concurrent transactions
- [ ] Verify no blocking occurs
- [ ] Write integration tests

**Acceptance Criteria**:
- SKIP LOCKED works correctly
- No blocking on locked rows
- Integration tests pass

---

#### Task 3.3: Add Batch Size Configuration
**Owner**: Backend Engineer  
**Duration**: 2 hours  
**Dependencies**: Task 3.1

**Actions**:
- [ ] Add batch_size to TableConfig
- [ ] Use batch_size in query (LIMIT)
- [ ] Add default (10,000)
- [ ] Write unit tests

**Acceptance Criteria**:
- Batch size configurable per table
- Default works
- Unit tests pass

---

### Day 3: Serialization

#### Task 3.4: Implement PostgreSQL Type → JSON Conversion
**Owner**: Backend Engineer  
**Duration**: 8 hours  
**Dependencies**: Task 2.1

**Actions**:
- [ ] Create `archiver/serializer.py`
- [ ] Implement type handlers:
  - int, bigint, smallint
  - text, varchar
  - boolean
  - numeric, decimal (as string)
  - timestamp, timestamptz (ISO 8601)
  - uuid (as string)
  - json, jsonb (preserve)
  - arrays (JSON array)
  - bytea (base64)
- [ ] Handle NULL values
- [ ] Write unit tests for each type

**Acceptance Criteria**:
- All types serialize correctly
- NULL handled properly
- Unit tests cover all types

---

#### Task 3.5: Create JSONL Formatter
**Owner**: Backend Engineer  
**Duration**: 3 hours  
**Dependencies**: Task 3.4

**Actions**:
- [ ] Implement JSONL output (one JSON per line)
- [ ] Add row metadata (_archived_at, _batch_id, etc.)
- [ ] Write to file-like object
- [ ] Write unit tests

**Acceptance Criteria**:
- JSONL format correct
- Metadata included
- Unit tests pass

---

### Day 4: Compression

#### Task 3.6: Implement Gzip Compression
**Owner**: Backend Engineer  
**Duration**: 3 hours  
**Dependencies**: Task 3.5

**Actions**:
- [ ] Create `archiver/compressor.py`
- [ ] Implement gzip compression
- [ ] Add configurable compression level (1-9, default: 6)
- [ ] Calculate compressed size
- [ ] Write unit tests

**Acceptance Criteria**:
- Compression works correctly
- Size calculated accurately
- Unit tests pass

---

### Day 5: Basic Verification

#### Task 3.7: Implement Count Verification
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 3.1, Task 2.1

**Actions**:
- [ ] Create `archiver/verifier.py`
- [ ] Implement DB count (SELECT COUNT(*) before fetch)
- [ ] Implement memory count (count fetched records)
- [ ] Implement S3 count (count lines in JSONL)
- [ ] Add verification failure handling
- [ ] Write unit tests

**Acceptance Criteria**:
- All three counts match
- Mismatch detected and handled
- Unit tests pass

---

## Week 4: Transaction Safety & Delete Operations

### Day 1-2: Transaction Management

#### Task 4.1: Implement Transaction Boundaries Per Batch
**Owner**: Lead Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 2.3, Task 3.1

**Actions**:
- [ ] Create `archiver/transaction_manager.py`
- [ ] Implement transaction per batch
- [ ] Add transaction timeout (default: 30 minutes)
- [ ] Add transaction monitoring
- [ ] Write unit tests

**Acceptance Criteria**:
- Transactions isolated per batch
- Timeout enforced
- Unit tests pass

---

#### Task 4.2: Add Savepoint Support
**Owner**: Lead Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 4.1

**Actions**:
- [ ] Implement savepoint creation
- [ ] Add rollback to savepoint
- [ ] Limit savepoint depth
- [ ] Write unit tests

**Acceptance Criteria**:
- Savepoints work correctly
- Rollback to savepoint works
- Unit tests pass

---

### Day 3: Delete Operations

#### Task 4.3: Implement DELETE Using Primary Keys
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 3.1, Task 2.1

**Actions**:
- [ ] Extract primary keys from fetched records
- [ ] Build DELETE query with PKs
- [ ] Execute DELETE in transaction
- [ ] Verify count of deleted rows
- [ ] Write unit tests

**Acceptance Criteria**:
- DELETE works correctly
- Correct rows deleted
- Unit tests pass

---

### Day 4: Verify-Then-Delete Pattern

#### Task 4.4: Implement Strict Ordering Pattern
**Owner**: Lead Engineer  
**Duration**: 8 hours  
**Dependencies**: Task 4.1, Task 3.7, Task 4.3

**Actions**:
- [ ] Integrate all components:
  - FETCH → UPLOAD → VERIFY → DELETE → COMMIT
- [ ] Add rollback on any failure
- [ ] Add comprehensive error handling
- [ ] Write integration tests

**Acceptance Criteria**:
- Pattern followed strictly
- Rollback on any failure
- Integration tests pass

---

### Day 5: CLI Interface

#### Task 4.5: Implement CLI Using Click
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: All previous tasks

**Actions**:
- [ ] Install Click
- [ ] Create `archiver/main.py`
- [ ] Add `--config` option
- [ ] Add `--dry-run` mode
- [ ] Add `--verbose` flag
- [ ] Add progress output
- [ ] Write CLI tests

**Acceptance Criteria**:
- CLI works correctly
- All options functional
- Tests pass

---

## Week 5: Integration & Testing

### Day 1-2: End-to-End Integration

#### Task 5.1: Integrate All Components
**Owner**: Lead Engineer  
**Duration**: 8 hours  
**Dependencies**: All Week 4 tasks

**Actions**:
- [ ] Wire all components together
- [ ] Test full archival workflow
- [ ] Fix integration issues
- [ ] Add comprehensive error messages

**Acceptance Criteria**:
- Full workflow works end-to-end
- Error messages clear

---

### Day 3: Unit Test Coverage

#### Task 5.2: Achieve >70% Code Coverage
**Owner**: Backend Engineer  
**Duration**: 8 hours  
**Dependencies**: All previous tasks

**Actions**:
- [ ] Run coverage report
- [ ] Identify gaps
- [ ] Write missing tests
- [ ] Test all error paths
- [ ] Test edge cases

**Acceptance Criteria**:
- >70% coverage achieved
- All error paths tested

---

### Day 4: Integration Testing

#### Task 5.3: Test with Real PostgreSQL and MinIO
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 5.1

**Actions**:
- [ ] Create test data (100K records)
- [ ] Run full archival
- [ ] Verify zero data loss
- [ ] Test dry-run mode
- [ ] Verify S3 files correct

**Acceptance Criteria**:
- 100K records archived successfully
- Zero data loss verified
- Dry-run works

---

### Day 5: Documentation

#### Task 5.4: Write README and Documentation
**Owner**: Lead Engineer  
**Duration**: 6 hours  
**Dependencies**: Task 5.1

**Actions**:
- [ ] Update README with quick start
- [ ] Document configuration options
- [ ] Create example configurations
- [ ] Write troubleshooting guide

**Acceptance Criteria**:
- Documentation complete
- Examples work

---

## Week 6: MVP Polish & Validation

### Day 1-2: Performance Testing

#### Task 6.1: Performance Testing
**Owner**: Lead Engineer  
**Duration**: 8 hours  
**Dependencies**: Task 5.1

**Actions**:
- [ ] Test with 100K records
- [ ] Measure throughput
- [ ] Measure database impact
- [ ] Profile bottlenecks
- [ ] Optimize if needed

**Acceptance Criteria**:
- >10K records/minute
- <5% database impact

---

### Day 3: Error Handling Improvements

#### Task 6.2: Improve Error Handling
**Owner**: Backend Engineer  
**Duration**: 4 hours  
**Dependencies**: Task 5.1

**Actions**:
- [ ] Improve error messages
- [ ] Add retry logic for transient failures
- [ ] Add graceful shutdown handling

**Acceptance Criteria**:
- Error messages clear
- Retry logic works

---

### Day 4: Final Testing

#### Task 6.3: Final Test Suite
**Owner**: Backend Engineer  
**Duration**: 6 hours  
**Dependencies**: All tasks

**Actions**:
- [ ] Run full test suite
- [ ] Test on PostgreSQL 11, 12, 14, 16
- [ ] Test with AWS S3 and MinIO

**Acceptance Criteria**:
- All tests pass
- Works on all PostgreSQL versions

---

### Day 5: MVP Review

#### Task 6.4: MVP Review
**Owner**: Lead Engineer  
**Duration**: 4 hours  
**Dependencies**: All tasks

**Actions**:
- [ ] Code review
- [ ] Documentation review
- [ ] Stakeholder demo
- [ ] Plan Phase 2

**Acceptance Criteria**:
- MVP approved
- Phase 2 planned

---

## Phase 1 Success Criteria Checklist

- [ ] Archive 100K records successfully
- [ ] Zero data loss verified through count matching
- [ ] Basic restore works (manual process)
- [ ] >70% test coverage
- [ ] <5% database impact
- [ ] >10K records/minute throughput
- [ ] Documentation complete
- [ ] Code reviewed and approved

---

**END OF PHASE 1 TASK BREAKDOWN**

