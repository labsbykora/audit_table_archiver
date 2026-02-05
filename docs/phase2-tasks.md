# Phase 2: Production Hardening - Task Breakdown

**Duration**: 3-4 weeks  
**Goal**: Multi-database support, reliability, and production-grade error handling

## Week 7: Multi-Database Support

### Day 1-2: Database Configuration & Connection Pool Isolation

**Status**: ✅ Partially Complete
- ✅ Configuration already supports multiple databases
- ✅ Per-database connection pools already implemented
- ⚠️ Need to verify proper isolation and cleanup

**Tasks**:
- [ ] Review and verify connection pool isolation
- [ ] Add connection pool size configuration per database
- [ ] Ensure proper cleanup of connection pools on failure
- [ ] Add unit tests for multi-database configuration
- [ ] Add integration tests for multiple databases

### Day 3: Sequential Processing Enhancement

**Status**: ✅ Complete
- ✅ Sequential processing already implemented
- ✅ Error isolation already in place
- ⚠️ Need to improve error reporting and logging

**Tasks**:
- [ ] Enhance error reporting with database context
- [ ] Add detailed statistics per database
- [ ] Improve logging for multi-database scenarios
- [ ] Add integration tests for database failure isolation

### Day 4-5: Parallel Database Processing (Optional)

**Status**: ❌ Not Implemented

**Tasks**:
- [ ] Add configuration option for parallel processing
- [ ] Implement concurrency limit (default: 2-3 databases)
- [ ] Add resource monitoring (memory, connections)
- [ ] Implement graceful shutdown handling
- [ ] Add integration tests for parallel processing
- [ ] Document parallel processing trade-offs

## Week 8: Advanced Verification & Checksums

### Day 1-2: Checksum Implementation

**Tasks**:
- [ ] Implement SHA-256 checksum calculation
- [ ] Calculate checksum before compression
- [ ] Store checksum in metadata file
- [ ] Add checksum verification during restore
- [ ] Write unit tests for checksum calculation
- [ ] Write integration tests for checksum verification

### Day 3: Primary Key Verification

**Tasks**:
- [ ] Extract primary keys from fetched records
- [ ] Verify PKs in S3 match delete statement
- [ ] Create deletion manifest (JSON file)
- [ ] Store manifest in S3 alongside data file
- [ ] Write tests for PK verification
- [ ] Write tests for manifest generation

### Day 4: Sample Verification

**Tasks**:
- [ ] Implement random sample selection (1% min 10, max 1000)
- [ ] Download samples from S3
- [ ] Verify samples not in database (deleted)
- [ ] Log verification results
- [ ] Write tests for sample selection
- [ ] Write tests for sample verification

### Day 5: Metadata Files

**Tasks**:
- [ ] Create metadata file structure (JSON)
- [ ] Include schema, counts, checksums, timestamps
- [ ] Generate metadata per batch
- [ ] Store alongside data file in S3
- [ ] Write tests for metadata generation
- [ ] Write tests for metadata validation

## Week 9: Schema Management & Watermarks

### Day 1-2: Schema Detection

**Tasks**:
- [ ] Query information_schema for table structure
- [ ] Detect column types, constraints, indexes
- [ ] Store schema in metadata
- [ ] Write unit tests for schema detection
- [ ] Write integration tests

### Day 3: Schema Drift Detection

**Tasks**:
- [ ] Compare current schema with previous (from metadata)
- [ ] Detect column additions/removals
- [ ] Detect type changes
- [ ] Log warnings/errors
- [ ] Write tests for drift detection
- [ ] Add configuration for drift handling (warn/fail)

### Day 4-5: Watermark Management

**Tasks**:
- [ ] Implement watermark storage (S3 or database)
- [ ] Store last archived timestamp + PK
- [ ] Update watermark after successful batch
- [ ] Use watermark for incremental archival (skip already archived)
- [ ] Add watermark integrity verification
- [ ] Write tests for watermark storage
- [ ] Write tests for incremental archival

## Week 10: Distributed Locking & Checkpointing

### Day 1-2: Distributed Locking

**Tasks**:
- [ ] Implement Redis backend for locking
- [ ] Implement PostgreSQL advisory locks backend
- [ ] Implement file-based backend (dev/testing)
- [ ] Add lock heartbeat mechanism (30s)
- [ ] Add stale lock detection and cleanup
- [ ] Write unit tests for each backend
- [ ] Write integration tests for concurrent run prevention

### Day 3: Checkpoint System

**Tasks**:
- [ ] Create checkpoint data structure (JSON)
- [ ] Save checkpoint every N batches (default: 10)
- [ ] Store checkpoint in S3 or local file
- [ ] Implement checkpoint loading
- [ ] Write tests for checkpoint save/load
- [ ] Write tests for checkpoint recovery

### Day 4: Resume Capability

**Tasks**:
- [ ] Detect interrupted runs (checkpoint exists)
- [ ] Load checkpoint on startup
- [ ] Resume from last completed batch
- [ ] Clean up orphaned multipart uploads
- [ ] Write integration tests for resume
- [ ] Test resume after various failure scenarios

### Day 5: Testing & Documentation

**Tasks**:
- [ ] Test lock acquisition/release
- [ ] Test concurrent run prevention
- [ ] Test resume from checkpoint
- [ ] Update documentation
- [ ] Create runbook for lock issues
- [ ] Create runbook for checkpoint recovery

## Phase 2 Success Criteria

✅ Archive from 5+ databases, 20+ tables  
✅ Handle network failures gracefully (resume uploads)  
✅ Pass chaos testing (network drops, DB disconnects)  
✅ Zero data loss in all test scenarios  
✅ 99.9% success rate  
✅ Checkpoint/resume working  

