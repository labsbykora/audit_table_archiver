# Core Tool Improvements Plan

## Overview

This document outlines improvements for reliability, error handling, performance optimization, and documentation for the core archiver tool.

## 1. Reliability & Error Handling

### Current State Analysis

**Strengths:**
- ✅ Comprehensive exception hierarchy (`ArchiverError` with context)
- ✅ Transaction safety with rollback on failure
- ✅ Retry logic with exponential backoff
- ✅ Circuit breaker for S3 operations
- ✅ Lock management to prevent concurrent runs
- ✅ Checkpoint/resume capability

**Gaps Identified:**
1. **Partial Failure Recovery**: If a batch fails mid-table, entire table archival fails
2. **Network Interruption**: Limited handling for transient network failures during uploads
3. **Database Connection Loss**: No automatic reconnection logic
4. **S3 Partial Uploads**: No cleanup of partial multipart uploads on failure
5. **Error Context**: Some errors lack sufficient context for debugging
6. **Graceful Degradation**: Limited fallback strategies when components fail

### Improvements Needed

#### 1.1 Batch-Level Error Recovery
- **Current**: Table-level error handling - one failed batch fails entire table
- **Improvement**: Implement batch-level retry with skip-on-failure option
- **Impact**: Higher success rate, better resilience

#### 1.2 Connection Resilience
- **Current**: Connection pool errors propagate immediately
- **Improvement**: Automatic reconnection with exponential backoff
- **Impact**: Better handling of transient database issues

#### 1.3 S3 Upload Recovery
- **Current**: Failed uploads require full retry
- **Improvement**: Resume partial uploads, cleanup orphaned multipart uploads
- **Impact**: Faster recovery, reduced S3 costs

#### 1.4 Enhanced Error Context
- **Current**: Some errors lack correlation IDs and context
- **Improvement**: Add correlation IDs to all operations, enrich error context
- **Impact**: Better debugging and troubleshooting

#### 1.5 Graceful Degradation
- **Current**: Component failures cause complete failure
- **Improvement**: Optional components (notifications, metrics) shouldn't block archival
- **Impact**: Higher availability

## 2. Performance Optimization

### Current State Analysis

**Strengths:**
- ✅ Batch processing with configurable sizes
- ✅ Connection pooling
- ✅ Parallel database processing
- ✅ Multipart uploads for large files
- ✅ Compression to reduce upload size

**Bottlenecks Identified:**
1. **Sequential Batch Processing**: Batches processed one at a time
2. **Synchronous Uploads**: Upload blocks next batch processing
3. **Query Optimization**: No query plan analysis or optimization hints
4. **Memory Usage**: All batch data held in memory until upload completes
5. **Compression**: Single-threaded gzip compression
6. **Verification**: Sequential verification steps

### Improvements Needed

#### 2.1 Parallel Batch Processing
- **Current**: Process batches sequentially
- **Improvement**: Process multiple batches in parallel (pipeline: fetch → serialize → compress → upload)
- **Impact**: 30-50% faster archival for large tables

#### 2.2 Async Upload Pipeline
- **Current**: Upload blocks batch processing
- **Improvement**: Upload batches asynchronously while processing next batch
- **Impact**: 20-40% faster archival

#### 2.3 Query Optimization
- **Current**: Basic queries with no optimization hints
- **Improvement**: 
  - Use `EXPLAIN ANALYZE` to detect slow queries
  - Add query hints for better plans
  - Use covering indexes when possible
- **Impact**: 10-30% faster queries

#### 2.4 Streaming Compression
- **Current**: Compress entire batch in memory
- **Improvement**: Stream compression during serialization
- **Impact**: Lower memory usage, faster processing

#### 2.5 Parallel Compression
- **Current**: Single-threaded gzip
- **Improvement**: Use parallel compression (pigz) when available
- **Impact**: 2-4x faster compression for large batches

#### 2.6 Optimized Verification
- **Current**: Sequential verification steps
- **Improvement**: Parallel verification where possible
- **Impact**: Faster verification, less blocking

## 3. Documentation

### Current State Analysis

**Strengths:**
- ✅ Comprehensive troubleshooting guide
- ✅ Performance tuning guide
- ✅ Architecture documentation
- ✅ Configuration reference

**Gaps Identified:**
1. **Error Recovery Guide**: No guide for recovering from specific error scenarios
2. **Performance Benchmarks**: Limited benchmark data
3. **Best Practices**: Scattered across multiple documents
4. **API Documentation**: Limited inline documentation
5. **Operational Runbooks**: Missing runbooks for common operations

### Improvements Needed

#### 3.1 Error Recovery Guide
- Create comprehensive guide for recovering from common errors
- Include step-by-step recovery procedures
- Add decision trees for error resolution

#### 3.2 Performance Benchmarks
- Add detailed benchmark results for different scenarios
- Include performance characteristics by table size
- Add optimization case studies

#### 3.3 Best Practices Guide
- Consolidate best practices into single document
- Add production deployment patterns
- Include monitoring and alerting recommendations

#### 3.4 Enhanced API Documentation
- Improve docstrings with examples
- Add type hints where missing
- Document error conditions and recovery

#### 3.5 Operational Runbooks
- Create runbooks for common operations:
  - Recovering from failed archival
  - Handling partial uploads
  - Managing stuck locks
  - Performance troubleshooting

## Implementation Priority

### Phase 1: Critical Reliability (Week 1)
1. Batch-level error recovery
2. Connection resilience
3. Enhanced error context
4. Error recovery guide

### Phase 2: Performance (Week 2)
1. Parallel batch processing
2. Async upload pipeline
3. Query optimization
4. Performance benchmarks

### Phase 3: Documentation & Polish (Week 3)
1. Best practices guide
2. Operational runbooks
3. Enhanced API documentation
4. Streaming compression (if time permits)

## Success Metrics

### Reliability
- **Target**: 99.9% batch success rate (up from ~95%)
- **Target**: < 1% operations requiring manual intervention
- **Target**: Automatic recovery from 90% of transient failures

### Performance
- **Target**: 30-50% faster archival for large tables (>1M rows)
- **Target**: 20-30% reduction in memory usage
- **Target**: < 5% database performance impact (maintain current)

### Documentation
- **Target**: 100% API coverage with examples
- **Target**: Complete runbooks for top 10 operations
- **Target**: Performance benchmarks for 5 common scenarios

