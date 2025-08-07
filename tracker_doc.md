# Universal Data Pipeline Tracking Framework

## Table of Contents
- [Problem Statement](#problem-statement)
- [Solution Overview](#solution-overview)
- [Core Design Principles](#core-design-principles)
- [Configuration Schema](#configuration-schema)
- [Implementation Architecture](#implementation-architecture)
- [Usage Examples](#usage-examples)
- [Benefits for Downstream Teams](#benefits-for-downstream-teams)
- [Advanced Scenarios](#advanced-scenarios)
- [Getting Started](#getting-started)

## Problem Statement

Modern data teams face critical challenges in managing complex multi-stage data pipelines:

### Current Pain Points
- **No Centralized Visibility**: Teams can't see which pipeline stages are running, pending, or failed
- **Debugging Nightmares**: When pipelines break, engineers spend hours identifying which stage failed and why
- **Poor Coordination**: Downstream teams don't know if their source data is ready for processing
- **Manual Audit Processes**: Stage-to-stage data validation is error-prone and time-consuming
- **Resource Conflicts**: Multiple transfers run simultaneously without coordination, causing system overload
- **Accountability Gaps**: No clear ownership when pipeline stages fail
- **Inflexible Systems**: Hardcoded pipeline logic that breaks when business requirements change

### Business Impact
- Delayed reporting and analytics
- Increased operational costs due to manual intervention
- Data quality issues reaching production systems
- Poor resource utilization and system performance
- Difficulty meeting SLA commitments to downstream consumers

## Solution Overview

A **configuration-driven, storage-agnostic pipeline tracking framework** that provides:

1. **Centralized Metadata Management**: Single source of truth for all pipeline state
2. **Flexible Audit Framework**: Configurable validation between any pipeline stages
3. **Storage Independence**: Works with Snowflake, MongoDB, PostgreSQL, S3, or any storage system
4. **User-Defined Logic**: Framework provides structure, users provide business logic
5. **Operational Intelligence**: Real-time visibility and automated alerting

## Core Design Principles

### 1. Configuration Over Code
- Pipeline behavior defined in YAML/JSON files
- No hardcoded business logic in the framework
- Easy to modify without touching application code

### 2. User-Defined Content
- Framework provides column structure
- Users fill values using their own business logic and functions
- Maximum flexibility for diverse organizational needs

### 3. Storage Agnostic
- Pluggable storage adapters for any backend
- Unified API regardless of underlying technology
- Easy migration between storage systems

### 4. Robustness First
- Handle partial failures gracefully
- Support complex dependency management
- Built-in retry and recovery mechanisms

## Configuration Schema

### Pipeline Metadata
```yaml
pipeline_metadata:
  pipeline_id: ""              # User-defined unique pipeline identifier
  pipeline_name: ""            # Human-readable pipeline name
  pipeline_description: ""     # Optional description
  pipeline_version: ""         # User-defined versioning
  created_by: ""              # Pipeline creator
  created_timestamp: ""       # Creation time
```

### Stage Transfer Configuration
Each row in the tracking system represents one stage-to-stage transfer:

```yaml
stage_transfers:
  - # Stage Identification (User-Defined Content)
    stage_i_details: ""                    # User-defined format for source stage
    stage_i_unique_identifier: ""          # User-defined unique ID generation logic
    stage_i_plus_1_details: ""             # User-defined format for target stage  
    stage_i_plus_1_unique_identifier: ""   # User-defined unique ID generation logic
    
    # Transfer Control
    transfer_order_number: 1.0             # Float for execution sequence
    transfer_enabled: true                 # Quick enable/disable toggle
    transfer_status: "pending"             # [pending, in_progress, completed, failed]
    
    # Timing & Performance
    estimated_duration: "1d12h30m"         # User-defined human-readable format
    transfer_start_timestamp: null         # Actual start time
    transfer_end_timestamp: null           # Actual end time  
    actual_duration: null                  # Calculated actual duration
    
    # Data Handling Strategy
    can_partial_data_be_collected: true    # Allow incomplete transfers
    can_process_be_parallelized: true      # Enable parallel execution
    can_access_historical_data: true       # Historical data availability
    upstream_completion_required: false    # Wait for upstream completion
    dependent_on_previous_transfer: true   # Dependency on prior stage
    
    # Retry & Recovery
    retry_attempt_number: 0                # Current retry count
    max_retry_attempts: 3                  # User-defined retry limit
    
    # Audit Configuration
    audit_enabled: true                    # Enable/disable auditing
    audit_result: null                     # [matched, mismatched, pending]
    audit_metadata: {}                     # User-defined audit details
    
    # Operational Management
    transfer_process_owner_name: ""        # Responsible person
    transfer_process_email_alerts: []      # Notification email list
    
    # Tracking System Storage (Where to store THIS tracking metadata)
    tracking_location: []                  # Fallback storage locations for tracking data
    transfer_unique_id: ""                 # User-defined unique transfer ID
```

### User Flexibility Examples

The framework provides column structure, users provide content:

```yaml
# Example 1: Simple Naming
stage_i_details: "customer_raw_data"

# Example 2: Hierarchical Structure  
stage_i_details: "customer||demographics||daily_snapshot"

# Example 3: Function-Based
stage_i_details: get_stage_description(env="prod", type="customer", date="2025-08-07")

# Example 4: JSON-like
stage_i_details: "{source:salesforce,category:leads,env:production}"

# Example 5: Custom Business Logic
stage_i_details: f"{business_unit}_{data_domain}_{processing_frequency}"
```

### Tracking System Storage Strategy

**CRITICAL DISTINCTION**: `tracking_location` specifies where to store **the tracking metadata itself**, not where the pipeline data lives.

#### Fallback Storage Configuration
The tracking system needs resilient storage to ensure metadata is never lost:

```yaml
tracking_location: [
  "snowflake||prod_db||pipeline_schema||tracking_table",     # Primary storage
  "aws_s3||s3://company-pipeline-logs/tracking/",            # Backup storage  
  "local_postgres||pipeline_db||public||tracking_table",     # Local fallback
  "local_file||/opt/pipeline/logs/tracking.json"             # Emergency fallback
]
```

#### Storage Location Format Examples
```yaml
# Snowflake
"snowflake||database_name||schema_name||table_name"

# AWS S3
"aws_s3||s3://bucket-name/path/to/tracking/files/"

# PostgreSQL
"postgres||database_name||schema_name||table_name"

# MongoDB  
"mongodb||database_name||collection_name"

# Local File System
"local_file||/complete/path/to/tracking.json"

# Any format user defines
"custom_storage||user_defined_connection_string"
```

#### Fallback Logic
1. **Primary**: Try to store tracking record in first location (Snowflake)
2. **Backup**: If primary fails, automatically attempt second location (S3)
3. **Emergency**: Continue down the list until successful storage
4. **Guarantee**: Tracking metadata is never lost, even during infrastructure failures

#### User Implementation Required
```python
def connect_to_tracking_storage(location_string: str):
    """User implements connection logic for each storage type"""
    storage_type, connection_details = parse_location_string(location_string)
    
    if storage_type == "snowflake":
        db, schema, table = connection_details.split("||")
        # User provides Snowflake connection logic
        return snowflake_connection
    
    elif storage_type == "aws_s3":
        s3_uri = connection_details  
        # User provides S3 connection logic
        return s3_connection
        
    elif storage_type == "local_postgres":
        db, schema, table = connection_details.split("||")
        # User provides PostgreSQL connection logic  
        return postgres_connection
        
    # User can add any storage type they need
```

This ensures the **tracking system itself** remains operational even when primary infrastructure fails.

### Core Components

#### 1. Configuration Manager
- Validates YAML/JSON configuration files
- Provides schema validation and error reporting
- Manages configuration versioning

#### 2. Pipeline Orchestrator
- Executes transfers based on configuration
- Manages dependencies and execution order
- Handles retry logic and failure recovery

#### 3. Storage Adapter Interface
```python
class StorageAdapter:
    def write_tracking_record(self, record: dict) -> bool:
        """User implements for their storage backend"""
        pass
    
    def read_tracking_records(self, pipeline_id: str) -> list:
        """User implements for their storage backend"""
        pass
    
    def update_tracking_record(self, transfer_id: str, updates: dict) -> bool:
        """User implements for their storage backend"""
        pass
```

#### 4. Audit Engine
- Executes user-defined audit functions
- Stores audit results in audit_metadata
- Supports any audit strategy (row counts, checksums, business rules)

#### 5. Tracking Metadata Store Manager
- Abstracts **tracking data persistence** (not pipeline data)
- Handles concurrent access to tracking records
- Manages **tracking storage fallback** when primary systems fail
- Ensures tracking metadata is never lost

### User Implementation Points

Users provide custom functions for:

```python
# Stage identification
def generate_stage_details(stage_info: dict) -> str:
    """User defines how to format stage details"""
    pass

def generate_stage_unique_id(stage_details: str, context: dict) -> str:
    """User defines unique ID generation logic"""
    pass

# Transfer identification  
def generate_transfer_unique_id(transfer_info: dict) -> str:
    """User defines transfer ID generation logic"""
    pass

# Storage operations (for tracking metadata)
def tracking_storage_write(record: dict, location: str) -> bool:
    """User implements storage connection for tracking data"""
    pass

def tracking_storage_read(query: dict, location: str) -> list:
    """User implements storage retrieval for tracking data"""
    pass

# Audit operations
def execute_audit(source_stage: str, target_stage: str, audit_config: dict) -> dict:
    """User defines audit logic and returns results"""
    pass
```

## Usage Examples

### Example 1: Simple ETL Pipeline
```yaml
pipeline_metadata:
  pipeline_id: "daily_customer_etl_20250807_001"
  pipeline_name: "Daily Customer Data ETL"
  
stage_transfers:
  - stage_i_details: "salesforce_raw"
    stage_i_plus_1_details: "customer_staging"
    transfer_order_number: 1.0
    estimated_duration: "30m"
    can_partial_data_be_collected: false
    upstream_completion_required: true
    audit_enabled: true
    transfer_process_owner_name: "navneeth"
    transfer_process_email_alerts: ["data-team@company.com"]
    
  - stage_i_details: "customer_staging" 
    stage_i_plus_1_details: "customer_production"
    transfer_order_number: 2.0
    estimated_duration: "45m"
    can_partial_data_be_collected: true
    upstream_completion_required: true
    audit_enabled: true
    transfer_process_owner_name: "analytics_team"
    transfer_process_email_alerts: ["analytics@company.com", "ops@company.com"]
```

### Example 2: Real-time Streaming Pipeline
```yaml
stage_transfers:
  - stage_i_details: "kafka_stream_topic_orders"
    stage_i_plus_1_details: "orders_buffer"
    transfer_order_number: 1.0
    can_partial_data_be_collected: true
    upstream_completion_required: false    # Stream processing
    can_process_be_parallelized: true
    estimated_duration: "continuous"
```

## Benefits for Downstream Teams

### Data Engineers
- **Real-time Pipeline Status**: Know exactly which transfers are running, pending, or failed
- **Dependency Awareness**: Understand upstream requirements before starting work
- **Performance Optimization**: Track actual vs estimated durations to improve planning
- **Failure Recovery**: Quick identification of failed stages and retry status

### Analytics Teams
- **Data Readiness Indicators**: Check if required data is available before running reports
- **Quality Assurance**: View audit results to understand data completeness and accuracy
- **Historical Data Access**: Know which datasets support historical analysis
- **Partial Data Decisions**: Understand if they can proceed with incomplete datasets

### Operations Teams
- **SLA Monitoring**: Track performance against estimated durations
- **Resource Planning**: Monitor parallel processing capabilities and constraints
- **Incident Response**: Automated alerting with clear ownership information
- **Capacity Management**: Understand which stages can be parallelized

### Business Stakeholders
- **Process Transparency**: Clear visibility into data pipeline health
- **Impact Assessment**: Understand how pipeline issues affect business reporting
- **Risk Management**: Proactive identification of data quality issues
- **Compliance Reporting**: Automated audit trails for regulatory requirements

## Advanced Scenarios

### Scenario 1: Pipeline Schema Changes
```
Current: source → stage1 → stage2 → target
New Requirement: source → stage1 → new_stage → stage2 → target
```
**Solution**: Create new pipeline with new `pipeline_id` - old and new pipelines tracked separately.

### Scenario 2: Partial Failure Handling
```
Transfer Status: 700K records successful, 300K failed
```
**Decision Matrix:**
- `can_partial_data_be_collected = true` → Continue with 700K records
- `can_partial_data_be_collected = false` → Rollback and retry all 1M records

### Scenario 3: Complex Dependencies
```
stage1 → stage3 (bypassing stage2)
stage2 → stage3 (parallel path)
```
**Handling**: `transfer_order_number` and `dependent_on_previous_transfer` manage complex flows.

### Scenario 4: Multiple Storage Backends
```yaml
tracking_location: ["snowflake.pipeline_db.tracking_table", "s3://bucket/pipeline-logs/", "mongodb.pipelines.tracking"]
```
**Implementation**: User's storage adapter handles multiple destinations simultaneously.

## Getting Started

### Step 1: Define Your Pipeline Configuration
Create a YAML file with your pipeline structure using our schema.

### Step 2: Implement User Functions
Provide implementations for:
- Stage detail formatting functions
- Unique ID generation logic  
- Storage adapter for your backend
- Audit functions for data validation

### Step 3: Initialize Tracking System
Load configuration and start pipeline execution with tracking enabled.

### Step 4: Monitor and Operate
Use tracking data for monitoring, alerting, and operational decision-making.

## Technical Implementation Notes

### Storage Table Structure (Example: Snowflake)
```sql
CREATE TABLE pipeline_tracking (
  -- Pipeline Context
  pipeline_id VARCHAR NOT NULL,
  
  -- Stage Information (User-Defined Content)
  stage_i_details VARCHAR,
  stage_i_unique_identifier VARCHAR,
  stage_i_plus_1_details VARCHAR,
  stage_i_plus_1_unique_identifier VARCHAR,
  
  -- Transfer Control
  transfer_order_number FLOAT,
  transfer_enabled BOOLEAN,
  transfer_status VARCHAR,
  
  -- Timing & Performance
  estimated_duration VARCHAR,
  transfer_start_timestamp TIMESTAMP,
  transfer_end_timestamp TIMESTAMP,
  actual_duration VARCHAR,
  
  -- Data Strategy
  can_partial_data_be_collected BOOLEAN,
  can_process_be_parallelized BOOLEAN,
  can_access_historical_data BOOLEAN,
  upstream_completion_required BOOLEAN,
  dependent_on_previous_transfer BOOLEAN,
  
  -- Retry & Recovery
  retry_attempt_number INTEGER DEFAULT 0,
  max_retry_attempts INTEGER DEFAULT 3,
  
  -- Audit Configuration
  audit_enabled BOOLEAN,
  audit_result VARCHAR,
  audit_metadata VARIANT,  -- JSON in Snowflake
  
  -- Operational Management
  transfer_process_owner_name VARCHAR,
  transfer_process_email_alerts ARRAY,
  
  -- Infrastructure
  tracking_location ARRAY,
  transfer_unique_id VARCHAR,
  
  -- System Fields
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Key Queries for Operations

```sql
-- Monitor current pipeline execution
SELECT 
  stage_i_details,
  stage_i_plus_1_details,
  transfer_status,
  actual_duration,
  retry_attempt_number,
  transfer_process_owner_name
FROM pipeline_tracking 
WHERE pipeline_id = 'current_run_id'
ORDER BY transfer_order_number;

-- Find problematic transfers across all runs
SELECT 
  stage_i_details,
  stage_i_plus_1_details,
  COUNT(*) as total_attempts,
  AVG(retry_attempt_number) as avg_retries
FROM pipeline_tracking 
GROUP BY stage_i_details, stage_i_plus_1_details
HAVING AVG(retry_attempt_number) > 1;

-- Get latest status for each transfer
SELECT * FROM pipeline_tracking p1
WHERE p1.retry_attempt_number = (
  SELECT MAX(p2.retry_attempt_number)
  FROM pipeline_tracking p2
  WHERE p1.pipeline_id = p2.pipeline_id 
    AND p1.transfer_unique_id = p2.transfer_unique_id
);
```

## Framework vs User Responsibilities

### Framework Provides
- **Column Structure**: Predefined tracking schema
- **Orchestration Logic**: Pipeline execution engine
- **Storage Interface**: Abstract storage adapter pattern
- **Validation Framework**: Configuration schema validation
- **Monitoring Tools**: Query templates and dashboard examples

### User Provides
- **Configuration Values**: All actual data for each column
- **Business Logic Functions**: Custom implementations for ID generation, stage formatting
- **Storage Implementation**: Adapter for their specific storage backend
- **Audit Logic**: Custom validation functions between stages
- **Operational Procedures**: Alert handling, escalation processes

### Clear Separation of Concerns
```python
# Framework provides the structure
def track_pipeline_transfer(config: dict):
    # Framework orchestration logic
    record = {
        'pipeline_id': config['pipeline_id'],
        'stage_i_details': user_generate_stage_details(),      # User implements
        'stage_i_unique_identifier': user_generate_stage_id(), # User implements  
        'transfer_unique_id': user_generate_transfer_id(),     # User implements
        # ... other framework-managed fields
    }
    user_storage_adapter.write(record)  # User implements

# User implements all content generation
def user_generate_stage_details():
    # User's business logic here
    return f"{environment}||{data_type}||{date}"

def user_generate_stage_id():
    # User's ID generation logic here  
    return hashlib.md5(stage_details + timestamp).hexdigest()
```

## Contributing

This framework is designed to be:
- **Technology Agnostic**: Works with any storage backend
- **Industry Neutral**: Applicable across different business domains
- **Scalable**: Handles simple 3-stage pipelines to complex 50+ stage workflows
- **Extensible**: Easy to add new columns or functionality

### Extension Points
- Custom audit strategies
- Additional metadata fields
- New storage adapters
- Enhanced monitoring capabilities
- Integration with existing workflow tools

## License
Open source - community contributions welcome.

## Authors
gummala navneeth

---

*This framework prioritizes flexibility and user control while providing robust pipeline tracking capabilities for any data infrastructure.*