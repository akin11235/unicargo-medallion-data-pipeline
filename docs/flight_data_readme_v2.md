# Flight Data Medallion Architecture

A comprehensive data engineering solution for flight analytics using Azure Data Factory, Databricks, and Azure Data Lake Storage with proper dimensional modeling and surrogate keys.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Data Model](#data-model)
- [Implementation Guide](#implementation-guide)
- [Code Examples](#code-examples)
- [Deployment](#deployment)
- [Monitoring](#monitoring)
- [FAQ](#faq)

## Architecture Overview

This solution implements a medallion architecture with three layers:

```
Data Sources → Bronze (Raw) → Silver (Dimensional) → Gold (Analytics) → BI Tools
     ↓             ↓              ↓                    ↓              ↓
   CSV Files → Delta Tables → Fact/Dim Tables → Aggregated Data → Power BI
```

Delta Lake Features

Time Travel: Query historical versions
ACID Transactions: Ensure data consistency
Schema Evolution: Handle changing data structures
Optimize & Z-Order: Improve query performance

Data Lineage

Unity Catalog: Track data lineage and governance
Delta Lake Transaction Logs: Audit data changes
Azure Purview: Data discovery and classification

8. Performance Optimization
   Partitioning Strategy

Bronze: No partitioning (preserve raw structure)
Silver: Partition by flight_date
Gold: Partition by relevant dimensions

7. Security & Governance
   Access Control

Azure AD Integration: Role-based access
Service Principal: ADF to Databricks authentication
Managed Identity: Secure resource access
Key Vault: Store connection strings and secrets

### Technology Stack

- **Orchestration**: Azure Data Factory (ADF)
- **Processing**: Azure Databricks
- **Storage**: Azure Data Lake Storage Gen2 (ADLS)
- **Format**: Delta Lake
- **BI**: Power BI / Tableau

## Data Model

### Source Data Structure

- **Flights**: Fact table with flight details, delays, cancellations
- **Airlines**: Dimension with airline information
- **Airports**: Dimension with airport details and locations

### Dimensional Model (Silver Layer)

#### Fact Table

**FACT_FLIGHT**

```
FLIGHT_SK (PK)              -- Surrogate Key
DATE_SK (FK)                -- References DIM_DATE
AIRLINE_SK (FK)             -- References DIM_AIRLINE
ORIGIN_AIRPORT_SK (FK)      -- References DIM_AIRPORT
DESTINATION_AIRPORT_SK (FK) -- References DIM_AIRPORT
FLIGHT_NUMBER               -- Degenerate Dimension
TAIL_NUMBER                 -- Degenerate Dimension
DEPARTURE_DELAY             -- Measure
ARRIVAL_DELAY               -- Measure
DISTANCE                    -- Measure
... (other measures)
```

#### Dimension Tables

**DIM_AIRLINE**

```
AIRLINE_SK (PK)    -- Surrogate Key
IATA_CODE          -- Natural Key
AIRLINE            -- Airline Name
```

**DIM_AIRPORT**

```
AIRPORT_SK (PK)    -- Surrogate Key
IATA_CODE          -- Natural Key
AIRPORT            -- Airport Name
CITY, STATE, COUNTRY
LATITUDE, LONGITUDE
```

**DIM_DATE**

```
DATE_SK (PK)       -- Surrogate Key
FULL_DATE          -- Actual Date
YEAR, MONTH, DAY
DAY_OF_WEEK, QUARTER
IS_WEEKEND
```

## Prerequisites

### Azure Resources

- Azure Data Factory
- Azure Databricks Workspace
- Azure Data Lake Storage Gen2
- Azure Key Vault (recommended)

### Permissions

- Storage Blob Data Contributor on ADLS
- Databricks workspace access
- ADF pipeline execution rights

## Data Lake Structure

```
/datalake/
├── raw/                    # Bronze Layer - Raw CSV files
│   ├── flights/
│   │   └── year=2024/month=01/day=15/flights.csv
│   ├── airlines/
│   │   └── airlines.csv
│   └── airports/
│       └── airports.csv
├── bronze/                 # Bronze Layer - Delta format
│   ├── flights/
│   ├── airlines/
│   └── airports/
├── silver/                 # Silver Layer - Dimensional model
│   ├── dim_airline/
│   ├── dim_airport/
│   ├── dim_date/
│   └── fact_flight/
│       └── DATE_SK=20240115/
└── gold/                   # Gold Layer - Analytics ready
    ├── flight_summary/
    ├── airline_performance/
    ├── route_analytics/
    └── monthly_trends/
```

## Implementation Guide

### Step 1: Bronze Layer Processing

**Purpose**: Ingest raw CSV files and convert to Delta format

# Databricks Notebook: bronze_ingestion.py

# Read raw CSV files

# Write to Delta format

### Step 2: Silver Layer - Dimensional Modeling

**Purpose**: Create clean dimensional model with surrogate keys

# Databricks Notebook: silver_processing.py

# Read bronze data

# Create dimensions with surrogate keys

# Create fact table with foreign key lookups

# Write with partitioning

### Step 3: Gold Layer - Analytics Tables

**Purpose**: Create business-ready aggregated tables

# Databricks Notebook: gold_analytics.py

# Daily summary with dimensional joins

### Step 4: ADF Orchestration

**Pipeline Structure**:

1. **Copy Activity**: Ingest CSV to raw layer
2. **Databricks Activity**: Bronze processing
3. **Databricks Activity**: Silver processing
4. **Databricks Activity**: Gold analytics
5. **Validation**: Data quality checks

## Code Examples

### Data Quality Validation

```python
def validate_data_quality(df, table_name, required_columns):
    """Validate data quality metrics"""

    # Row count check
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"{table_name}: No data found")

    # Null checks for required columns
    null_counts = {}
    for col_name in required_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percentage = (null_count / row_count) * 100

        if null_percentage > 5:  # Threshold: 5%
            raise ValueError(f"{table_name}.{col_name}: {null_percentage:.2f}% nulls exceed threshold")

        null_counts[col_name] = null_count

    print(f"{table_name} validation passed: {row_count:,} rows, null checks: {null_counts}")
    return True
```

### Incremental Processing Pattern

```python
def process_incremental_flights(date_str):
    """Process flights for specific date incrementally"""

    # Read new data for specific date
    new_flights = spark.read.csv(f"/mnt/datalake/raw/flights/date={date_str}/")

    # Merge with existing data
    from delta.tables import DeltaTable

    existing_table = DeltaTable.forPath(spark, "/mnt/datalake/silver/fact_flight")

    existing_table.alias("existing") \
        .merge(
            new_flights.alias("new"),
            "existing.FLIGHT_SK = new.FLIGHT_SK"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
```

## Deployment Strategy

### Environment Setup

Environment Promotion

Development: Individual feature development
Testing: Integration and UAT
Production: Live data processing

1. **Development**

   ```bash
   # Create resource group
   az group create --name rg-flight-data-dev --location eastus

   # Deploy ARM template
   az deployment group create \
     --resource-group rg-flight-data-dev \
     --template-file infrastructure/main.json \
     --parameters @infrastructure/dev.parameters.json
   ```

2. **Production**
   ```bash
   # Deploy with production parameters
   az deployment group create \
     --resource-group rg-flight-data-prod \
     --template-file infrastructure/main.json \
     --parameters @infrastructure/prod.parameters.json
   ```

### CI/CD Pipeline (Azure DevOps)

Azure DevOps: Pipeline orchestration
ARM Templates: Infrastructure as Code
Git Integration: Version control for notebooks
Automated Testing: Data validation tests

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
      - develop

stages:
  - stage: Build
    jobs:
      - job: ValidateCode
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.8"
          - script: |
              pip install pyspark delta-spark
              python -m py_compile notebooks/*.py
            displayName: "Validate Python notebooks"

  - stage: Deploy
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployToProduction
        environment: production
        strategy:
          runOnce:
            deploy:
              steps:
                - task: AzureCLI@2
                  inputs:
                    azureSubscription: "Production Service Connection"
                    scriptType: "bash"
                    scriptLocation: "inlineScript"
                    inlineScript: |
                      # Deploy ADF pipeline
                      az datafactory pipeline create \
                        --factory-name ${{ variables.adfName }} \
                        --pipeline-name flight-data-pipeline \
                        --pipeline @pipeline/flight-data-pipeline.json
```

## Monitoring & Alerting

### Key Metrics to Monitor

| Metric                | Threshold | Alert Level |
| --------------------- | --------- | ----------- |
| Pipeline Success Rate | < 95%     | Critical    |
| Data Freshness        | > 4 hours | Warning     |
| Row Count Variance    | > 20%     | Warning     |
| Processing Time       | > 2 hours | Warning     |

### Azure Monitor Queries

```kusto
// Pipeline failure rate
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.DATAFACTORY"
| where Status == "Failed"
| summarize FailureCount = count() by bin(TimeGenerated, 1h)
| render timechart

// Data processing volume trends
customEvents
| where name == "DataProcessed"
| extend RowCount = todouble(customMeasurements.RowCount)
| summarize avg(RowCount) by bin(timestamp, 1d)
| render timechart
```

### Data Quality Dashboard

Create Power BI dashboard to monitor:

- Daily data volumes
- Processing success rates
- Data quality scores
- SLA compliance metrics

### Alert Conditions

Pipeline failures
Data quality threshold breaches
Unexpected data volume changes
Processing time anomalies

## FAQ

### Q: Why use surrogate keys instead of natural keys?

**A**: Surrogate keys provide:

- Better performance (integer joins vs string joins)
- Stability when business keys change
- Support for slowly changing dimensions
- Reduced storage in fact tables

### Q: How do you handle late-arriving data?

**A**:

- Use Delta Lake's MERGE operations for upserts
- Implement date-based partitioning for efficient updates
- Set up monitoring for data arrival patterns

### Q: What's the difference between Bronze, Silver, and Gold?

**A**:

- **Bronze**: Raw data as-is, minimal processing
- **Silver**: Clean, validated, dimensional model
- **Gold**: Business-ready, aggregated analytics tables

### Q: How do you ensure data quality?

**A**:

- Automated validation checks at each layer
- Schema enforcement with Delta Lake
- Data profiling and anomaly detection
- Comprehensive logging and monitoring

### Q: How to handle schema evolution?

**A**:

- Use Delta Lake's schema evolution features
- Version control for schema changes
- Backward compatibility testing
- Gradual rollout of schema updates

### Q: What about real-time processing?

**A**: This architecture focuses on batch processing. For real-time:

- Add Azure Event Hubs for streaming ingestion
- Use Databricks Structured Streaming
- Implement lambda architecture pattern

## Additional Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Azure Databricks Best Practices](https://docs.microsoft.com/en-us/azure/databricks/)
- [Azure Data Factory Patterns](https://docs.microsoft.com/en-us/azure/data-factory/)
- [Dimensional Modeling Guide](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests and documentation
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
