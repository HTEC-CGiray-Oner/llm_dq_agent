# Multi-Connector Data Quality Agent - Feature Summary

## ✅ What's Been Added

### 1. **Connector Architecture** (`src/connectors/`)

A flexible, extensible connector system that supports multiple data sources:

#### **Base Classes:**
- `BaseConnector` - Abstract base class defining connector interface
- `ConnectorFactory` - Factory pattern for creating connectors dynamically

#### **Implemented Connectors:**
- **SnowflakeConnector** - Connect to Snowflake data warehouse
- **PostgresConnector** - Connect to PostgreSQL databases
- **CSVConnector** - Read local CSV files

#### **Key Features:**
- Context manager support (`with connector:`)
- Automatic connection cleanup
- Connection testing capabilities
- Configuration loading from `.env` and `settings.yaml`
- Extensible design for adding new connectors

---

### 2. **Configuration System**

#### **`config/settings.yaml`:**
```yaml
default_connector: csv  # Default data source
connectors:
  snowflake: {...}
  postgres: {...}
  csv:
    base_path: ./data
```

#### **`.env` file:**
Environment variables for sensitive credentials:
- Snowflake: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, etc.
- PostgreSQL: `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, etc.
- CSV: `CSV_BASE_PATH`

---

### 3. **Updated Data Quality Functions**

#### **`load_data_by_id()` enhancements:**
- Now accepts `connector_type` parameter
- Automatically selects connector based on settings
- Falls back to dummy data if connection fails
- Supports all connector-specific parameters

#### **`check_dataset_duplicates()` enhancements:**
- Added `connector_type` parameter
- Updated docstring with examples
- Works with any configured connector

---

### 4. **Intelligent Agent Integration**

#### **Updated system prompt** in `planner.py`:
The agent can now:
- **Extract connector type** from natural language:
  - "check Snowflake table CUSTOMERS" → `connector_type='snowflake'`
  - "analyze postgres users table" → `connector_type='postgres'`
  - "check CSV sales.csv" → `connector_type='csv'`

- **Use default connector** if not specified:
  - "check table orders" → uses default from `settings.yaml`

---

### 5. **Dependencies & Installation**

#### **Updated `pyproject.toml`:**
```toml
# Optional connector dependencies
snowflake-connector-python = {version = "^3.7.0", optional = true}
psycopg2-binary = {version = "^2.9.9", optional = true}

[tool.poetry.extras]
snowflake = ["snowflake-connector-python"]
postgres = ["psycopg2-binary"]
all-connectors = ["snowflake-connector-python", "psycopg2-binary"]
```

#### **Installation commands:**
```bash
# Base installation
poetry install

# With Snowflake support
poetry install -E snowflake

# With PostgreSQL support
poetry install -E postgres

# With all connectors
poetry install -E all-connectors
```

---

### 6. **Testing Infrastructure**

#### **`tests/test_connectors.py`:**
Comprehensive test suite that:
- Tests CSV connector functionality
- Tests data quality check integration
- Validates data loading and duplicate detection
- Can be extended to test Snowflake/PostgreSQL when configured

#### **Sample data:**
- `data/sample_data.csv` - Test CSV with intentional duplicates

---

### 7. **Documentation**

#### **`docs/CONNECTORS.md`:**
Complete guide covering:
- Installation instructions
- Configuration examples
- Usage patterns for each connector
- Agent integration examples
- Troubleshooting guide
- How to add new connectors

---

## 🎯 Usage Examples

### Example 1: Direct Connector Usage
```python
from src.connectors.connector_factory import ConnectorFactory

connector = ConnectorFactory.create_connector('snowflake')
with connector:
    df = connector.load_data('SCHEMA.TABLE')
    print(df.head())
```

### Example 2: Data Quality Check with Specific Connector
```python
from src.data_quality.checks import check_dataset_duplicates

result = check_dataset_duplicates('MY_TABLE', connector_type='snowflake')
print(result)  # {'dataset_id': 'MY_TABLE', 'duplicate_qty': 5, 'status': 'failure'}
```

### Example 3: Natural Language Agent Query
```python
from src.agent.planner import run_dq_check

# Agent extracts: connector_type='snowflake', dataset_id='CUSTOMERS'
query = "Check for duplicate rows in Snowflake CUSTOMERS table"
result = run_dq_check(query)
```

### Example 4: Using Default Connector
```python
# Configure default_connector: snowflake in settings.yaml
from src.data_quality.checks import check_dataset_duplicates

# Will use Snowflake automatically
result = check_dataset_duplicates('MY_TABLE')
```

---

## 🔧 Configuration Quick Start

### For Snowflake:

1. **Install dependency:**
   ```bash
   poetry install -E snowflake
   ```

2. **Configure `.env`:**
   ```bash
   SNOWFLAKE_ACCOUNT=myaccount.us-east-1
   SNOWFLAKE_USER=myuser
   SNOWFLAKE_PASSWORD=mypassword
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=MY_DB
   SNOWFLAKE_SCHEMA=PUBLIC
   ```

3. **Update `settings.yaml`:**
   ```yaml
   default_connector: snowflake
   ```

4. **Use it:**
   ```python
   from src.agent.planner import run_dq_check

   result = run_dq_check("Check duplicates in ORDERS table")
   ```

---

## 📊 Architecture Flow

```
User Query: "Check duplicates in Snowflake CUSTOMERS table"
     ↓
Agent (planner.py)
  - Extracts: dataset_id="CUSTOMERS", connector_type="snowflake"
     ↓
check_dataset_duplicates(dataset_id, connector_type)
     ↓
load_data_by_id(dataset_id, connector_type)
     ↓
ConnectorFactory.create_connector('snowflake')
     ↓
SnowflakeConnector
  - Loads config from .env
  - Connects to Snowflake
  - Executes: SELECT * FROM CUSTOMERS
  - Returns DataFrame
     ↓
Duplicate Detection Logic
     ↓
Result: {"dataset_id": "CUSTOMERS", "duplicate_qty": X, "status": "..."}
```

---

## 🎉 Key Benefits

1. **Flexibility**: Support multiple data sources without changing agent code
2. **Extensibility**: Easy to add new connectors (MySQL, MongoDB, S3, etc.)
3. **Intelligence**: Agent automatically detects data source from natural language
4. **Configuration**: Centralized config via `.env` and `settings.yaml`
5. **Safety**: Automatic connection cleanup via context managers
6. **Fallback**: Falls back to dummy data if connection fails (for dev/test)

---

## 🚀 Next Steps

1. **Install connector dependencies** for your data sources
2. **Configure credentials** in `.env`
3. **Set default connector** in `settings.yaml`
4. **Test connection** using `tests/test_connectors.py`
5. **Run agent** with natural language queries!

---

## 📝 Files Modified/Created

**New Files:**
- `src/connectors/__init__.py`
- `src/connectors/base_connector.py`
- `src/connectors/connector_factory.py`
- `src/connectors/snowflake_connector.py`
- `src/connectors/postgres_connector.py`
- `src/connectors/csv_connector.py`
- `tests/test_connectors.py`
- `data/sample_data.csv`
- `docs/CONNECTORS.md`
- `docs/FEATURE_SUMMARY.md` (this file)

**Modified Files:**
- `pyproject.toml` - Added connector dependencies
- `config/settings.yaml` - Added connector configuration
- `.env` - Added connector environment variables
- `src/data_quality/checks.py` - Updated to use connectors
- `src/agent/planner.py` - Enhanced agent prompt for connector detection

---

## 💡 Example Queries the Agent Can Now Handle

```python
# Snowflake queries
"Check for duplicates in Snowflake SALES_DATA table"
"Analyze Snowflake PUBLIC.CUSTOMERS for duplicate rows"

# PostgreSQL queries
"Check postgres users table for duplicates"
"Analyze PostgreSQL database table orders"

# CSV queries
"Check CSV file sales_2024.csv for duplicates"
"Analyze data in customers.csv"

# Default connector queries
"Check ORDERS table for duplicates"
"Are there any duplicate rows in TRANSACTIONS?"
```

The agent intelligently extracts both the **dataset identifier** and the **connector type** from natural language!
