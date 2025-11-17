# Data Connectors Guide

## Overview

The LLM Data Quality Agent now supports multiple data source connectors, allowing you to run data quality checks on data from various sources:

- **Snowflake** - Cloud data warehouse
- **PostgreSQL** - Relational database
- **CSV** - Local CSV files

## Installation

### Install Base Dependencies
```bash
poetry install
```

### Install Connector-Specific Dependencies

**For Snowflake:**
```bash
poetry install -E snowflake
```

**For PostgreSQL:**
```bash
poetry install -E postgres
```

**For All Connectors:**
```bash
poetry install -E all-connectors
```

## Configuration

### 1. Environment Variables (`.env`)

Copy and configure the relevant sections in your `.env` file:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role  # optional

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password

# CSV Configuration
CSV_BASE_PATH=./data
```

### 2. Settings File (`config/settings.yaml`)

Set your default connector:

```yaml
default_connector: csv  # or 'snowflake', 'postgres'
```

## Usage

### 1. Using Connectors Directly

```python
from src.connectors.connector_factory import ConnectorFactory

# Create a connector
connector = ConnectorFactory.create_connector('snowflake')

# Use with context manager (recommended)
with connector:
    df = connector.load_data('MY_TABLE')
    print(df.head())
```

### 2. Using with Data Quality Checks

```python
from src.data_quality.checks import check_dataset_duplicates

# Use default connector (from settings.yaml)
result = check_dataset_duplicates('my_dataset')

# Specify connector explicitly
result = check_dataset_duplicates('MY_TABLE', connector_type='snowflake')
```

### 3. Using with LLM Agent

The agent can automatically detect the connector type from natural language:

```python
from src.agent.planner import run_dq_check

# Agent will extract connector_type='snowflake' and dataset_id='CUSTOMERS'
query = "Check for duplicates in Snowflake table CUSTOMERS"
result = run_dq_check(query)

# Agent will use default connector
query = "Check for duplicates in orders table"
result = run_dq_check(query)

# Agent will extract connector_type='csv'
query = "Check CSV file sales_data.csv for duplicates"
result = run_dq_check(query)
```

## Connector-Specific Features

### Snowflake Connector

```python
connector = ConnectorFactory.create_connector('snowflake')

with connector:
    # Load entire table
    df = connector.load_data('MY_SCHEMA.MY_TABLE')

    # Load with custom query
    df = connector.load_data(
        'MY_TABLE',
        query="SELECT * FROM MY_TABLE WHERE date > '2024-01-01'"
    )

    # Load with row limit
    df = connector.load_data('MY_TABLE', limit=1000)
```

### PostgreSQL Connector

```python
connector = ConnectorFactory.create_connector('postgres')

with connector:
    # Load table
    df = connector.load_data('users')

    # Custom query
    df = connector.load_data(
        'users',
        query="SELECT * FROM users WHERE active = true"
    )

    # With limit
    df = connector.load_data('users', limit=500)
```

### CSV Connector

```python
connector = ConnectorFactory.create_connector('csv')

with connector:
    # Load CSV (extension optional)
    df = connector.load_data('customers.csv')
    # or
    df = connector.load_data('customers')

    # Pass pandas read_csv parameters
    df = connector.load_data(
        'data.csv',
        delimiter=';',
        encoding='utf-8'
    )
```

## Testing

Run the connector test suite:

```bash
poetry run python tests/test_connectors.py
```

Test individual connectors:

```python
from src.connectors.connector_factory import ConnectorFactory

# Test connection
connector = ConnectorFactory.create_connector('snowflake')
if connector.test_connection():
    print("Connection successful!")
```

## Adding New Connectors

1. Create a new connector class in `src/connectors/` that extends `BaseConnector`
2. Implement required methods: `connect()`, `disconnect()`, `load_data()`, `test_connection()`
3. Register it in `ConnectorFactory._connectors`
4. Add configuration to `settings.yaml` and `.env`
5. Add dependencies to `pyproject.toml`

Example:

```python
from src.connectors.base_connector import BaseConnector
import pandas as pd

class MyConnector(BaseConnector):
    def connect(self):
        # Your connection logic
        pass

    def disconnect(self):
        # Your cleanup logic
        pass

    def load_data(self, dataset_id: str, **kwargs) -> pd.DataFrame:
        # Your data loading logic
        pass

    def test_connection(self) -> bool:
        # Your connection test logic
        pass
```

## Troubleshooting

### Connection Errors

1. **Check credentials** in `.env` file
2. **Verify network access** to the data source
3. **Test connection** using `connector.test_connection()`

### Import Errors

Install the required connector package:
```bash
poetry install -E snowflake  # or postgres, or all-connectors
```

### Missing Configuration

Ensure both `.env` and `config/settings.yaml` are properly configured.

## Examples

See `tests/test_connectors.py` for comprehensive examples of using each connector type.
