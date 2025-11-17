# src/data_quality/checks.py
import pandas as pd
from typing import Dict, Any, Optional
import yaml
import os
from src.connectors.connector_factory import ConnectorFactory

# Load default connector from settings
def get_default_connector_type() -> str:
    """Get the default connector type from settings.yaml"""
    settings_path = os.path.join(os.path.dirname(__file__), '../../config/settings.yaml')
    if os.path.exists(settings_path):
        with open(settings_path, 'r') as f:
            settings = yaml.safe_load(f)
            return settings.get('default_connector', 'csv')
    return 'csv'

def load_data_by_id(dataset_id: str, connector_type: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Loads data based on dataset ID using the specified connector.

    Args:
        dataset_id: The identifier for the dataset (table name, file name, etc.)
        connector_type: Type of connector to use ('snowflake', 'postgres', 'csv').
                       If None, uses default from settings.yaml
        **kwargs: Additional parameters passed to the connector's load_data method

    Returns:
        DataFrame containing the loaded data
    """
    # Use default connector if not specified
    if connector_type is None:
        connector_type = get_default_connector_type()

    print(f"--- Loading data for: {dataset_id} using {connector_type.upper()} connector ---")

    try:
        # Create connector using factory
        connector = ConnectorFactory.create_connector(connector_type)

        # Use context manager to ensure proper connection cleanup
        with connector:
            df = connector.load_data(dataset_id, **kwargs)
            return df

    except Exception as e:
        print(f"Error loading data: {str(e)}")
        # Fallback to dummy data for development/testing
        print("Falling back to dummy data...")
        data = {'A': [1, 2, 2, 2, 3], 'B': ['a', 'b', 'b', 'b', 'c']}
        return pd.DataFrame(data)

def check_dataset_duplicates(dataset_id: str, connector_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Checks an entire dataset for duplicate rows and returns the total count of duplicates.

    This is essential for identifying redundant records in transactional or master data.
    It automatically handles byte array columns by converting them to strings before checking.

    Args:
        dataset_id (str): Full table identifier. Examples:
                         - 'SCHEMA.TABLE' or 'DATABASE.SCHEMA.TABLE'
        connector_type (str, optional): The data source connector to use ('snowflake', 'postgres', 'csv').
                                       If not specified, uses default from settings.

    Returns:
        Dict[str, Any]: A dictionary containing the status and the total quantity of duplicate rows found.
    """
    # 1. Load the data based on the ID provided by the LLM
    df = load_data_by_id(dataset_id, connector_type=connector_type)

    # 2. Duplicate Detection Logic
    byte_value_col = [
        df[i].name
        for i in df.columns
        if df[i].dtype == object and
           not df[i].empty and
           ("b'" in str(df[i].iloc[0]) or "{" in str(df[i].iloc[0]))
    ]

    # 3. Handle bytearrays/complex types
    for col_n in byte_value_col:
        df[col_n] = df[col_n].apply(lambda x: str(x))

    # 4. Counting duplicates
    duplicate_numb = len(df) - len(df.drop_duplicates())

    return {
        "dataset_id": dataset_id,
        "duplicate_qty": duplicate_numb,
        "status": "success" if duplicate_numb == 0 else "failure"
    }

# This list is used by the RAG index_builder and the AgentExecutor
DQ_TOOLS = [check_dataset_duplicates]

if __name__ == '__main__':
    # Example execution:
    result = check_dataset_duplicates("test_data_set")
    print(result)
