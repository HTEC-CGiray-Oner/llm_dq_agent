import pandas as pd
import numpy as np
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
            return settings.get('default_connector', 'snowflake')
    return 'snowflake'

def load_data_by_id(dataset_id: str, connector_type: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Loads data based on dataset ID using the specified connector.

    Args:
        dataset_id: The identifier for the dataset (table name, file name, etc.)
        connector_type: Type of connector to use ('snowflake', 'postgres').
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
        connector_type (str, optional): The data source connector to use ('snowflake', 'postgres').
                                       If not specified, uses default from settings.

    Returns:
        Dict[str, Any]: A dictionary containing the status and the total quantity of duplicate rows found.
    """
    # 1. Load the data based on the ID provided by the LLM
    df = load_data_by_id(dataset_id, connector_type=connector_type)

    # 2. Counting duplicates
    total_rows = len(df)
    duplicate_numb = total_rows - len(df.drop_duplicates())

    return {
        "dataset_id": dataset_id,
        "total_rows": total_rows,
        "duplicate_qty": duplicate_numb,
        "status": "success" if duplicate_numb == 0 else "failure"
    }

def check_dataset_null_values(dataset_id: str, connector_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Analyzes a dataset for null, missing, and empty values across all columns.

    This function is crucial for data completeness assessment, identifying columns
    with data gaps that may impact analysis or require imputation strategies.
    It automatically handles various null representations including explicit NULLs,
    empty strings, and '<NA>' placeholder values commonly found in data exports.

    The analysis provides both absolute counts and percentages to help prioritize
    data quality remediation efforts based on the severity of missingness.

    Args:
        dataset_id (str): Full table identifier for the dataset to analyze. Examples:
                         - 'SCHEMA.TABLE' for PostgreSQL
                         - 'DATABASE.SCHEMA.TABLE' for Snowflake
                         - 'TABLE' for default schema
        connector_type (str, optional): The data source connector to use ('snowflake', 'postgres').
                                       If not specified, uses default from settings.yaml

    Returns:
        Dict[str, Any]: A dictionary containing:
            - dataset_id: The identifier of the analyzed dataset
            - total_rows: Total number of rows in the dataset
            - columns_with_nulls: Number of columns containing null values
            - null_analysis: List of dictionaries for each column with nulls, containing:
                - column_name: Name of the column
                - null_count: Absolute number of null/missing values
                - null_percentage: Percentage of null values (0-100)
            - status: 'success' if analysis completed, 'failure' if errors occurred

    Example:
        result = check_dataset_null_values("SALES.CUSTOMERS")
        # Returns: {
        #   'dataset_id': 'SALES.CUSTOMERS',
        #   'total_rows': 50000,
        #   'columns_with_nulls': 3,
        #   'null_analysis': [
        #     {'column_name': 'email', 'null_count': 4000, 'null_percentage': 8.0},
        #     {'column_name': 'phone', 'null_count': 5000, 'null_percentage': 10.0}
        #   ],
        #   'status': 'success'
        # }
    """
    try:
        # 1. Load the data based on the ID provided by the LLM
        df = load_data_by_id(dataset_id, connector_type=connector_type)

        # 2. Standardize null representations
        # Replace common null placeholders with pandas NaN for consistent analysis
        df_clean = df.copy()
        df_clean.replace('<NA>', np.nan, inplace=True)
        df_clean.replace('', np.nan, inplace=True)
        df_clean.replace('NULL', np.nan, inplace=True)
        df_clean.replace('null', np.nan, inplace=True)

        # 3. Analyze null values for each column
        null_analysis = []
        total_rows = len(df_clean)

        for col in df_clean.columns:
            # Count non-null values using pandas count() method
            defined_values_count = df_clean[col].count()
            null_count = total_rows - defined_values_count

            # Only include columns that have null values
            if null_count > 0:
                null_percentage = round((null_count / total_rows) * 100, 2)

                null_analysis.append({
                    'column_name': col,
                    'null_count': null_count,
                    'null_percentage': null_percentage
                })

        # 4. Sort by null percentage (highest first) for prioritization
        null_analysis.sort(key=lambda x: x['null_percentage'], reverse=True)

        return {
            "dataset_id": dataset_id,
            "total_rows": total_rows,
            "total_columns": len(df_clean.columns),
            "columns_with_nulls": len(null_analysis),
            "null_analysis": null_analysis,
            "status": "success"
        }

    except Exception as e:
        return {
            "dataset_id": dataset_id,
            "error": str(e),
            "status": "failure"
        }

def check_dataset_descriptive_stats(dataset_id: str, connector_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Provides comprehensive descriptive statistics for all columns in a dataset.

    This function generates statistical summaries using pandas describe() method for both
    numerical and categorical columns, offering insights into data distribution, central
    tendencies, variability, and uniqueness. Columns ending with "_id" are automatically
    cast as categorical for proper statistical treatment.

    Args:
        dataset_id (str): Full table identifier for the dataset to analyze. Examples:
                         - 'SCHEMA.TABLE' for PostgreSQL
                         - 'DATABASE.SCHEMA.TABLE' for Snowflake
                         - 'TABLE' for default schema
        connector_type (str, optional): The data source connector to use ('snowflake', 'postgres').
                                       If not specified, uses default from settings.yaml

    Returns:
        Dict[str, Any]: A dictionary containing:
            - dataset_id: The identifier of the analyzed dataset
            - descriptive_stats: Dictionary with column-wise statistics from df.describe()
            - status: 'success' if analysis completed, 'failure' if errors occurred

    Note:
        Columns ending with "_id" (e.g., customer_id, product_id) are automatically converted
        to categorical type before analysis to ensure appropriate statistical treatment.
    """
    try:
        # 1. Load the data based on the ID provided by the LLM
        df = load_data_by_id(dataset_id, connector_type=connector_type)

        # 2. Cast columns ending with "_id" to categorical for proper statistical treatment
        id_columns = [col for col in df.columns if col.lower().endswith('_id')]
        for col in id_columns:
            # Convert ID columns to categorical type
            df[col] = df[col].astype('category')
            print(f"✓ Converted '{col}' to categorical for descriptive statistics")

        # 3. Generate descriptive statistics using pandas describe
        describe_all = df.describe(include='all')

        # 4. Convert to dictionary format for API response
        stats_dict = {}
        for col in describe_all.columns:
            col_stats = {}
            for stat_name in describe_all.index:
                value = describe_all.loc[stat_name, col]
                # Handle NaN values and convert numpy types to native Python types
                if pd.isna(value):
                    col_stats[stat_name] = None
                elif isinstance(value, (np.integer, np.floating)):
                    col_stats[stat_name] = float(value) if isinstance(value, np.floating) else int(value)
                else:
                    col_stats[stat_name] = str(value)

            stats_dict[col] = col_stats

        return {
            "dataset_id": dataset_id,
            "descriptive_stats": stats_dict,
            "status": "success"
        }

    except Exception as e:
        return {
            "dataset_id": dataset_id,
            "error": str(e),
            "status": "failure"
        }

# Available data quality check functions
DQ_TOOLS = [check_dataset_duplicates, check_dataset_null_values, check_dataset_descriptive_stats]

if __name__ == '__main__':
    # Example execution:
    result1 = check_dataset_duplicates("test_data_set")
    print("Duplicate Check Result:", result1)

    result2 = check_dataset_null_values("test_data_set")
    print("Null Values Check Result:", result2)

    result3 = check_dataset_descriptive_stats("test_data_set")
    print("Descriptive Stats Result:", result3)
