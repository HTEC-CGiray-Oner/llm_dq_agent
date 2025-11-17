# src/data_quality/checks.py
import pandas as pd
from typing import Dict, Any

# Placeholder function: In a real project, this loads data from a DB/file
def load_data_by_id(dataset_id: str) -> pd.DataFrame:
    """Simulates loading data based on a user-provided ID."""
    print(f"--- Loading data for: {dataset_id} ---")
    # Example: If user says 'customer_data', we load that specific file/table
    data = {'A': [1, 2, 2, 2, 3], 'B': ['a', 'b', 'b', 'b', 'c']}
    return pd.DataFrame(data)

def check_dataset_duplicates(dataset_id: str) -> Dict[str, Any]:
    """
    Checks an entire dataset for duplicate rows and returns the total count of duplicates.

    This is essential for identifying redundant records in transactional or master data.
    It automatically handles byte array columns by converting them to strings before checking.

    Args:
        dataset_id (str): The unique identifier (name or ID) of the dataset to be analyzed (e.g., 'customer_data', 'october_sales').

    Returns:
        Dict[str, Any]: A dictionary containing the status and the total quantity of duplicate rows found.
    """
    # 1. Load the data based on the ID provided by the LLM
    df = load_data_by_id(dataset_id)

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
