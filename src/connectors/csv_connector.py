# src/connectors/csv_connector.py
import pandas as pd
from typing import Dict, Any, Optional
from pathlib import Path
from .base_connector import BaseConnector


class CSVConnector(BaseConnector):
    """Connector for CSV files."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CSV connector.

        Config should contain:
            - base_path: Base directory path for CSV files
        """
        super().__init__(config)
        self.base_path = Path(config.get('base_path', './data'))

    def connect(self) -> None:
        """Validate CSV base path exists."""
        if not self.base_path.exists():
            raise ConnectionError(f"CSV base path does not exist: {self.base_path}")
        print(f"✓ CSV connector initialized. Base path: {self.base_path}")

    def disconnect(self) -> None:
        """No-op for CSV connector."""
        print("✓ CSV connector closed")

    def load_data(self, dataset_id: str, **kwargs) -> pd.DataFrame:
        """
        Load data from CSV file.

        Args:
            dataset_id: CSV filename (e.g., 'customers.csv' or 'customers')
            **kwargs: Additional pandas.read_csv parameters

        Returns:
            DataFrame with the data
        """
        # Add .csv extension if not present
        if not dataset_id.endswith('.csv'):
            dataset_id = f"{dataset_id}.csv"

        file_path = self.base_path / dataset_id

        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        try:
            print(f"Loading CSV: {file_path}")
            df = pd.read_csv(file_path, **kwargs)
            print(f"✓ Loaded {len(df)} rows from CSV")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV file: {str(e)}")

    def test_connection(self) -> bool:
        """Test if base path is accessible."""
        try:
            self.connect()
            print(f"✓ CSV connector test successful. Base path: {self.base_path}")
            return True
        except Exception as e:
            print(f"✗ CSV connector test failed: {str(e)}")
            return False
