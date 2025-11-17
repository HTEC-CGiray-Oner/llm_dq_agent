# src/connectors/base_connector.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Optional


class BaseConnector(ABC):
    """Abstract base class for all data source connectors."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connector with configuration.

        Args:
            config: Dictionary containing connector-specific configuration
        """
        self.config = config
        self._connection = None

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the data source."""
        pass

    @abstractmethod
    def load_data(self, dataset_id: str, **kwargs) -> pd.DataFrame:
        """
        Load data from the data source.

        Args:
            dataset_id: Identifier for the dataset (table name, file path, etc.)
            **kwargs: Additional parameters specific to the connector

        Returns:
            DataFrame containing the loaded data
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test if the connection is valid.

        Returns:
            True if connection is successful, False otherwise
        """
        pass

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
