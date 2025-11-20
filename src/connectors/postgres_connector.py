# src/connectors/postgres_connector.py
import pandas as pd
from typing import Dict, Any, Optional
from .base_connector import BaseConnector


class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL database."""

    def __init__(self, config: Dict[str, Any], verbose: bool = True):
        """
        Initialize PostgreSQL connector.

        Config should contain:
            - host: Database host
            - port: Database port (default: 5432)
            - database: Database name
            - user: Username
            - password: Password
        """
        super().__init__(config)
        self._cursor = None
        self.verbose = verbose

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        try:
            import psycopg2

            self._connection = psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432),
                database=self.config.get('database'),
                user=self.config.get('user'),
                password=self.config.get('password')
            )
            self._cursor = self._connection.cursor()
            if self.verbose:
                print(f"✓ Connected to PostgreSQL: {self.config.get('database')}")
        except ImportError:
            raise ImportError(
                "psycopg2-binary is not installed. "
                "Install it with: poetry add psycopg2-binary"
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")

    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        if self.verbose:
            print("✓ Disconnected from PostgreSQL")

    def load_data(self, dataset_id: str, query: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load data from PostgreSQL table or custom query.

        Args:
            dataset_id: Table name (e.g., 'customers', 'schema.table')
            query: Optional custom SQL query (overrides dataset_id)
            limit: Optional row limit

        Returns:
            DataFrame with the data
        """
        if not self._cursor:
            self.connect()

        try:
            if query:
                sql_query = query
            else:
                sql_query = f"SELECT * FROM {dataset_id}"
                if limit:
                    sql_query += f" LIMIT {limit}"

            print(f"Executing query: {sql_query}")
            df = pd.read_sql_query(sql_query, self._connection)
            print(f"✓ Loaded {len(df)} rows from PostgreSQL")
            return df

        except Exception as e:
            raise RuntimeError(f"Failed to load data from PostgreSQL: {str(e)}")

    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        try:
            if not self._cursor:
                self.connect()
            self._cursor.execute("SELECT version()")
            result = self._cursor.fetchone()
            print(f"✓ PostgreSQL connection test successful. Version: {result[0]}")
            return True
        except Exception as e:
            print(f"✗ PostgreSQL connection test failed: {str(e)}")
            return False
