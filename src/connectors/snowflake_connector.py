import pandas as pd
from typing import Dict, Any, Optional
from .base_connector import BaseConnector


class SnowflakeConnector(BaseConnector):
    """Connector for Snowflake data warehouse."""

    def __init__(self, config: Dict[str, Any], verbose: bool = True):
        """
        Initialize Snowflake connector.

        Config should contain:
            - account: Snowflake account identifier
            - user: Username
            - password: Password
            - warehouse: Warehouse name
            - database: Database name
            - schema: Schema name
            - role: (Optional) Role name
        """
        super().__init__(config)
        self._cursor = None
        self.verbose = verbose

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            import snowflake.connector

            self._connection = snowflake.connector.connect(
                account=self.config.get('account'),
                user=self.config.get('user'),
                password=self.config.get('password'),
                warehouse=self.config.get('warehouse'),
                database=self.config.get('database'),
                schema=self.config.get('schema'),
                role=self.config.get('role')
            )
            self._cursor = self._connection.cursor()
            if self.verbose:
                print(f"✓ Connected to Snowflake: {self.config.get('database')}.{self.config.get('schema')}")
        except ImportError:
            raise ImportError(
                "snowflake-connector-python is not installed. "
                "Install it with: poetry add snowflake-connector-python"
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}")

    def disconnect(self) -> None:
        """Close Snowflake connection."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        if self.verbose:
            print("✓ Disconnected from Snowflake")
        if self._connection:
            self._connection.close()

    def load_data(self, dataset_id: str, query: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load data from Snowflake table or custom query.

        Args:
            dataset_id: Full table name ('DB.SCHEMA.TABLE') or just table name
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
                # Simple table query
                sql_query = f"SELECT * FROM {dataset_id}"
                if limit:
                    sql_query += f" LIMIT {limit}"

            print(f"Executing query: {sql_query}")
            self._cursor.execute(sql_query)

            # Fetch data and column names
            columns = [desc[0] for desc in self._cursor.description]
            data = self._cursor.fetchall()

            df = pd.DataFrame(data, columns=columns)
            print(f"✓ Loaded {len(df)} rows from Snowflake")
            return df

        except Exception as e:
            raise RuntimeError(f"Failed to load data from Snowflake: {str(e)}")

    def test_connection(self) -> bool:
        """Test Snowflake connection."""
        try:
            if not self._cursor:
                self.connect()
            self._cursor.execute("SELECT CURRENT_VERSION()")
            result = self._cursor.fetchone()
            print(f"✓ Snowflake connection test successful. Version: {result[0]}")
            return True
        except Exception as e:
            print(f"✗ Snowflake connection test failed: {str(e)}")
            return False
