# src/connectors/connector_factory.py
import os
from typing import Dict, Any, Optional
from .base_connector import BaseConnector
from .snowflake_connector import SnowflakeConnector
from .postgres_connector import PostgresConnector
from .csv_connector import CSVConnector
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ConnectorFactory:
    """Factory class to create appropriate connector based on type."""

    _connectors = {
        'snowflake': SnowflakeConnector,
        'postgres': PostgresConnector,
        'postgresql': PostgresConnector,
        'csv': CSVConnector,
    }

    @classmethod
    def create_connector(cls, connector_type: str, config: Optional[Dict[str, Any]] = None) -> BaseConnector:
        """
        Create a connector instance based on type.

        Args:
            connector_type: Type of connector ('snowflake', 'postgres', 'csv')
            config: Configuration dictionary. If None, loads from settings.yaml and .env

        Returns:
            Initialized connector instance
        """
        connector_type = connector_type.lower()

        if connector_type not in cls._connectors:
            raise ValueError(
                f"Unknown connector type: {connector_type}. "
                f"Available types: {list(cls._connectors.keys())}"
            )

        # Load config if not provided
        if config is None:
            config = cls._load_config(connector_type)

        connector_class = cls._connectors[connector_type]
        return connector_class(config)

    @classmethod
    def _load_config(cls, connector_type: str) -> Dict[str, Any]:
        """Load connector configuration from settings.yaml and environment variables."""
        config = {}

        # Try to load from settings.yaml
        settings_path = os.path.join(os.path.dirname(__file__), '../../config/settings.yaml')
        if os.path.exists(settings_path):
            with open(settings_path, 'r') as f:
                settings = yaml.safe_load(f)
                if settings and 'connectors' in settings:
                    connector_config = settings['connectors'].get(connector_type, {})
                    # Make sure we don't get None
                    if connector_config:
                        config = connector_config
                    else:
                        config = {}

        # Override with environment variables if they exist
        env_mapping = {
            'snowflake': {
                'account': 'SNOWFLAKE_ACCOUNT',
                'user': 'SNOWFLAKE_USER',
                'password': 'SNOWFLAKE_PASSWORD',
                'warehouse': 'SNOWFLAKE_WAREHOUSE',
                'database': 'SNOWFLAKE_DATABASE',
                'schema': 'SNOWFLAKE_SCHEMA',
                'role': 'SNOWFLAKE_ROLE',
            },
            'postgres': {
                'host': 'POSTGRES_HOST',
                'port': 'POSTGRES_PORT',
                'database': 'POSTGRES_DATABASE',
                'user': 'POSTGRES_USER',
                'password': 'POSTGRES_PASSWORD',
            },
            'csv': {
                'base_path': 'CSV_BASE_PATH',
            }
        }

        if connector_type in env_mapping:
            for config_key, env_key in env_mapping[connector_type].items():
                env_value = os.getenv(env_key)
                if env_value:
                    config[config_key] = env_value

        return config

    @classmethod
    def get_available_connectors(cls) -> list:
        """Return list of available connector types."""
        return list(cls._connectors.keys())
