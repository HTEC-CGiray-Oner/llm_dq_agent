# src/connectors/schema_discovery.py
"""
Automatic schema discovery and metadata extraction for data sources.
Discovers tables, columns, and metadata to help the agent select appropriate datasets.
"""
import pandas as pd
from typing import Dict, List, Any, Optional
from .connector_factory import ConnectorFactory
import json


class SchemaDiscovery:
    """Discovers and extracts metadata from data sources."""

    def __init__(self, connector_type: str = 'snowflake'):
        self.connector_type = connector_type
        self.connector = ConnectorFactory.create_connector(connector_type)

    def discover_snowflake_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_views: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Discover all tables in Snowflake with their metadata.

        Args:
            database: Specific database to scan (None = current database)
            schema: Specific schema to scan (None = current schema)
            include_views: Whether to include views

        Returns:
            List of table metadata dictionaries
        """
        with self.connector:
            # Get current database/schema if not specified
            cursor = self.connector._cursor

            if not database:
                cursor.execute("SELECT CURRENT_DATABASE()")
                database = cursor.fetchone()[0]

            if not schema:
                cursor.execute("SELECT CURRENT_SCHEMA()")
                schema = cursor.fetchone()[0]

            print(f"Discovering tables in {database}.{schema}...")

            # Query information schema for tables
            query = f"""
            SELECT
                TABLE_CATALOG as database_name,
                TABLE_SCHEMA as schema_name,
                TABLE_NAME as table_name,
                TABLE_TYPE as table_type,
                ROW_COUNT,
                BYTES,
                COMMENT as table_comment,
                CREATED,
                LAST_ALTERED
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            """

            if not include_views:
                query += " AND TABLE_TYPE = 'BASE TABLE'"

            query += " ORDER BY TABLE_NAME"

            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            tables = []
            for row in rows:
                table_info = dict(zip(columns, row))
                tables.append(table_info)

            print(f"✓ Found {len(tables)} tables/views")
            return tables

    def get_table_columns(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get column information for a specific table.

        Args:
            table_name: Name of the table
            database: Database name (None = current)
            schema: Schema name (None = current)

        Returns:
            List of column metadata dictionaries
        """
        with self.connector:
            cursor = self.connector._cursor

            if not database:
                cursor.execute("SELECT CURRENT_DATABASE()")
                database = cursor.fetchone()[0]

            if not schema:
                cursor.execute("SELECT CURRENT_SCHEMA()")
                schema = cursor.fetchone()[0]

            query = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COMMENT as column_comment
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """

            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            column_info = []
            for row in rows:
                col_dict = dict(zip(columns, row))
                column_info.append(col_dict)

            return column_info

    def get_table_sample(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 5
    ) -> pd.DataFrame:
        """
        Get sample data from a table.

        Args:
            table_name: Name of the table
            database: Database name
            schema: Schema name
            limit: Number of sample rows

        Returns:
            DataFrame with sample data
        """
        with self.connector:
            cursor = self.connector._cursor

            if not database:
                cursor.execute("SELECT CURRENT_DATABASE()")
                database = cursor.fetchone()[0]

            if not schema:
                cursor.execute("SELECT CURRENT_SCHEMA()")
                schema = cursor.fetchone()[0]

            full_table_name = f"{database}.{schema}.{table_name}"
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            return pd.DataFrame(rows, columns=columns)

    def create_table_metadata_document(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_sample: bool = False
    ) -> str:
        """
        Create a comprehensive metadata document for a table.
        This document will be embedded in the vector database.

        Args:
            table_name: Name of the table
            database: Database name
            schema: Schema name
            include_sample: Whether to include sample data

        Returns:
            Formatted metadata document as string
        """
        # Get table info
        tables = self.discover_snowflake_tables(database, schema)
        table_info = next((t for t in tables if t['TABLE_NAME'] == table_name), None)

        if not table_info:
            return f"Table {table_name} not found"

        # Get columns
        columns = self.get_table_columns(table_name, database, schema)

        # Build document
        doc_parts = []

        # Header
        full_name = f"{table_info['DATABASE_NAME']}.{table_info['SCHEMA_NAME']}.{table_info['TABLE_NAME']}"
        doc_parts.append(f"TABLE: {full_name}")
        doc_parts.append(f"Type: {table_info['TABLE_TYPE']}")

        if table_info.get('TABLE_COMMENT'):
            doc_parts.append(f"Description: {table_info['TABLE_COMMENT']}")

        if table_info.get('ROW_COUNT'):
            doc_parts.append(f"Row Count: {table_info['ROW_COUNT']:,}")

        # Columns
        doc_parts.append("\nCOLUMNS:")
        for col in columns:
            col_desc = f"  - {col['COLUMN_NAME']} ({col['DATA_TYPE']})"
            if col.get('COLUMN_COMMENT'):
                col_desc += f" - {col['COLUMN_COMMENT']}"
            doc_parts.append(col_desc)

        # Sample data (optional)
        if include_sample:
            try:
                sample_df = self.get_table_sample(table_name, database, schema, limit=3)
                doc_parts.append("\nSAMPLE DATA:")
                doc_parts.append(sample_df.to_string(index=False))
            except Exception as e:
                doc_parts.append(f"\nSample data unavailable: {str(e)}")

        # Metadata
        doc_parts.append(f"\nCreated: {table_info.get('CREATED', 'N/A')}")
        doc_parts.append(f"Last Modified: {table_info.get('LAST_ALTERED', 'N/A')}")

        return "\n".join(doc_parts)

    def discover_all_table_metadata(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_sample: bool = False,
        max_tables: Optional[int] = None
    ) -> List[Dict[str, str]]:
        """
        Discover all tables and create metadata documents for each.

        Args:
            database: Database to scan
            schema: Schema to scan
            include_sample: Include sample data in metadata
            max_tables: Maximum number of tables to process

        Returns:
            List of dicts with 'table_name' and 'metadata' keys
        """
        tables = self.discover_snowflake_tables(database, schema)

        if max_tables:
            tables = tables[:max_tables]

        metadata_docs = []

        print(f"\nGenerating metadata documents for {len(tables)} tables...")
        for i, table in enumerate(tables, 1):
            table_name = table['TABLE_NAME']
            print(f"  [{i}/{len(tables)}] Processing {table_name}...")

            try:
                metadata = self.create_table_metadata_document(
                    table_name,
                    database,
                    schema,
                    include_sample
                )

                metadata_docs.append({
                    'table_name': table_name,
                    'full_name': f"{table['DATABASE_NAME']}.{table['SCHEMA_NAME']}.{table_name}",
                    'metadata': metadata
                })
            except Exception as e:
                print(f"    ✗ Error processing {table_name}: {str(e)}")
                continue

        print(f"✓ Generated {len(metadata_docs)} metadata documents")
        return metadata_docs
