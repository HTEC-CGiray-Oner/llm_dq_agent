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
        self.connector = ConnectorFactory.create_connector(connector_type, verbose=False)

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

            return tables

    def discover_postgres_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_views: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Discover all tables in PostgreSQL with their metadata.

        Args:
            database: Specific database to scan (None = current database)
            schema: Specific schema to scan (None = current schema, default: public)
            include_views: Whether to include views

        Returns:
            List of table metadata dictionaries
        """
        with self.connector:
            cursor = self.connector._cursor

            if not database:
                cursor.execute("SELECT current_database()")
                database = cursor.fetchone()[0]

            if not schema:
                schema = 'public'

            print(f"Discovering tables in {database}.{schema}...")

            # Query information schema for tables
            query = """
            SELECT
                table_catalog as database_name,
                table_schema as schema_name,
                table_name,
                table_type,
                NULL as row_count,
                NULL as bytes,
                NULL as table_comment,
                NULL as created,
                NULL as last_altered
            FROM information_schema.tables
            WHERE table_schema = %s
            """

            if not include_views:
                query += " AND table_type = 'BASE TABLE'"

            query += " ORDER BY table_name"

            cursor.execute(query, (schema,))
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            tables = []
            for row in rows:
                table_info = dict(zip(columns, row))
                # Normalize keys to match Snowflake format
                table_info['DATABASE_NAME'] = table_info.pop('database_name')
                table_info['SCHEMA_NAME'] = table_info.pop('schema_name')
                table_info['TABLE_NAME'] = table_info.pop('table_name')
                table_info['TABLE_TYPE'] = table_info.pop('table_type')
                table_info['ROW_COUNT'] = table_info.pop('row_count')
                table_info['BYTES'] = table_info.pop('bytes')
                table_info['TABLE_COMMENT'] = table_info.pop('table_comment')
                table_info['CREATED'] = table_info.pop('created')
                table_info['LAST_ALTERED'] = table_info.pop('last_altered')
                tables.append(table_info)

            print(f"✓ Found {len(tables)} tables/views")
            return tables

    def discover_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_views: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Discover tables - automatically uses the right method for the connector type.

        Args:
            database: Specific database to scan
            schema: Specific schema to scan
            include_views: Whether to include views

        Returns:
            List of table metadata dictionaries
        """
        if self.connector_type == 'snowflake':
            return self.discover_snowflake_tables(database, schema, include_views)
        elif self.connector_type == 'postgres':
            return self.discover_postgres_tables(database, schema, include_views)
        else:
            raise NotImplementedError(f"Table discovery not implemented for {self.connector_type}")

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
        if self.connector_type == 'snowflake':
            return self._get_snowflake_columns(table_name, database, schema)
        elif self.connector_type == 'postgres':
            return self._get_postgres_columns(table_name, database, schema)
        else:
            raise NotImplementedError(f"Column discovery not implemented for {self.connector_type}")

    def _get_snowflake_columns(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get Snowflake column information."""
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

    def _get_postgres_columns(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get PostgreSQL column information."""
        with self.connector:
            cursor = self.connector._cursor

            if not database:
                cursor.execute("SELECT current_database()")
                database = cursor.fetchone()[0]

            if not schema:
                schema = 'public'

            query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                NULL as column_comment
            FROM information_schema.columns
            WHERE table_schema = %s
                AND table_name = %s
            ORDER BY ordinal_position
            """

            cursor.execute(query, (schema, table_name))
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            column_info = []
            for row in rows:
                col_dict = dict(zip(columns, row))
                # Normalize keys to match Snowflake format
                col_dict['COLUMN_NAME'] = col_dict.pop('column_name')
                col_dict['DATA_TYPE'] = col_dict.pop('data_type')
                col_dict['IS_NULLABLE'] = col_dict.pop('is_nullable')
                col_dict['COLUMN_DEFAULT'] = col_dict.pop('column_default')
                col_dict['COLUMN_COMMENT'] = col_dict.pop('column_comment')
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

            if self.connector_type == 'snowflake':
                if not database:
                    cursor.execute("SELECT CURRENT_DATABASE()")
                    database = cursor.fetchone()[0]

                if not schema:
                    cursor.execute("SELECT CURRENT_SCHEMA()")
                    schema = cursor.fetchone()[0]

                full_table_name = f"{database}.{schema}.{table_name}"
                query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

            elif self.connector_type == 'postgres':
                if not database:
                    cursor.execute("SELECT current_database()")
                    database = cursor.fetchone()[0]

                if not schema:
                    schema = 'public'

                full_table_name = f"{schema}.{table_name}"
                query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

            else:
                raise NotImplementedError(f"Sample data not implemented for {self.connector_type}")

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
        # Get table info using connector-agnostic method
        tables = self.discover_tables(database, schema)
        table_info = next((t for t in tables if t['TABLE_NAME'] == table_name), None)

        if not table_info:
            return f"Table {table_name} not found"

        # Get columns
        columns = self.get_table_columns(table_name, database, schema)

        # Build document
        doc_parts = []

        # Header with connector type
        full_name = f"{table_info['DATABASE_NAME']}.{table_info['SCHEMA_NAME']}.{table_info['TABLE_NAME']}"

        # Add connector type info - helps agent distinguish between data sources
        connector_display = self.connector_type.upper()
        doc_parts.append(f"DATA SOURCE: {connector_display}")
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
                # Use provided limit or default to 3
                limit = getattr(include_sample, '__self__', {}).get('sample_row_limit', 3) if isinstance(include_sample, bool) else 3
                sample_df = self.get_table_sample(table_name, database, schema, limit=limit)
                if not sample_df.empty:
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
        sample_row_limit: int = 3,
        max_tables: Optional[int] = None
    ) -> List[Dict[str, str]]:
        """
        Discover all tables and create metadata documents for each.

        Args:
            database: Database to scan
            schema: Schema to scan
            include_sample: Include sample data in metadata
            sample_row_limit: Number of sample rows per table
            max_tables: Maximum number of tables to process

        Returns:
            List of dicts with 'table_name' and 'metadata' keys
        """
        # Store sample_row_limit for use in create_table_metadata_document
        self._sample_row_limit = sample_row_limit
        tables = self.discover_tables(database, schema)

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

        return metadata_docs
