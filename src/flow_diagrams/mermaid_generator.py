# src/flow_diagrams/mermaid_generator.py
"""
Specialized Mermaid diagram generator for data quality workflows.
Provides templates and utilities for creating consistent Mermaid diagrams.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import json


class MermaidGenerator:
    """
    Generator for Mermaid.js diagrams with pre-built templates for common data quality patterns.
    """

    def __init__(self):
        self.node_counter = 0
        self.node_map = {}

    def _get_node_id(self, label: str) -> str:
        """Generate unique node ID for a given label."""
        if label not in self.node_map:
            self.node_counter += 1
            self.node_map[label] = f"N{self.node_counter}"
        return self.node_map[label]

    def reset_nodes(self):
        """Reset node counter and mapping for new diagram."""
        self.node_counter = 0
        self.node_map = {}

    def create_dq_workflow(self,
                          data_sources: List[str],
                          quality_checks: List[str],
                          outputs: List[str],
                          error_handling: bool = True) -> str:
        """
        Create a standardized data quality workflow diagram.

        Args:
            data_sources: List of data source names
            quality_checks: List of quality check types
            outputs: List of output/report types
            error_handling: Include error handling paths

        Returns:
            Mermaid flowchart code
        """

        self.reset_nodes()

        lines = [
            "flowchart TD",
            "    %% Data Quality Workflow",
            f"    %% Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]

        # Start node
        start_id = self._get_node_id("Start")
        lines.append(f"    {start_id}((Start))")

        # Data sources
        source_ids = []
        for source in data_sources:
            source_id = self._get_node_id(source)
            source_ids.append(source_id)
            lines.append(f"    {source_id}[({source})]")
            lines.append(f"    {start_id} --> {source_id}")

        # Data loading
        load_id = self._get_node_id("Load Data")
        lines.append(f"    {load_id}[Load Data]")
        for source_id in source_ids:
            lines.append(f"    {source_id} --> {load_id}")

        # Quality checks
        check_ids = []
        for check in quality_checks:
            check_id = self._get_node_id(check)
            check_ids.append(check_id)
            lines.append(f"    {check_id}[{check}]")
            lines.append(f"    {load_id} --> {check_id}")

        # Decision point
        decision_id = self._get_node_id("Quality OK?")
        lines.append(f"    {decision_id}{{{' Quality OK? '}}}")
        for check_id in check_ids:
            lines.append(f"    {check_id} --> {decision_id}")

        # Outputs (success path)
        output_ids = []
        for output in outputs:
            output_id = self._get_node_id(output)
            output_ids.append(output_id)
            lines.append(f"    {output_id}(({output}))")
            lines.append(f"    {decision_id} -->|Pass| {output_id}")

        # Error handling
        if error_handling:
            error_id = self._get_node_id("Error Report")
            retry_id = self._get_node_id("Retry?")
            lines.extend([
                f"    {error_id}[!Error Report]",
                f"    {retry_id}{{{' Retry? '}}}",
                f"    {decision_id} -->|Fail| {error_id}",
                f"    {error_id} --> {retry_id}",
                f"    {retry_id} -->|Yes| {load_id}",
                f"    {retry_id} -->|No| {output_ids[0] if output_ids else 'End'}"
            ])

        # End
        end_id = self._get_node_id("End")
        lines.append(f"    {end_id}((End))")
        for output_id in output_ids:
            lines.append(f"    {output_id} --> {end_id}")

        # Styling
        lines.extend([
            "",
            "    %% Styling",
            f"    classDef dataSource fill:#e1f5fe",
            f"    classDef process fill:#f3e5f5",
            f"    classDef decision fill:#fff3e0",
            f"    classDef output fill:#e8f5e8",
            f"    classDef error fill:#ffebee",
            ""
        ])

        # Apply styles
        for source_id in source_ids:
            lines.append(f"    class {source_id} dataSource")

        for check_id in check_ids:
            lines.append(f"    class {check_id} process")

        lines.append(f"    class {decision_id} decision")

        for output_id in output_ids:
            lines.append(f"    class {output_id} output")

        if error_handling:
            lines.append(f"    class {error_id} error")

        return "\n".join(lines)

    def create_agent_interaction_flow(self,
                                    user_query: str,
                                    tools: List[str],
                                    databases: List[str]) -> str:
        """
        Create a diagram showing AI agent interaction flow.

        Args:
            user_query: Example user query
            tools: List of available tools
            databases: List of available databases

        Returns:
            Mermaid sequence diagram code
        """

        lines = [
            "sequenceDiagram",
            "    participant User",
            "    participant Agent",
            "    participant VectorDB",
            "    participant Tools",
            "    participant Database",
            "",
            f"    User->>Agent: {user_query}",
            "    Agent->>VectorDB: Search relevant tables",
            "    VectorDB-->>Agent: Table metadata + ranking",
            "    Agent->>Agent: Select best table & connector",
            ""
        ]

        # Add tool interactions
        for i, tool in enumerate(tools, 1):
            lines.append(f"    Agent->>Tools: Execute {tool}")
            lines.append(f"    Tools->>Database: Connect & query data")
            lines.append(f"    Database-->>Tools: Return results")
            lines.append(f"    Tools-->>Agent: Quality check results")
            if i < len(tools):
                lines.append("")

        lines.extend([
            "",
            "    Agent->>Agent: Generate comprehensive report",
            "    Agent-->>User: Final report with insights"
        ])

        return "\n".join(lines)

    def create_system_architecture(self, components: Dict[str, List[str]]) -> str:
        """
        Create a system architecture diagram.

        Args:
            components: Dictionary with component categories and their items

        Returns:
            Mermaid graph diagram code
        """

        self.reset_nodes()

        lines = [
            "graph TB",
            "    %% System Architecture",
            ""
        ]

        # Create subgraphs for each component category
        for category, items in components.items():
            category_id = category.replace(" ", "").lower()
            lines.append(f"    subgraph {category_id}[\"{category}\"]")

            for item in items:
                item_id = self._get_node_id(item)
                lines.append(f"        {item_id}[\"{item}\"]")

            lines.append("    end")
            lines.append("")

        return "\n".join(lines)

    def create_database_schema_flow(self,
                                  databases: List[str],
                                  schemas: Dict[str, List[str]],
                                  tables: Dict[str, List[str]]) -> str:
        """
        Create a database schema relationship diagram.

        Args:
            databases: List of database names
            schemas: Dictionary mapping databases to schemas
            tables: Dictionary mapping schemas to tables

        Returns:
            Mermaid graph diagram code
        """

        lines = [
            "graph TD",
            "    %% Database Schema Structure",
            ""
        ]

        for db in databases:
            db_schemas = schemas.get(db, [])

            lines.append(f"    subgraph {db.lower()}[\"{db} Database\"]")

            for schema in db_schemas:
                schema_tables = tables.get(schema, [])
                schema_id = f"{db.lower()}_{schema.lower()}"

                lines.append(f"        subgraph {schema_id}[\"{schema} Schema\"]")

                for table in schema_tables[:5]:  # Limit to first 5 tables
                    table_id = f"{schema_id}_{table.lower()}"
                    lines.append(f"            {table_id}[\"{table}\"]")

                if len(schema_tables) > 5:
                    lines.append(f"            {schema_id}_more[\"... {len(schema_tables) - 5} more tables\"]")

                lines.append("        end")

            lines.append("    end")
            lines.append("")

        return "\n".join(lines)

    def save_to_file(self, diagram_code: str, filename: str, output_dir: str = "./diagrams") -> str:
        """
        Save Mermaid diagram code to a file.

        Args:
            diagram_code: Mermaid diagram code
            filename: Output filename (without extension)
            output_dir: Output directory

        Returns:
            Full path to saved file
        """
        import os

        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"{filename}.mmd")

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(diagram_code)

        return filepath


# Example usage and templates
if __name__ == "__main__":
    generator = MermaidGenerator()

    # Test DQ workflow
    print("=== Data Quality Workflow ===")
    dq_diagram = generator.create_dq_workflow(
        data_sources=["Snowflake", "PostgreSQL"],
        quality_checks=["Check Duplicates", "Validate Nulls", "Data Types"],
        outputs=["Report", "Dashboard", "Alerts"]
    )
    print(dq_diagram)

    print("\n=== Agent Interaction Flow ===")
    agent_diagram = generator.create_agent_interaction_flow(
        user_query="Check for duplicates in customer table",
        tools=["check_dataset_duplicates", "check_dataset_nulls"],
        databases=["Snowflake", "PostgreSQL"]
    )
    print(agent_diagram)

    print("\n=== System Architecture ===")
    arch_diagram = generator.create_system_architecture({
        "Data Sources": ["Snowflake", "PostgreSQL", "CSV Files"],
        "Processing": ["LangChain Agent", "Quality Checks", "Report Generator"],
        "Storage": ["ChromaDB", "Vector Store", "File System"],
        "Output": ["Reports", "Dashboards", "Alerts"]
    })
    print(arch_diagram)
