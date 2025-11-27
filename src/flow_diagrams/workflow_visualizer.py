# src/flow_diagrams/workflow_visualizer.py
"""
High-level workflow visualizer that integrates with the existing data quality system.
Creates diagrams from actual system components and configurations.
"""

import os
import yaml
from typing import Dict, List, Any, Optional
from datetime import datetime
from .mermaid_generator import MermaidGenerator


class WorkflowVisualizer:
    """
    Creates visualizations of actual data quality workflows based on system configuration.
    Integrates with existing components to auto-generate accurate diagrams.
    """

    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        Initialize with system configuration.

        Args:
            config_path: Path to settings.yaml configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.mermaid = MermaidGenerator()

    def _load_config(self) -> Dict[str, Any]:
        """Load system configuration from settings.yaml"""
        try:
            full_path = os.path.join(os.path.dirname(__file__), '..', '..', self.config_path)
            with open(full_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Warning: Configuration file {self.config_path} not found. Using defaults.")
            return {}
        except Exception as e:
            print(f"Warning: Could not load configuration: {e}. Using defaults.")
            return {}

    def visualize_current_system_architecture(self) -> str:
        """
        Create a system architecture diagram based on current configuration.

        Returns:
            Mermaid diagram code showing actual system components
        """

        # Extract components from config
        connectors = list(self.config.get('connectors', {}).keys())

        components = {
            "Data Sources": connectors if connectors else ["Snowflake", "PostgreSQL"],
            "AI Components": [
                "LangChain Agent",
                "OpenAI GPT-4",
                "HuggingFace Embeddings",
                "ChromaDB Vector Store"
            ],
            "Quality Tools": [
                "Duplicate Detection",
                "Null Value Analysis",
                "Descriptive Statistics",
                "Type Validation",
                "Outlier Detection"
            ],
            "Processing": [
                "Schema Indexer",
                "Table Discovery",
                "Smart Connector Detection",
                "Report Generation"
            ],
            "Output": [
                "Markdown Reports",
                "HTML Reports",
                "JSON Results",
                "File Export"
            ]
        }

        return self.mermaid.create_system_architecture(components)

    def visualize_data_discovery_flow(self) -> str:
        """
        Create a diagram showing the table discovery and selection process.

        Returns:
            Mermaid flowchart showing RAG-based table discovery
        """

        self.mermaid.reset_nodes()

        lines = [
            "flowchart TD",
            "    %% Table Discovery & Selection Flow",
            f"    %% Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "    A((User Query)) --> B[Parse Query Keywords]",
            "    B --> C{Database Type Detected?}",
            "    C -->|Yes| D[Use Detected Connector]",
            "    C -->|No| E[Use Default Connector]",
            "    D --> F[Query Vector Database]",
            "    E --> F",
            "",
            "    F --> G[HuggingFace Embeddings]",
            "    G --> H[ChromaDB Similarity Search]",
            "    H --> I[Rank Tables by Relevance]",
            "    I --> J[Apply Database Boost]",
            "    J --> K[Apply Table Name Boost]",
            "    K --> L[Filter by Min Relevance]",
            "    L --> M[Return Top 3 Tables]",
            "",
            "    M --> N{Tables Found?}",
            "    N -->|Yes| O[Execute Quality Checks]",
            "    N -->|No| P[Return No Tables Message]",
            "",
            "    O --> Q[Generate Report]",
            "    Q --> R((Final Output))",
            "    P --> R",
            "",
            "    %% Styling",
            "    classDef input fill:#e3f2fd",
            "    classDef process fill:#f3e5f5",
            "    classDef decision fill:#fff3e0",
            "    classDef output fill:#e8f5e8",
            "    classDef vector fill:#fce4ec",
            "",
            "    class A,R input",
            "    class B,D,E,F,I,J,K,L,O,Q process",
            "    class C,N decision",
            "    class M,P output",
            "    class G,H vector"
        ]

        return "\n".join(lines)

    def visualize_agent_execution_flow(self) -> str:
        """
        Create a sequence diagram showing agent execution with tools.

        Returns:
            Mermaid sequence diagram of agent workflow
        """

        return self.mermaid.create_agent_interaction_flow(
            user_query="Check for duplicates in prod customers table",
            tools=[
                "search_tables",
                "check_dataset_duplicates",
                "check_dataset_nulls",
                "generate_report"
            ],
            databases=list(self.config.get('connectors', {}).keys())
        )

    def visualize_quality_check_workflow(self) -> str:
        """
        Create a detailed quality check workflow based on available tools.

        Returns:
            Mermaid flowchart of quality check process
        """

        # Get available connectors from config
        connectors = list(self.config.get('connectors', {}).keys())
        if not connectors:
            connectors = ["Snowflake", "PostgreSQL"]

        quality_checks = [
            "Check Duplicates",
            "Analyze Null Values",
            "Descriptive Statistics",
            "Data Type Validation",
            "Outlier Detection"
        ]

        outputs = [
            "Comprehensive Report",
            "Individual Check Reports",
            "HTML Dashboard",
            "JSON Export"
        ]

        return self.mermaid.create_dq_workflow(
            data_sources=connectors,
            quality_checks=quality_checks,
            outputs=outputs,
            error_handling=True
        )

    def visualize_database_schema_structure(self) -> str:
        """
        Create a diagram showing database and schema relationships.

        Returns:
            Mermaid graph showing database structure
        """

        connectors = self.config.get('connectors', {})
        databases = {}
        schemas = {}

        for connector_type, config in connectors.items():
            discovery = config.get('discovery', {})
            database = discovery.get('database', f'{connector_type.title()}_DB')
            schema_list = discovery.get('schemas', ['public', 'analytics'])

            databases[database] = schema_list
            for schema in schema_list:
                schemas[schema] = ['customers', 'orders', 'products', 'invoices']

        if not databases:
            # Default structure if no config
            databases = {
                'PROD_SALES': ['PUBLIC', 'ANALYTICS'],
                'STAGE_SALES': ['PUBLIC', 'STAGING']
            }
            schemas = {
                'PUBLIC': ['customers', 'orders', 'products'],
                'ANALYTICS': ['customer_metrics', 'sales_summary'],
                'STAGING': ['temp_customers', 'temp_orders']
            }

        return self.mermaid.create_database_schema_flow(
            databases=list(databases.keys()),
            schemas=databases,
            tables=schemas
        )

    def create_complete_system_documentation(self, output_dir: str = "./diagrams") -> Dict[str, str]:
        """
        Generate all system diagrams and save them to files.

        Args:
            output_dir: Directory to save diagram files

        Returns:
            Dictionary mapping diagram names to file paths
        """

        diagrams = {
            "system_architecture": self.visualize_current_system_architecture(),
            "table_discovery_flow": self.visualize_data_discovery_flow(),
            "agent_execution": self.visualize_agent_execution_flow(),
            "quality_check_workflow": self.visualize_quality_check_workflow(),
            "database_schema_structure": self.visualize_database_schema_structure()
        }

        saved_files = {}

        for name, diagram_code in diagrams.items():
            filepath = self.mermaid.save_to_file(diagram_code, name, output_dir)
            saved_files[name] = filepath
            print(f"Saved {name} to {filepath}")

        # Create index file
        index_path = os.path.join(output_dir, "README.md")
        self._create_diagram_index(saved_files, index_path)

        return saved_files

    def _create_diagram_index(self, saved_files: Dict[str, str], index_path: str):
        """Create an index file listing all generated diagrams."""

        content = [
            "# Data Quality System Diagrams",
            f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "This directory contains Mermaid diagrams documenting the data quality system architecture and workflows.",
            "",
            "## Available Diagrams",
            ""
        ]

        diagram_descriptions = {
            "system_architecture": "Overall system components and their relationships",
            "table_discovery_flow": "RAG-based table discovery and selection process",
            "agent_execution": "AI agent interaction sequence with tools and databases",
            "quality_check_workflow": "Complete data quality check process flow",
            "database_schema_structure": "Database and schema organization"
        }

        for name, filepath in saved_files.items():
            filename = os.path.basename(filepath)
            description = diagram_descriptions.get(name, "System diagram")
            content.extend([
                f"### {name.replace('_', ' ').title()}",
                f"**File**: `{filename}`",
                f"**Description**: {description}",
                ""
            ])

        content.extend([
            "## Usage",
            "",
            "### View in VS Code",
            "1. Install the 'Mermaid Preview' extension",
            "2. Open any `.mmd` file",
            "3. Use Ctrl+Shift+P and search for 'Mermaid Preview'",
            "",
            "### Convert to Images",
            "1. Install mermaid-cli: `npm install -g @mermaid-js/mermaid-cli`",
            "2. Convert: `mmdc -i diagram.mmd -o diagram.png`",
            "",
            "### Embed in Documentation",
            "Copy the Mermaid code and paste it in markdown files with:",
            "````markdown",
            "```mermaid",
            "[paste diagram code here]",
            "```",
            "````"
        ])

        with open(index_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(content))

        print(f"Created diagram index at {index_path}")


# Example usage
if __name__ == "__main__":
    visualizer = WorkflowVisualizer()

    print("=== System Architecture ===")
    print(visualizer.visualize_current_system_architecture())

    print("\n=== Table Discovery Flow ===")
    print(visualizer.visualize_data_discovery_flow())

    print("\n=== Quality Check Workflow ===")
    print(visualizer.visualize_quality_check_workflow())

    # Generate all diagrams
    print("\n=== Generating All Diagrams ===")
    saved_files = visualizer.create_complete_system_documentation()
    print(f"Generated {len(saved_files)} diagrams")
