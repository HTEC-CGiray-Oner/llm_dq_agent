# Flow Diagrams Usage Examples
"""
Examples of how to use the flow diagrams package for creating various types of diagrams.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.flow_diagrams import DiagramAgent, MermaidGenerator, WorkflowVisualizer


def example_1_basic_diagram_agent():
    """Example 1: Using the AI-powered diagram agent"""
    print("=== EXAMPLE 1: AI-Powered Diagram Agent ===")

    # Initialize the agent
    agent = DiagramAgent()

    # Create different types of diagrams
    examples = [
        "User registration process with email verification",
        "Data quality check: load CSV, validate columns, check duplicates, generate report",
        "API authentication flow with JWT tokens",
        "Database backup and recovery process"
    ]

    for i, description in enumerate(examples, 1):
        print(f"\n--- Example {i}: {description} ---")
        try:
            result = agent.create_diagram(description)
            print(result)
            print("\n" + "="*80)
        except Exception as e:
            print(f"Error: {e}")


def example_2_mermaid_templates():
    """Example 2: Using pre-built Mermaid templates"""
    print("=== EXAMPLE 2: Pre-built Mermaid Templates ===")

    generator = MermaidGenerator()

    # Data Quality Workflow
    print("\n--- Data Quality Workflow Template ---")
    dq_diagram = generator.create_dq_workflow(
        data_sources=["Snowflake Production", "PostgreSQL Staging"],
        quality_checks=["Duplicate Detection", "Null Validation", "Type Checking", "Outlier Analysis"],
        outputs=["Executive Dashboard", "Technical Report", "Alert Notifications"],
        error_handling=True
    )
    print(dq_diagram)

    # Agent Interaction Flow
    print("\n--- Agent Interaction Sequence ---")
    agent_flow = generator.create_agent_interaction_flow(
        user_query="Find duplicate customers in the sales database",
        tools=["search_tables", "check_duplicates", "generate_report"],
        databases=["Snowflake", "PostgreSQL"]
    )
    print(agent_flow)

    # System Architecture
    print("\n--- System Architecture ---")
    system_arch = generator.create_system_architecture({
        "Data Layer": ["Snowflake DW", "PostgreSQL OLTP", "CSV Files", "API Sources"],
        "Processing Layer": ["LangChain Agent", "Quality Engines", "RAG System"],
        "AI Layer": ["OpenAI GPT-4", "HuggingFace Embeddings", "Vector Database"],
        "Output Layer": ["Web Dashboard", "Reports", "Alerts", "File Exports"]
    })
    print(system_arch)


def example_3_system_visualization():
    """Example 3: Visualizing actual system configuration"""
    print("=== EXAMPLE 3: System Configuration Visualization ===")

    visualizer = WorkflowVisualizer()

    # Current system architecture
    print("\n--- Current System Architecture ---")
    current_system = visualizer.visualize_current_system_architecture()
    print(current_system)

    # Table discovery process
    print("\n--- Table Discovery Flow ---")
    discovery_flow = visualizer.visualize_data_discovery_flow()
    print(discovery_flow)

    # Quality check workflow
    print("\n--- Quality Check Workflow ---")
    qc_workflow = visualizer.visualize_quality_check_workflow()
    print(qc_workflow)


def example_4_save_diagrams():
    """Example 4: Generating and saving all diagrams"""
    print("=== EXAMPLE 4: Generate and Save All Diagrams ===")

    visualizer = WorkflowVisualizer()

    # Create diagrams directory
    output_dir = "./generated_diagrams"

    # Generate all system diagrams
    saved_files = visualizer.create_complete_system_documentation(output_dir)

    print(f"\nGenerated {len(saved_files)} diagrams in {output_dir}/")
    for name, filepath in saved_files.items():
        print(f"  - {name}: {os.path.basename(filepath)}")

    print(f"\nTo view the diagrams:")
    print("1. Install VS Code extension: 'Mermaid Preview'")
    print("2. Open any .mmd file and preview it")
    print("3. Or visit https://mermaid.live/ and paste the code")


def example_5_custom_dq_scenarios():
    """Example 5: Custom data quality scenarios"""
    print("=== EXAMPLE 5: Custom DQ Scenarios ===")

    agent = DiagramAgent()

    scenarios = [
        "Customer data deduplication workflow with fuzzy matching",
        "Real-time data quality monitoring with alerts",
        "Multi-source data validation before loading into data warehouse",
        "Data quality scorecard generation process",
        "Automated data profiling and anomaly detection pipeline"
    ]

    for scenario in scenarios:
        print(f"\n--- Scenario: {scenario} ---")
        try:
            result = agent.create_dq_workflow_diagram(scenario)
            print(result)
            print("\n" + "="*60)
        except Exception as e:
            print(f"Error: {e}")


def example_6_interactive_demo():
    """Example 6: Interactive diagram creation"""
    print("=== EXAMPLE 6: Interactive Demo ===")

    agent = DiagramAgent()

    print("Enter a description of the process you want to diagram (or 'quit' to exit):")

    while True:
        try:
            user_input = input("\n> ")
            if user_input.lower() in ['quit', 'exit', 'q']:
                break

            if user_input.strip():
                result = agent.create_diagram(user_input)
                print("\nGenerated Diagram:")
                print("-" * 50)
                print(result)
                print("-" * 50)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

    print("Demo finished!")


if __name__ == "__main__":
    print("Flow Diagrams Package Examples")
    print("=" * 50)

    # Run examples
    examples = [
        # example_1_basic_diagram_agent,
        example_2_mermaid_templates,
        example_3_system_visualization,
        example_4_save_diagrams,
        # example_5_custom_dq_scenarios,
        # example_6_interactive_demo  # Commented out for automated running
    ]

    for example_func in examples:
        try:
            example_func()
            print("\n" + "="*100 + "\n")
        except Exception as e:
            print(f"Error in {example_func.__name__}: {e}")

    print("All examples completed!")
    print("\nTo run individual examples, call the functions directly:")
    print("- example_1_basic_diagram_agent()")
    print("- example_2_mermaid_templates()")
    print("- example_3_system_visualization()")
    print("- example_4_save_diagrams()")
    print("- example_5_custom_dq_scenarios()")
    print("- example_6_interactive_demo()")
