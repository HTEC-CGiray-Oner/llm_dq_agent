# src/flow_diagrams/diagram_agent.py
"""
LangChain agent for creating flow diagrams based on natural language descriptions.
Generates Mermaid.js, PlantUML, and Graphviz syntax for various diagram types.
"""

from langchain_openai import ChatOpenAI
from langchain.tools import Tool
from langchain.agents import create_openai_functions_agent, AgentExecutor
from langchain.prompts import ChatPromptTemplate
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()


class DiagramAgent:
    """
    LangChain agent for generating flow diagrams from natural language descriptions.
    Supports multiple diagram formats including Mermaid, PlantUML, and Graphviz.
    """

    def __init__(self, model: str = "gpt-4"):
        """
        Initialize the diagram agent.

        Args:
            model: OpenAI model to use (default: gpt-4)
        """
        self.llm = ChatOpenAI(model=model, temperature=0.1)
        self.tools = self._create_tools()
        self.agent = self._create_agent()

    def _create_tools(self) -> list:
        """Create tools for diagram generation."""

        def create_mermaid_flowchart(description: str) -> str:
            """Create a Mermaid flowchart diagram from description."""
            prompt = f"""
            Create a Mermaid flowchart for the following description:
            {description}

            Guidelines:
            - Use flowchart TD (top-down) or LR (left-right) direction
            - Use appropriate node shapes: [] for process, {{}} for decision, () for start/end
            - Include clear labels and logical flow
            - Use meaningful node IDs (A, B, C, etc.)

            Return only the Mermaid code in this format:
            ```mermaid
            flowchart TD
                [your diagram here]
            ```
            """

            response = self.llm.invoke(prompt)
            return response.content

        def create_mermaid_sequence(description: str) -> str:
            """Create a Mermaid sequence diagram from description."""
            prompt = f"""
            Create a Mermaid sequence diagram for the following description:
            {description}

            Guidelines:
            - Show interactions between different actors/systems
            - Use participant definitions
            - Include proper message flow with arrows
            - Add notes where helpful

            Return only the Mermaid code in this format:
            ```mermaid
            sequenceDiagram
                [your diagram here]
            ```
            """

            response = self.llm.invoke(prompt)
            return response.content

        def create_plantuml_diagram(description: str) -> str:
            """Create a PlantUML diagram from description."""
            prompt = f"""
            Create a PlantUML diagram for the following description:
            {description}

            Guidelines:
            - Use appropriate PlantUML syntax
            - Include start and stop for activity diagrams
            - Use proper formatting and structure

            Return only the PlantUML code in this format:
            @startuml
            [your diagram here]
            @enduml
            """

            response = self.llm.invoke(prompt)
            return response.content

        def create_graphviz_diagram(description: str) -> str:
            """Create a Graphviz DOT diagram from description."""
            prompt = f"""
            Create a Graphviz DOT diagram for the following description:
            {description}

            Guidelines:
            - Use digraph for directed graphs
            - Include proper node and edge definitions
            - Use appropriate attributes for styling
            - Set rankdir for layout direction

            Return only the DOT code in this format:
            digraph G {{
                [your diagram here]
            }}
            """

            response = self.llm.invoke(prompt)
            return response.content

        def create_data_quality_workflow(workflow_steps: str) -> str:
            """Create a data quality workflow diagram."""
            prompt = f"""
            Create a Mermaid flowchart specifically for this data quality workflow:
            {workflow_steps}

            Include typical DQ elements:
            - Data sources (databases, files)
            - Quality checks (duplicates, nulls, validation)
            - Decision points (pass/fail)
            - Reporting and outputs
            - Error handling paths

            Use these node styles:
            - [(Database)] for data sources
            - [Process] for quality checks
            - {{Decision?}} for pass/fail points
            - ((Report)) for outputs
            - [!Error] for error handling

            Return only the Mermaid code:
            ```mermaid
            flowchart TD
                [your DQ workflow here]
            ```
            """

            response = self.llm.invoke(prompt)
            return response.content

        # Create LangChain tools
        tools = [
            Tool(
                name="create_mermaid_flowchart",
                description="Create a Mermaid flowchart diagram from a description. Good for process flows, workflows, and decision trees.",
                func=create_mermaid_flowchart
            ),
            Tool(
                name="create_mermaid_sequence",
                description="Create a Mermaid sequence diagram from a description. Good for showing interactions between systems or actors over time.",
                func=create_mermaid_sequence
            ),
            Tool(
                name="create_plantuml_diagram",
                description="Create a PlantUML diagram from a description. Good for UML diagrams, activity diagrams, and formal documentation.",
                func=create_plantuml_diagram
            ),
            Tool(
                name="create_graphviz_diagram",
                description="Create a Graphviz DOT diagram from a description. Good for network diagrams, dependency graphs, and complex relationships.",
                func=create_graphviz_diagram
            ),
            Tool(
                name="create_data_quality_workflow",
                description="Create a specialized data quality workflow diagram with DQ-specific elements like quality checks, validation points, and error handling.",
                func=create_data_quality_workflow
            )
        ]

        return tools

    def _create_agent(self) -> AgentExecutor:
        """Create the LangChain agent with diagram tools."""

        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a diagram creation expert. You help users create various types of flow diagrams.

Available diagram types:
1. Mermaid Flowcharts - For process flows, workflows, decision trees
2. Mermaid Sequence Diagrams - For system interactions over time
3. PlantUML Diagrams - For formal UML and activity diagrams
4. Graphviz Diagrams - For network diagrams and complex relationships
5. Data Quality Workflows - Specialized DQ process diagrams

When a user requests a diagram:
1. Analyze their description to determine the best diagram type
2. Use the appropriate tool to generate the diagram code
3. Provide the generated code with brief usage instructions

Always choose the most suitable diagram type for the user's needs."""),
            ("user", "{input}"),
            ("assistant", "{agent_scratchpad}")
        ])

        agent = create_openai_functions_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=prompt
        )

        return AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
            handle_parsing_errors=True
        )

    def create_diagram(self, description: str, diagram_type: Optional[str] = None) -> str:
        """
        Create a diagram from a natural language description.

        Args:
            description: Natural language description of the diagram
            diagram_type: Optional specific diagram type to use

        Returns:
            Generated diagram code with usage instructions
        """

        if diagram_type:
            query = f"Create a {diagram_type} diagram for: {description}"
        else:
            query = f"Create the most appropriate diagram for: {description}"

        result = self.agent.invoke({"input": query})
        return result["output"]

    def create_dq_workflow_diagram(self, workflow_description: str) -> str:
        """
        Create a data quality workflow diagram specifically.

        Args:
            workflow_description: Description of the DQ workflow

        Returns:
            Mermaid flowchart code for the DQ workflow
        """

        query = f"Create a data quality workflow diagram for: {workflow_description}"
        result = self.agent.invoke({"input": query})
        return result["output"]


# Example usage and testing
if __name__ == "__main__":
    # Initialize the agent
    diagram_agent = DiagramAgent()

    # Test different diagram types
    test_cases = [
        "User login process with authentication and error handling",
        "Data quality check workflow: load data, check duplicates, validate nulls, generate report",
        "API request flow between frontend, backend, and database",
        "CI/CD pipeline with build, test, and deployment stages"
    ]

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'='*60}")
        print(f"TEST CASE {i}: {test_case}")
        print('='*60)

        try:
            result = diagram_agent.create_diagram(test_case)
            print(result)
        except Exception as e:
            print(f"Error: {e}")
