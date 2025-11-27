# Code Reference Guide - Functions and Classes

This document provides detailed information about all functions and classes in the LLM Data Quality Tool project, organized by directory structure.

## Table of Contents

- [/src/agent - AI Agent Components](#srcagent---ai-agent-components)
- [/src/connectors - Database Connectors](#srcconnectors---database-connectors)
- [/src/data_quality - Data Quality Checks](#srcdata_quality---data-quality-checks)
- [/src/retrieval - Schema Indexing](#srcretrieval---schema-indexing)
- [/src/reporting - Report Generation](#srcreporting---report-generation)
- [/notebooks - Testing and Examples](#notebooks---testing-and-examples)
- [/data - Sample Data](#data---sample-data)

---

## /src/agent - AI Agent Components

### smart_planner.py
**Purpose**: Main AI agent that handles natural language questions and orchestrates data quality analysis

#### Functions:

**`get_schema_aware_retriever()`**
- **Purpose**: Initialize schema discovery for finding relevant tables
- **Returns**: `SchemaIndexer` instance
- **Usage**: Sets up table discovery functionality

**`create_dq_tool_wrapper(dq_function)`**
- **Purpose**: Create wrapper functions for DQ tools that add connector support
- **Parameters**:
  - `dq_function`: Data quality function to wrap
- **Returns**: Wrapped function with connector support
- **Usage**: Enables DQ functions to work with multiple database types

**`create_smart_dq_agent()`**
- **Purpose**: Creates an enhanced LLM agent that can discover tables automatically
- **Returns**: Configured LangChain agent with DQ tools
- **Features**:
  - Automatic table discovery via RAG
  - Multi-connector support (Snowflake, PostgreSQL)
  - Intelligent tool selection based on user queries
  - Enhanced prompt engineering for DQ tasks

**`run_smart_dq_check(user_input: str, top_k_tables: int = 3)`**
- **Purpose**: Main entry point for running smart data quality checks
- **Parameters**:
  - `user_input`: Natural language question about data quality
  - `top_k_tables`: Number of relevant tables to find (default: 3)
- **Returns**: Dictionary with analysis results
- **Features**:
  - Natural language understanding
  - Automatic table discovery
  - Connector type detection
  - Comprehensive result formatting

### reporting_tools.py
**Purpose**: Specialized tools for generating and managing data quality reports

#### Functions:

**`run_comprehensive_dq_assessment(dataset_id: str, connector_type: str = 'postgres', checks_to_run: str = 'duplicates,null_values,descriptive_stats')`**
- **Purpose**: Run comprehensive data quality assessment and return raw results
- **Parameters**:
  - `dataset_id`: Full table name
  - `connector_type`: Database type ('snowflake' or 'postgres')
  - `checks_to_run`: Comma-separated list of checks to run
- **Returns**: Dictionary with complete assessment results
- **Usage**: Optimized tool for running multiple DQ checks efficiently

**`generate_comprehensive_dq_report(dataset_id: str, connector_type: str = 'postgres', output_format: str = 'markdown')`**
- **Purpose**: Generate comprehensive data quality report with all checks
- **Parameters**:
  - `dataset_id`: Full table name
  - `connector_type`: Database type
  - `output_format`: Report format ('markdown', 'html', or 'summary')
- **Returns**: Dictionary with report content and statistics
- **Features**: Runs all DQ checks and generates formatted reports

**`save_dq_report_to_file(dataset_id: str, connector_type: str = 'postgres', formats: str = 'markdown,html,json')`**
- **Purpose**: Generate and save comprehensive DQ reports to files
- **Parameters**:
  - `dataset_id`: Full table name
  - `connector_type`: Database type
  - `formats`: Comma-separated list of output formats
- **Returns**: Dictionary with file paths and summary information
- **Features**: Creates persistent report files for sharing and archival

**`generate_report_from_assessment_results(assessment_results_json: str, output_format: str = 'markdown')`**
- **Purpose**: Generate reports from pre-computed assessment results
- **Parameters**:
  - `assessment_results_json`: JSON string of assessment results
  - `output_format`: Desired output format
- **Returns**: Generated report content
- **Usage**: Efficient report generation when results are already available

**`save_report_from_assessment_results(assessment_results_json: str, formats: str = 'markdown,html,json')`**
- **Purpose**: Save reports from pre-computed assessment results to files
- **Parameters**:
  - `assessment_results_json`: JSON string of assessment results
  - `formats`: Output formats to generate
- **Returns**: File paths and summary information
- **Usage**: File saving when assessment results are cached

**`_create_summary_report(assessment_results: Dict[str, Any])`** *(Private)*
- **Purpose**: Create concise summary report for agent responses
- **Parameters**: Assessment results dictionary
- **Returns**: Formatted summary string
- **Usage**: Internal function for creating user-friendly summaries

---

## /src/connectors - Database Connectors

### base_connector.py
**Purpose**: Abstract base class defining the connector interface

#### Classes:

**`BaseConnector`** *(Abstract Base Class)*
- **Purpose**: Define standard interface for all database connectors
- **Methods**:
  - `__init__(config: Dict[str, Any])`: Initialize connector with configuration
  - `connect()`: Establish connection to data source *(Abstract)*
  - `disconnect()`: Close connection to data source *(Abstract)*
  - `load_data(dataset_id: str, **kwargs) -> pd.DataFrame`: Load data *(Abstract)*
  - `execute_query(query: str, **kwargs) -> pd.DataFrame`: Execute SQL query *(Abstract)*
  - `get_table_info(dataset_id: str) -> Dict[str, Any]`: Get table metadata *(Abstract)*

### connector_factory.py
**Purpose**: Factory pattern for creating appropriate database connectors

#### Functions:

**`create_connector(connector_type: str = None, config: Dict[str, Any] = None)`**
- **Purpose**: Create appropriate database connector based on type and configuration
- **Parameters**:
  - `connector_type`: Database type ('snowflake', 'postgres')
  - `config`: Override configuration (optional)
- **Returns**: Configured connector instance
- **Features**:
  - Automatic configuration loading from settings.yaml
  - Environment variable override support
  - Intelligent connector selection

**`_load_config(connector_type: str = None)`** *(Private)*
- **Purpose**: Load configuration with environment variable overrides
- **Parameters**: Connector type
- **Returns**: Merged configuration dictionary
- **Features**: Hierarchical configuration (defaults < yaml < env vars)

### snowflake_connector.py
**Purpose**: Snowflake database connector implementation

#### Classes:

**`SnowflakeConnector(BaseConnector)`**
- **Purpose**: Connect to and interact with Snowflake databases
- **Methods**:
  - `connect()`: Establish Snowflake connection
  - `disconnect()`: Close Snowflake connection
  - `load_data(dataset_id: str, sample_size: Optional[int] = None) -> pd.DataFrame`: Load table data
  - `execute_query(query: str) -> pd.DataFrame`: Execute SQL query
  - `get_table_info(dataset_id: str) -> Dict[str, Any]`: Get table metadata
- **Features**:
  - Connection pooling
  - Query optimization
  - Metadata extraction
  - Sample data loading

### postgres_connector.py
**Purpose**: PostgreSQL database connector implementation

#### Classes:

**`PostgresConnector(BaseConnector)`**
- **Purpose**: Connect to and interact with PostgreSQL databases
- **Methods**:
  - `connect()`: Establish PostgreSQL connection
  - `disconnect()`: Close PostgreSQL connection
  - `load_data(dataset_id: str, sample_size: Optional[int] = None) -> pd.DataFrame`: Load table data
  - `execute_query(query: str) -> pd.DataFrame`: Execute SQL query
  - `get_table_info(dataset_id: str) -> Dict[str, Any]`: Get table metadata
- **Features**:
  - psycopg2 integration
  - Connection management
  - Schema introspection
  - Efficient data loading

### schema_discovery.py
**Purpose**: Database schema discovery and analysis

#### Functions:

**`discover_schema_structure(connector_type: str = 'postgres')`**
- **Purpose**: Discover and analyze database schema structure
- **Parameters**: Database connector type
- **Returns**: Comprehensive schema information
- **Features**:
  - Table discovery
  - Column analysis
  - Relationship mapping
  - Metadata extraction

---

## /src/data_quality - Data Quality Checks

### checks.py
**Purpose**: Core data quality check functions

#### Functions:

**`get_default_connector_type()`**
- **Purpose**: Intelligently determine connector type from settings.yaml
- **Returns**: Default connector type string
- **Logic**:
  1. Check explicit default_connector setting
  2. Auto-detect from available connectors
  3. Fallback to 'snowflake'

**`smart_connector_detection(dataset_id: str)`**
- **Purpose**: Intelligently detect connector type based on dataset patterns
- **Parameters**: Dataset identifier
- **Returns**: Detected connector type
- **Logic**:
  - Table name patterns (uppercase = Snowflake, lowercase = postgres)
  - Database name mapping
  - Available connector configs

**`check_dataset_duplicates(dataset_id: str, connector_type: str = None)`**
- **Purpose**: Detect duplicate rows in dataset
- **Parameters**:
  - `dataset_id`: Table name to check
  - `connector_type`: Database type (auto-detected if None)
- **Returns**: Dictionary with duplicate analysis results
- **Features**:
  - Complete row duplicate detection
  - Duplicate count and percentage
  - Sample duplicate records
  - Performance optimization for large datasets

**`check_dataset_null_values(dataset_id: str, connector_type: str = None)`**
- **Purpose**: Analyze null/missing values in dataset
- **Parameters**:
  - `dataset_id`: Table name to check
  - `connector_type`: Database type (auto-detected if None)
- **Returns**: Dictionary with null value analysis
- **Features**:
  - Column-wise null counts and percentages
  - Overall completeness metrics
  - Null pattern identification
  - Missing data distribution

**`check_dataset_descriptive_stats(dataset_id: str, connector_type: str = None)`**
- **Purpose**: Generate comprehensive descriptive statistics
- **Parameters**:
  - `dataset_id`: Table name to analyze
  - `connector_type`: Database type (auto-detected if None)
- **Returns**: Dictionary with statistical analysis
- **Features**:
  - Numerical column statistics (mean, std, min, max, quartiles)
  - Categorical column analysis (unique values, frequencies)
  - Data type information
  - Distribution insights

**`check_dataset_types(dataset_id: str, connector_type: str = None)`**
- **Purpose**: Analyze data types and type consistency
- **Parameters**:
  - `dataset_id`: Table name to check
  - `connector_type`: Database type (auto-detected if None)
- **Returns**: Data type analysis results
- **Features**: Column data type validation and consistency checking

**`check_dataset_outliers(dataset_id: str, connector_type: str = None)`**
- **Purpose**: Detect statistical outliers in numerical data
- **Parameters**:
  - `dataset_id`: Table name to check
  - `connector_type`: Database type (auto-detected if None)
- **Returns**: Outlier detection results
- **Features**: IQR-based and statistical outlier detection

#### Constants:

**`DQ_TOOLS`**: List of available data quality check functions
- Contains all DQ check functions for tool registration
- Used by agent system for dynamic tool creation

---

## /src/retrieval - Schema Indexing

### schema_indexer.py
**Purpose**: Build and manage searchable database schema indexes

#### Classes:

**`SchemaIndexer`**
- **Purpose**: Create and manage vector database of table schema information
- **Methods**:
  - `__init__()`: Initialize ChromaDB and embedding model
  - `build_schema_index(connector_type: str = 'all')`: Build searchable schema index
  - `search_relevant_tables(query: str, top_k: int = 5)`: Find relevant tables via semantic search
  - `_get_db_display_name(connector_type: str)`: Get friendly database names
  - `_get_all_schemas(connector_type: str = 'all')`: Discover all database schemas
- **Features**:
  - Vector embeddings for semantic table search
  - Multi-database support
  - Intelligent table matching
  - Schema caching and fingerprinting

#### Functions:

**`build_schema_index_for_connector(connector_type: str)`**
- **Purpose**: Build schema index for specific connector type
- **Parameters**: Connector type ('snowflake', 'postgres')
- **Features**: Connector-specific schema indexing

---

## /src/reporting - Report Generation

### report_generator.py
**Purpose**: Core report generation engine

#### Classes:

**`DataQualityReportGenerator`**
- **Purpose**: Comprehensive data quality report generator
- **Methods**:
  - `__init__()`: Initialize with available check functions
  - `run_full_assessment(dataset_id: str, connector_type: str = 'postgres', checks: List[str] = None)`: Execute comprehensive DQ assessment
  - `create_assessment_from_results(check_results: Dict[str, Any], dataset_id: str, connector_type: str = 'postgres')`: Create assessment from pre-computed results
- **Features**:
  - Direct DQ check execution
  - Results caching and reuse
  - Comprehensive metadata tracking
  - Performance optimization

### report_processor.py
**Purpose**: Process and enhance report results

#### Classes:

**`SmartDQReportProcessor`**
- **Purpose**: Process DQ results and generate enhanced reports
- **Methods**:
  - `process_agent_result(result: Dict[str, Any])`: Process agent output
  - `run_full_assessment(dataset_id: str, connector_type: str)`: Run complete DQ assessment
  - `extract_dataset_metadata(result: Dict[str, Any])`: Extract dataset information
- **Features**:
  - Agent result processing
  - Metadata extraction
  - Error handling and validation
  - Result enhancement

### report_templates.py
**Purpose**: Report formatting and templating

#### Classes:

**`ReportTemplates`**
- **Purpose**: Generate formatted reports in multiple formats
- **Methods**:
  - `render_markdown_template(assessment_results: Dict[str, Any])`: Generate Markdown reports
  - `render_html_template(assessment_results: Dict[str, Any])`: Generate HTML reports
  - `render_json_template(assessment_results: Dict[str, Any])`: Generate JSON reports
- **Features**:
  - Multi-format output
  - Professional styling
  - Interactive HTML reports
  - Structured JSON exports

### remediation_advisor.py
**Purpose**: Generate data quality improvement recommendations

#### Classes:

**`RemediationAdvisor`**
- **Purpose**: Analyze DQ results and suggest improvements
- **Methods**:
  - `generate_recommendations(assessment_results: Dict[str, Any])`: Generate improvement suggestions
- **Features**:
  - Intelligent recommendation engine
  - Priority-based suggestions
  - Actionable improvement steps
  - Best practices guidance

---

## /notebooks - Testing and Examples

### tryouts.ipynb
**Purpose**: Main interactive testing and experimentation notebook

#### Features:
- **Interactive Testing**: Live testing environment for system functionality
- **Usage Examples**: Step-by-step examples of data quality workflows
- **Agent Testing**: Natural language query testing and validation
- **Report Generation**: Interactive report creation and preview
- **Performance Testing**: Benchmarking and optimization demonstrations

#### Sections:
- **Setup and Configuration**: Database connections and system initialization
- **Schema Indexing**: Building and testing the searchable table index
- **Smart DQ Checks**: Natural language query examples and results
- **Report Generation**: Multi-format report creation workflows
- **Advanced Usage**: Custom configurations and edge case handling

---

## /data - Sample Data

### fake_data_generator.py
**Purpose**: Generate synthetic test data for demonstrations

#### Functions:
- **Data generation utilities**: Create realistic test datasets
- **Features**: Multiple data formats, configurable parameters

### Sample Data Files:
- **`fake_customers_*.csv`**: Customer data samples
- **`fake_invoices_*.csv`**: Invoice data samples
- **`fake_products_*.csv`**: Product data samples
- **`fake_sales_*.csv`**: Sales data samples

---

## Key Design Patterns

### 1. Factory Pattern
- **Location**: `src/connectors/connector_factory.py`
- **Purpose**: Create appropriate database connectors based on configuration
- **Benefits**: Extensible connector system, configuration abstraction

### 2. RAG Pattern
- **Location**: `src/retrieval/schema_indexer.py` + `src/agent/smart_planner.py`
- **Purpose**: Retrieval-Augmented Generation for table discovery
- **Benefits**: Intelligent table matching, semantic understanding

---
