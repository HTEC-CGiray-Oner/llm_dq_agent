# Smart Data Quality Check Process Documentation

## Overview

Smart DQ Check process provides an AI-powered, natural language interface for automated data quality analysis. It combines RAG (Retrieval-Augmented Generation) for intelligent table discovery with LangChain agents for  tool selection and execution. Eliminates manual table specification by semantically understanding user queries and automatically finding relevant database tables for analysis.

## Architecture & Design Patterns

Smart DQ Check process implements below design patterns:

### **RAG-Powered Table Discovery**
- **Vector Search**: Uses HuggingFace embeddings (all-MiniLM-L6-v2) to convert user queries into 384-dimensional vectors
- **Semantic Matching**: ChromaDB performs cosine similarity search embedded queries against indexed table metadata
- **Intelligent Ranking**: Database preference relevance score boosting if there is a match with query and relevance scoring for optimal table selection

### **AI Agent Framework**
- **LangChain Integration**: Creates tool-calling agents with ChatOpenAI (GPT-4o) for intelligent decision making
- **Dynamic Tool Registration**: Automatically converts data quality functions into structured LangChain tools
- **Context-Aware Execution**: Uses retrieved table context to make informed tool, table and parameter selections

### **Multi-Database Support**
- **Factory Pattern**: Multiple connector creation for Snowflake and Postgres
- **Smart Auto-Detection**: Intelligent connector type determination based on table naming patterns and configuration
- **Unified Interface**: Consistent API across different database platforms through abstract base class implementation

## Configuration Requirements

### Environment Variables
```bash
# Database Connections
SNOWFLAKE_ACCOUNT=xyz.snowflakecomputing.com
SNOWFLAKE_USER=username
SNOWFLAKE_PASSWORD=password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PROD_SALES

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=analytics
POSTGRES_USER=username
POSTGRES_PASSWORD=password

# AI/LLM Configuration
OPENAI_API_KEY=sk-...
OPENAI_BASE_URL=https://api.openai.com/v1
```

### Smart Connector Detection
The system intelligently determines connector types through multiple strategies during RAG table discovery, with the AI agent making the final selection based on discovered table context:

**RAG Phase Connector Detection Strategies (Items 1-4):**

1. **Pattern Recognition** (`schema_indexer.py:306-324`):
   - During `search_tables()` execution, system analyzes table naming patterns
   - `if table_name.isupper(): prefer_snowflake = True` (e.g., `SALES.PUBLIC.CUSTOMERS`)
   - `elif table_name.islower(): prefer_postgres = True` (e.g., `analytics.users`)
   - Applied during relevance boosting phase to prioritize matching connector types

2. **Configuration Priority** (`connector_factory.py:54`):
   - RAG process loads configuration hierarchy: `.env` variables override `settings.yaml` defaults
   - `_load_config(connector_type)` merges environment variables with YAML configuration
   - Creates available connector pool for auto-detection fallback mechanism

3. **Auto-Detection** (`schema_indexer.py:288`):
   - When pattern recognition and explicit preferences fail during RAG search
   - System defaults to `settings.yaml` first available connector or environment variable precedence
   - Ensures RAG always returns valid connector context for discovered tables

4. **Database Mapping** (`schema_indexer.py:295-305`):
   - `_get_database_mappings()` extracts database→connector associations from ChromaDB metadata
   - Builds mapping dictionary: `{'prod_sales': 'snowflake', 'stage_sales': 'postgres'}`
   - Used during relevance ranking to match user query database hints with appropriate connectors
   - Environment keywords: staging/stage/ → postgres, prod/production → snowflake

**AI Agent Phase (Item 5):**

5. **Agent Selection** (Phase 3): LLM agent receives enriched table context from RAG with pre-determined connector types and makes final intelligent selection based on query semantics and table metadata

**Recent Connector Detection Improvements**:
- **Fixed Pattern Matching**: Resolved issue where 'PROD' was incorrectly found in 'PRODUCTS' tables
- **Environment-Specific Patterns**: `startswith('STAGE')` and `startswith('PROD')` for more precise matching
- **Staging Query Fix**: `stage_sales.public.products` now correctly detects as postgres (was incorrectly detecting as snowflake)
- **Priority Order**: Explicit mentions → Environment patterns → Case-based defaults

Example configuration in `settings.yaml`:
```yaml
default_connector: snowflake
connectors:
  snowflake:
    warehouse: COMPUTE_WH
    database: PROD_SALES
  postgres:
    host: localhost
    database: analytics
```

## Four-Phase Execution Process

### Phase 1: Query Processing & Table Discovery (RAG Implementation)
```
run_smart_dq_check(query: str, top_k_tables: int = 3)
├── get_schema_aware_retriever()                      📍 SmartPlanner.get_schema_aware_retriever() smart_planner.py:20
│   └── return SchemaIndexer()                       📍 SchemaIndexer.__init__() schema_indexer.py:23
├── search_tables(query, top_k=3)                    📍 SchemaIndexer.search_tables() schema_indexer.py:288
│   ├── _get_database_mappings()                     📍 SchemaIndexer._get_database_mappings() Build connector mappings from metadata
│   │   ├── chromadb.get_collection()               📍 Access indexed table metadata
│   │   └── Extract database → connector mappings    📍 {'prod': 'snowflake', 'sales': 'snowflake'}
│   ├── Database Preference Detection                📍 schema_indexer.py:306-324
│   │   ├── if 'postgres' in query.lower(): prefer PostgreSQL
│   │   ├── elif 'snowflake' in query.lower(): prefer Snowflake
│   │   └── else: use database_mappings lookup       📝 Note: Checks for 'prod', 'stage' environment hints
│   ├── Vector Embedding Generation                  📍 HuggingFace all-MiniLM-L6-v2
│   │   └── embeddings.embed_query(query)           📍 Convert query to 384-dim vector
│   ├── Similarity Search Execution                 📍 ChromaDB External Library
│   │   └── collection.query(query_embeddings)      📍 Cosine similarity search
│   ├── Relevance Boosting & Ranking               📍 schema_indexer.py:350-380
│   │   ├── base_relevance = 1 - cosine_distance
│   │   ├── if preferred_connector match: boost +0.3
│   │   ├── if table_name in query: boost +0.2
│   │   └── Sort by boosted_relevance DESC
│   └── ✅ Return ranked tables with metadata
└── Build tables_context for LLM agent              📍 smart_planner.py:188-195
```

**Purpose**: Uses RAG to semantically discover relevant tables from natural language queries, providing rich context for AI agent decision making.

**Key Features**:
- **Semantic Understanding**: Finds relevant tables even with different terminology (e.g., "invoice" query finds "billing" tables)
- **Database Preference**: Intelligently boosts tables from preferred database platforms
- **Rich Context**: Provides table metadata, column information, and relevance scores
- **Relevance Threshold Validation**: Enforces minimum 15% relevance score to prevent analysis of poorly matched tables

### Phase 2: AI Agent Setup & Tool Registration
```
create_smart_dq_agent()                              📍 SmartPlanner.create_smart_dq_agent() smart_planner.py:39
├── Tool Registration Loop                           📍 smart_planner.py:44-72
│   ├── for dq_function in DQ_TOOLS:               📍 From checks.py:247
│   │   ├── DQ_TOOLS = [check_dataset_duplicates,
│   │   │             check_dataset_null_values,
│   │   │             check_dataset_descriptive_stats]
│   │   ├── create_dq_tool_wrapper(dq_function)     📍 SmartPlanner.create_dq_tool_wrapper() smart_planner.py:25
│   │   │   └── def wrapper(dataset_id, connector_type):
│   │   │       return dq_function(dataset_id, connector_type=connector_type)
│   │   ├── Create tool descriptions based on function names
│   │   ├── tool = StructuredTool.from_function()   📍 LangChain Library
│   │   └── dq_tools.append(tool)
│   └── for reporting_function in REPORTING_TOOLS:  📍 reporting_tools.py
│       ├── generate_comprehensive_dq_report
│       └── save_dq_report_to_file
├── LLM Initialization                              📍 smart_planner.py:133-138
│   └── llm = ChatOpenAI(model="gpt-4o")           📍 LangChain Library
├── System Prompt Configuration                     📍 smart_planner.py:141-142
│   └── Define keyword-based tool selection logic
├── Agent Creation                                  📍 create_tool_calling_agent
│   └── agent = create_tool_calling_agent(llm, tools, prompt)
└── Executor Creation                               📍 AgentExecutor
    └── executor = AgentExecutor(agent=agent, tools=dq_tools)
```

**Purpose**: Creates an intelligent LLM-powered agent with access to data quality tools and sophisticated reasoning capabilities.

**Tool Selection Logic**:
- **Duplicates**: Keywords "duplicate", "duplicates" → `check_dataset_duplicates`
- **Null Values**: Keywords "null", "missing", "empty" → `check_dataset_null_values`
- **Statistics**: Keywords "stats", "statistics", "summary" → `check_dataset_descriptive_stats`
- **Reporting**: Keywords "report", "comprehensive", "save" → reporting tools

**Note**: Beyond keyword matching, the GPT-4o agent performs intelligent semantic analysis to select appropriate tools even with indirect language (e.g., "Are there repeated records?" → duplicates tool, "Is my data clean?" → comprehensive analysis)

### Phase 3: Agent Execution & Intelligent Decision Making
```
agent.invoke({"input": full_input, "chat_history": []}) 📍 AgentExecutor.invoke() smart_planner.py:208
├── LLM Analysis Phase                              📍 GPT-4o Model Processing
│   ├── Parse combined query and table context
│   ├── Analyze user intent from keywords
│   ├── Select appropriate data quality tool
│   ├── Choose highest relevance table matching requirements
│   └── Extract dataset_id and connector_type parameters
├── Tool Calling Decision                           📍 LangChain Agent Framework
│   └── Execute selected tool with parameters:
│       ├── check_dataset_duplicates(dataset_id, connector_type)
│       ├── check_dataset_null_values(dataset_id, connector_type)
│       ├── check_dataset_descriptive_stats(dataset_id, connector_type)
│       └── generate_comprehensive_dq_report(dataset_id, connector_type)
└── Response Generation                             📍 LangChain Internal Processing
    ├── Format analysis results with natural language
    ├── Provide actionable insights and recommendations
    └── Include error handling and status information
```

**Purpose**: Leverages LLM intelligence to analyze user intent, select appropriate tools, and execute data quality analysis with minimal human intervention.

**Decision Process**:
1. **Context Analysis**: LLM processes user query alongside discovered table metadata
2. **Intent Recognition**: Identifies data quality concerns from natural language
3. **Tool Selection**: Maps intent to appropriate analysis function
4. **Parameter Extraction**: Determines table selection and connector type
5. **Execution**: Calls selected tool with optimal parameters

### Phase 4: Database Connection & Data Quality Analysis
```
Tool Execution: check_dataset_duplicates(dataset_id, connector_type)
├── Smart Connector Detection                       📍 checks.py:19-42
│   └── load_data_by_id(dataset_id, connector_type)
│       ├── connector = ConnectorFactory.create_connector(connector_type)
│       │   ├── _load_config(connector_type)        📍 ConnectorFactory._load_config() connector_factory.py:54
│       │   │   ├── Load from settings.yaml and .env files
│       │   │   ├── Apply environment variable overrides
│       │   │   └── Return merged configuration
│       │   ├── if connector_type == 'snowflake':
│       │   │   └── return SnowflakeConnector(**config)
│       │   └── elif connector_type == 'postgres':
│       │       └── return PostgresConnector(**config)
├── Database Connection Management                   📍 Context Manager Protocol
│   ├── with connector:                            📍 Automatic resource management
│   │   ├── connector.__enter__()                  📍 Establish database connection
│   │   ├── df = connector.load_data(dataset_id)   📍 Execute SQL: SELECT * FROM table
│   │   └── connector.__exit__()                   📍 Automatic connection cleanup
├── Data Quality Analysis Execution                📍 Tool-specific analysis
│   ├── check_dataset_duplicates():
│   │   ├── total_rows = len(df)                   📍 Pandas operation
│   │   ├── duplicate_count = len(df) - len(df.drop_duplicates())
│   │   └── return structured results with counts and percentages
│   ├── check_dataset_null_values():
│   │   ├── null_counts = df.isnull().sum()        📍 Column-wise null analysis
│   │   ├── null_percentages = (null_counts/len(df)*100)
│   │   └── return detailed null value analysis
│   └── check_dataset_descriptive_stats():
│       ├── numeric_stats = df.describe()          📍 Statistical summary
│       ├── data_types = df.dtypes                 📍 Schema information
│       └── return comprehensive statistical analysis
└── Results Formatting & Response                  📍 Structured JSON output
    ├── Format analysis results with metadata
    ├── Include status indicators and error handling
    └── Return to LLM agent for natural language response
```

**Purpose**: Executes actual data quality analysis using hybrid SQL+Pandas approach with intelligent connector management and comprehensive error handling. Now enhanced with run_full_assessment method for guaranteed data consistency.

**Enhanced Reliability Features**:
- **run_full_assessment Integration**: Report generation now uses direct DQ check execution instead of agent output parsing
- **Consistent Data Flow**: Single execution path ensures matching total_rows across duplicates, null_values, and descriptive_stats sections
- **Parsing Error Elimination**: Bypasses unreliable regex extraction from agent responses
- **Guaranteed Accuracy**: Direct function calls provide verified results with consistent row counts

**Analysis Capabilities**:
- **Duplicate Detection**: Complete row duplicate analysis with counts and percentages
- **Null Value Analysis**: Column-wise missing data assessment with statistical insights
- **Descriptive Statistics**: Comprehensive statistical summaries for numeric and categorical data
- **Data Profiling**: Schema analysis, data types, and memory usage information

## Data Flow Between Phases

### **Phase 1 → Phase 2**: Query + Discovered Tables
```python
# Phase 1 Output: Ranked table list with metadata 📍 SchemaIndexer.search_tables() schema_indexer.py:380-390
relevant_tables = [
    {
        'rank': 1,
        'full_name': 'PROD_SALES.PUBLIC.INVOICES',
        'connector_type': 'snowflake',
        'relevance_score': 0.8523,
        'metadata': 'Table: INVOICES\nColumns: invoice_id, customer_id, amount...'
    }
]

# Phase 2 Input: Rich context for LLM agent 📍 SmartPlanner.run_smart_dq_check() smart_planner.py:188-195
tables_context = """RELEVANT TABLES (ranked by relevance):
--- Table #1 ---
Connector Type: SNOWFLAKE
Full Name: PROD_SALES.PUBLIC.INVOICES
Relevance: 85.23%
Metadata: [detailed table information]
"""
```

### **Phase 2 → Phase 3**: Agent + Tools Ready for Execution
```python
# Phase 2 Output: Configured agent with registered tools 📍 SmartPlanner.create_smart_dq_agent() smart_planner.py:150-160
agent_executor = AgentExecutor(
    agent=intelligent_llm_agent,
    tools=[duplicates_tool, nulls_tool, stats_tool, reporting_tools]
)

# Phase 3 Input: Combined query and context 📍 SmartPlanner.run_smart_dq_check() smart_planner.py:200-205
full_input = f"{user_query}\n\n{tables_context}"
```

### **Phase 3 → Phase 4**: Tool Selection + Parameters
```python
# Phase 3 Output: LLM decision 📍 LangChain Agent Framework
tool_call = {
    'tool': 'check_dataset_duplicates',
    'parameters': {
        'dataset_id': 'PROD_SALES.PUBLIC.INVOICES',
        'connector_type': 'snowflake'
    }
}

# Phase 4 Input: Execute with database connection 📍 DataQualityChecks.check_dataset_*() checks.py:50-150
result = tool_function(dataset_id, connector_type)
```

## Error Handling & Resilience

### **Connection Failures**
```python
# Graceful degradation in load_data_by_id() 📍 DataQualityChecks.load_data_by_id() checks.py:19-42
try:
    with connector:
        df = connector.load_data(dataset_id)
except Exception as e:
    print(f"Error loading data: {str(e)}")
    return pd.DataFrame()  # Returns empty DataFrame (not synthetic/dummy data) to ensure honest error handling and prevent misleading analysis results
```

### **No Tables Found**
```python
# Early validation in run_smart_dq_check() 📍 SmartPlanner.run_smart_dq_check() smart_planner.py:180-185
if not relevant_tables:
    return {
        "output": "No relevant tables found. Please ensure the schema index is built."
    }
```

### **Low Relevance Score Protection**
```python
# Relevance threshold validation 📍 SmartPlanner.run_smart_dq_check() smart_planner.py:175-190
MIN_RELEVANCE_THRESHOLD = 0.15  # 15% minimum match required

if not relevant_tables:
    if all_matches:
        best_match = all_matches[0]
        return {
            "output": f"No tables found with sufficient relevance (minimum 15% match required). "
                     f"Best match was '{best_match['full_name']}' with only {best_match['relevance_score']*100:.1f}% relevance. "
                     f"Please try a more specific table name or check if the table exists in your schema index."
        }
```

**Purpose**: Prevents analysis of poorly matched tables by enforcing minimum relevance thresholds, ensuring data quality analysis is performed only on semantically appropriate tables.

### **Tool Execution Errors**
- **SQL Errors**: Handled with try-catch blocks returning structured error responses
- **Permission Issues**: Graceful error messages with troubleshooting guidance
- **Data Type Errors**: Robust pandas operations with fallback handling

## Performance Characteristics

### **Table Discovery**
- **Vector Search**: O(log n) similarity search via ChromaDB indexing
- **Ranking Algorithm**: O(k) where k is top_k_tables parameter
- **Memory Usage**: Efficient vector operations with configurable limits

### **Database Operations**
- **Connection Pooling**: Automatic connection management via context managers
- **Query Optimization**: SELECT * queries with intelligent data sampling
- **Memory Management**: Pandas DataFrame operations with streaming for large datasets

### **Agent Processing**
- **LLM Latency**: Typically 2-5 seconds for tool selection and reasoning
- **Tool Execution**: Depends on data size and analysis complexity
- **Caching**: Intelligent caching of frequently accessed tables and results

## Key Benefits

### **Natural Language Interface**
- **Intuitive Queries**: "check invoice data quality", "find duplicates in sales"
- **Semantic Understanding**: Finds relevant tables regardless of exact terminology
- **Context-Aware**: Provides rich metadata for informed decision making

### **Intelligent Automation**
- **Zero Configuration**: Automatic table discovery eliminates manual specification
- **Smart Tool Selection**: LLM agent chooses optimal analysis methods
- **Multi-Database Support**: Seamless operation across Snowflake and PostgreSQL

### **Comprehensive Analysis**
- **Hybrid Approach**: Combines SQL efficiency with Pandas flexibility
- **Rich Reporting**: Detailed insights with actionable recommendations
- **Extensible Framework**: Easy addition of new data quality checks and databases

## Usage Examples

### **Basic Quality Check**
```python
from src.agent import run_smart_dq_check

# Natural language query
result = run_smart_dq_check("check for duplicates in customer data")
print(result['output'])
```

### **Advanced Analysis**
```python
# Multiple analysis types
result = run_smart_dq_check("comprehensive quality report for sales transactions")

# Custom table discovery
result = run_smart_dq_check("analyze invoice data quality", top_k_tables=5)
```

### **Cross-Database Analysis**
```python
# Automatic connector detection
result = run_smart_dq_check("check postgres analytics tables for missing values")

# Environment-specific analysis
result = run_smart_dq_check("please analyze prod sales table for data quality issues")

# Environment-based detection examples
result = run_smart_dq_check("staging customers table quality check")  # → postgres
result = run_smart_dq_check("production invoice data analysis")      # → snowflake
```

## Integration Points

### **Prerequisites**
1. **Schema Index**: Built via `python src/retrieval/schema_indexer.py`
2. **Database Connections**: Configured in `settings.yaml` and `.env`
3. **Vector Store**: ChromaDB collection with embedded table metadata
4. **LLM Access**: OpenAI API key and configuration

### **Extension Points**
- **New Data Quality Checks**: Add functions to `DQ_TOOLS` list
- **Additional Databases**: Implement new connector classes
- **Custom Tool Selection**: Modify agent prompt templates
- **Enhanced Reporting**: Extend reporting tools with new formats

This Smart DQ Check process documentation provides a complete understanding of the AI-powered data quality analysis system, from natural language query processing to sophisticated database analysis and intelligent result generation.
