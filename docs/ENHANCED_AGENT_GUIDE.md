# Enhanced Data Quality Agent with Reporting

## Overview

The enhanced Data Quality Agent now supports comprehensive reporting capabilities through natural language queries. Users can request individual data quality checks OR generate complete assessment reports with intelligent recommendations.

## 🚀 Quick Start

```python
from src.agent.smart_planner import run_smart_dq_check

# Individual checks
result = run_smart_dq_check("Check for duplicates in STAGE_SALES CUSTOMERS table")

# Comprehensive reporting
result = run_smart_dq_check("Generate comprehensive DQ report for STAGE_SALES CUSTOMERS")

# Save reports to files
result = run_smart_dq_check("Save DQ report for postgres customers in markdown and HTML")
```

## 🎯 Supported Query Types

### 1. Individual Data Quality Checks

**Duplicate Detection:**
```python
# Natural language examples:
"Check for duplicates in STAGE_SALES CUSTOMERS table"
"Find duplicate rows in postgres orders"
"Are there any duplicate records in the customers data?"
```

**Null Values Analysis:**
```python
# Natural language examples:  
"Find null values in STAGE_SALES CUSTOMERS"
"Check for missing data in postgres products table"
"Analyze null values in snowflake order data"
```

**Descriptive Statistics:**
```python
# Natural language examples:
"Get descriptive statistics for STAGE_SALES CUSTOMERS"
"Show data summary for postgres products"
"Generate statistics for snowflake orders table"
```

### 2. Comprehensive Reporting

**Generate In-Memory Reports:**
```python
# These generate reports and return them in the response
"Generate comprehensive DQ report for STAGE_SALES CUSTOMERS"
"Create full assessment report for postgres customers"
"Generate DQ assessment for snowflake orders"

# Specify output format:
"Generate DQ report for STAGE_SALES CUSTOMERS in summary format"
"Create HTML report for postgres products"
"Generate markdown report for customer data"
```

**Save Reports to Files:**
```python
# These generate AND save reports to the ./reports directory
"Save DQ report for STAGE_SALES CUSTOMERS to files"
"Export comprehensive report for postgres customers in markdown and HTML"
"Generate and save DQ assessment for snowflake orders in all formats"

# Specify formats:
"Save DQ report for customers in markdown format"
"Export report for products in HTML and JSON formats"
```

## 🔄 How It Works

### Step 1: Intelligent Table Discovery
- Agent uses semantic search to find relevant tables
- Matches database sources (Snowflake vs PostgreSQL)  
- Ranks tables by relevance score

### Step 2: Query Intent Recognition
The agent automatically recognizes what type of request you're making:

- **Keywords for duplicates:** "duplicates", "duplicate rows", "duplicate records"
- **Keywords for nulls:** "null values", "missing data", "nulls", "missing values"
- **Keywords for stats:** "descriptive stats", "statistics", "data summary"
- **Keywords for reports:** "comprehensive report", "full report", "assessment report", "generate report"
- **Keywords for saving:** "save report", "export report", "create files"

### Step 3: Smart Database Mapping
- Automatically detects database type from context
- Maps table names to correct connectors
- Uses intelligent database aliases (STAGE_SALES → postgres, PROD_SALES → snowflake)

### Step 4: Execution and Response
- Individual checks: Returns specific analysis results
- Comprehensive reports: Runs all checks + generates formatted report
- File saving: Creates professional reports in ./reports directory

## 📊 Report Features

### Individual Check Results
- **Duplicates:** Count, examples, percentage
- **Null Values:** Per-column analysis, overall nulls percentage  
- **Descriptive Stats:** Pandas describe() output with proper formatting

### Comprehensive Reports Include
- **Executive Summary:** Pass/fail counts, overall assessment
- **Detailed Check Results:** All individual check outputs
- **Smart Recommendations:** Priority-ranked improvement suggestions
- **Multiple Formats:** Markdown, HTML with CSS, JSON

### Output Formats
- **Summary:** Concise text for agent responses
- **Markdown:** Professional formatting for documentation
- **HTML:** Styled reports with CSS for presentations
- **JSON:** Structured data for programmatic use

## 🗃️ Database Support

### Automatic Database Detection
The agent automatically maps database names:
- Tables with **STAGE_SALES** → PostgreSQL connector
- Tables with **PROD_SALES** → Snowflake connector
- Or specify explicitly: "postgres customers" or "snowflake orders"

### Supported Connectors
- **PostgreSQL:** `connector_type="postgres"`
- **Snowflake:** `connector_type="snowflake"`

## 📁 File Structure

When reports are saved, they go to:
```
./reports/
├── dataset_name_YYYYMMDD_HHMMSS.md      # Markdown report
├── dataset_name_YYYYMMDD_HHMMSS.html    # HTML report  
└── dataset_name_YYYYMMDD_HHMMSS.json    # JSON report
```

## 🎯 Example Workflows

### Workflow 1: Quick Quality Check
```python
# Just want to know if there are duplicates
result = run_smart_dq_check("Check for duplicates in STAGE_SALES CUSTOMERS")
print(result['output'])  # Gets duplicate count and examples
```

### Workflow 2: Comprehensive Assessment  
```python
# Want full quality overview
result = run_smart_dq_check("Generate comprehensive DQ report for STAGE_SALES CUSTOMERS")
print(result['output'])  # Gets full report with recommendations
```

### Workflow 3: Executive Reporting
```python
# Need files to share with stakeholders
result = run_smart_dq_check("Save DQ report for STAGE_SALES CUSTOMERS in markdown and HTML")
# Creates professional reports in ./reports/ directory
```

## 🔧 Advanced Usage

### Direct Tool Access
```python
from src.agent.reporting_tools import generate_comprehensive_dq_report, save_dq_report_to_file

# Direct function calls (if you know exact table names)
report = generate_comprehensive_dq_report(
    dataset_id="AGENT_LLM_READ.PUBLIC.CUSTOMERS",
    connector_type="snowflake", 
    output_format="summary"
)

files = save_dq_report_to_file(
    dataset_id="public.customers",
    connector_type="postgres",
    formats="markdown,html,json"
)
```

### Programmatic Access
```python
from src.agent.smart_planner import create_smart_dq_agent

# Get the agent directly for more control
agent = create_smart_dq_agent()
result = agent.invoke({
    "input": "Generate report for customers table",
    "chat_history": []
})
```

## ⚡ Benefits

1. **Natural Language Interface:** No need to remember function names or parameters
2. **Intelligent Table Discovery:** Finds the right tables automatically
3. **Smart Database Mapping:** No hardcoded connections
4. **Comprehensive Reporting:** All checks + recommendations in one go
5. **Multiple Output Formats:** Choose the right format for your audience
6. **Professional Presentation:** CSS-styled HTML reports ready for sharing
7. **Actionable Recommendations:** Priority-ranked improvement suggestions

## 🛠️ Technical Notes

- Uses LangChain tool calling for robust function execution
- ChromaDB semantic search for intelligent table discovery  
- HuggingFace embeddings for relevance scoring
- Dynamic database mapping from indexed metadata
- Automatic module reloading for development workflows

## 🎉 Ready to Use!

The enhanced agent is now ready for natural language data quality reporting. Simply describe what you want in plain English and the agent will handle table discovery, database mapping, check execution, and report generation automatically!