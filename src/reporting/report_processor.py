# src/reporting/report_processor.py
"""
Main processor for handling Smart DQ Check results and generating comprehensive reports.
"""
import os
import re
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from .report_generator import DataQualityReportGenerator


class SmartDQReportProcessor:
    """
    Processes Smart DQ Check results and orchestrates comprehensive report generation.
    """

    def __init__(self, reports_dir: str = "../reports"):
        self.reports_dir = reports_dir
        self.generator = DataQualityReportGenerator()

        # Ensure reports directory exists
        os.makedirs(self.reports_dir, exist_ok=True)

    def validate_smart_dq_response(self, comprehensive_report: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate if the Smart DQ Check response indicates success or failure.

        Args:
            comprehensive_report: The response from run_smart_dq_check()

        Returns:
            Tuple of (is_valid, output_content)
        """
        report_output = comprehensive_report.get('output', '')

        # Check for relevance threshold failure
        if "No tables found with sufficient relevance" in report_output:
            return False, report_output

        # Check for no tables found at all
        if "No relevant tables found" in report_output:
            return False, report_output

        return True, report_output

    def extract_dataset_metadata(self, report_output: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract dataset ID and connector type from Smart DQ Check output.

        Args:
            report_output: The text output from Smart DQ Check

        Returns:
            Tuple of (dataset_id, connector_type) or (None, None) if extraction fails
        """
        dataset_id = None
        connector_type = None

        # Look for patterns like "stage_sales.analytics.customers" or "PROD_SALES.REPORTING.CUSTOMERS"
        # Updated to support any schema name, not just "public"
        dataset_patterns = [
            r'`([^`]+\.[^`]+\.[^`]+)`',      # Pattern: `database.schema.table`
            r'([A-Z_]+\.[A-Z_]+\.[A-Z_]+)',  # Pattern: DATABASE.SCHEMA.TABLE
            r'([a-z_]+\.[a-z_]+\.[a-z_]+)',  # Pattern: database.schema.table
        ]

        for pattern in dataset_patterns:
            match = re.search(pattern, report_output)
            if match:
                dataset_id = match.group(1)
                break

        # Determine connector type based on dataset naming convention or explicit mention
        if dataset_id:
            # Check for explicit database connector mentions first
            if 'snowflake' in report_output.lower():
                connector_type = 'snowflake'
            elif 'postgres' in report_output.lower():
                connector_type = 'postgres'
            # Check for environment-specific patterns (more specific patterns first)
            elif dataset_id.upper().startswith('STAGE') or '_STAGE' in dataset_id.upper():
                connector_type = 'postgres'
            elif dataset_id.upper().startswith('PROD') or '_PROD' in dataset_id.upper():
                connector_type = 'snowflake'
            else:
                # Default based on case - uppercase typically Snowflake, lowercase typically Postgres
                connector_type = 'snowflake' if dataset_id.isupper() else 'postgres'

        return dataset_id, connector_type

    def extract_check_results_from_report(self, report_output: str) -> Optional[Dict[str, Any]]:
        """
        Extract structured check results from the Smart DQ Check text output.

        This method parses the agent's markdown output to create a structured dictionary
        of check results.

        Args:
            report_output: The text output from Smart DQ Check

        Returns:
            Dictionary containing extracted check results or None if parsing fails
        """
        try:
            import json

            # Look for JSON data in the output first - the agent might include structured results
            json_pattern = r'\{[\s\S]*"assessment_results"[\s\S]*\}'
            json_match = re.search(json_pattern, report_output)

            if json_match:
                json_str = json_match.group(0)
                parsed_data = json.loads(json_str)

                # Extract assessment results if available
                if 'assessment_results' in parsed_data:
                    assessment_results = parsed_data['assessment_results']
                    if 'check_results' in assessment_results:
                        return assessment_results['check_results']

            # Parse markdown format (this is the main format from Smart DQ Check)
            check_results = {}

            # Extract duplicate check results from markdown
            # Pattern: "- **Total Rows**: 37,811" and "- **Duplicate Records**: 0 (0.00% of data)"
            total_rows_pattern = r'Total Rows.*?(\d+(?:,\d+)*)'
            duplicate_records_pattern = r'Duplicate Records.*?(\d+)\s*\(([0-9.]+)%'

            total_rows_match = re.search(total_rows_pattern, report_output, re.IGNORECASE)
            duplicate_match = re.search(duplicate_records_pattern, report_output, re.IGNORECASE)

            # Check if duplicates section shows ERROR
            duplicate_error_pattern = r'### [❌✅] Duplicates[\s\S]*?- \*\*Status\*\*: ERROR'
            duplicate_has_error = re.search(duplicate_error_pattern, report_output, re.IGNORECASE)

            total_rows = 0  # Default value for use in null_values check
            if total_rows_match and duplicate_match:
                total_rows = int(total_rows_match.group(1).replace(',', ''))
                duplicate_qty = int(duplicate_match.group(1))
                percentage = float(duplicate_match.group(2))

                check_results['duplicates'] = {
                    'dataset_id': '',  # Will be filled later
                    'total_rows': total_rows,
                    'duplicate_qty': duplicate_qty,
                    'status': 'success' if duplicate_qty == 0 else 'failure'
                }
            elif duplicate_has_error:
                # Mark duplicate check as needing re-execution due to error
                print("   Duplicate check showed ERROR - will re-execute during report generation")
                check_results['duplicates'] = {
                    'status': 'error',
                    'error': 'Agent reported error - requires fresh execution'
                }

            # Extract null values check results from markdown
            # Pattern: "- **Columns with Missing Data**: 3 out of 22 columns"
            null_pattern = r'Columns with Missing Data.*?(\d+)\s*out of\s*(\d+)\s*columns'
            null_match = re.search(null_pattern, report_output, re.IGNORECASE)
            if null_match:
                columns_with_nulls = int(null_match.group(1))
                total_columns = int(null_match.group(2))

                # Extract detailed null information and convert to expected format
                null_analysis = []
                detail_pattern = r'`([^`]+)`:\s*([0-9,]+)\s*nulls?\s*\(([0-9.]+)%\)'
                detail_matches = re.findall(detail_pattern, report_output)
                for column, count_str, percentage in detail_matches:
                    count = int(count_str.replace(',', ''))
                    null_analysis.append({
                        'column_name': column,
                        'null_count': count,
                        'null_percentage': float(percentage)
                    })

                check_results['null_values'] = {
                    'dataset_id': '',  # Will be filled later
                    'total_rows': total_rows,  # Add total_rows from duplicates check
                    'total_columns': total_columns,
                    'columns_with_nulls': columns_with_nulls,
                    'null_analysis': null_analysis,  # Changed from null_details to null_analysis
                    'status': 'success'  # Always success, template will determine PASS/FAIL based on columns_with_nulls
                }

            # Extract descriptive stats check from markdown including detailed table data
            stats_pattern = r'Columns Analyzed.*?(\d+)\s*columns'
            stats_match = re.search(stats_pattern, report_output, re.IGNORECASE)

            # Look for statistical table data in the output
            descriptive_stats = {}

            # Check if there's a statistical table in the output
            if "📊 Complete Statistical Summary:" in report_output or "Statistical Summary" in report_output:
                # Extract table data - pattern to match table rows
                table_pattern = r'\|\s*`([^`]+)`\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]*)\|'
                table_matches = re.findall(table_pattern, report_output)

                for match in table_matches:
                    if len(match) >= 11:  # Ensure we have all columns
                        column_name = match[0].strip()

                        def parse_stat_value(value_str):
                            value = value_str.strip()
                            if value == '-' or value == '':
                                return None
                            try:
                                # Remove commas and convert to float
                                cleaned = value.replace(',', '')
                                if '.' in cleaned:
                                    return float(cleaned)
                                else:
                                    return int(cleaned)
                            except (ValueError, AttributeError):
                                return str(value)

                        descriptive_stats[column_name] = {
                            'count': parse_stat_value(match[1]),
                            'mean': parse_stat_value(match[2]),
                            'std': parse_stat_value(match[3]),
                            'min': parse_stat_value(match[4]),
                            '25%': parse_stat_value(match[5]),
                            '50%': parse_stat_value(match[6]),
                            '75%': parse_stat_value(match[7]),
                            'max': parse_stat_value(match[8]),
                            'unique': parse_stat_value(match[9]),
                            'top': match[10].strip() if match[10].strip() else None,
                            'freq': parse_stat_value(match[11]) if len(match) > 11 else None
                        }

            if stats_match:
                columns_analyzed = int(stats_match.group(1))
                check_results['descriptive_stats'] = {
                    'status': 'success',
                    'descriptive_stats': descriptive_stats,  # Include extracted statistical data
                    'message': 'Descriptive statistics computed',
                    'columns_analyzed': columns_analyzed
                }
            elif 'descriptive' in report_output.lower():
                # Fallback if pattern doesn't match but descriptive stats are mentioned
                check_results['descriptive_stats'] = {
                    'status': 'success',
                    'descriptive_stats': descriptive_stats,  # Include any extracted data
                    'message': 'Descriptive statistics computed'
                }

            return check_results if check_results else None

        except Exception as e:
            print(f"   Warning: Could not extract check results from report: {e}")
            return None

    def generate_filename_suffix(self, dataset_id: str) -> str:
        """
        Generate filename suffix from dataset ID for organized file naming.

        Args:
            dataset_id: The dataset identifier (e.g., "PROD_SALES.PUBLIC.CUSTOMERS")

        Returns:
            Formatted suffix (e.g., "prod_sales_customers")
        """
        # Extract environment and table name from dataset_id for filename
        # Example: PROD_SALES.PUBLIC.CUSTOMERS -> prod_sales_customers
        # Example: stage_sales.public.customers -> stage_sales_customers
        parts = dataset_id.split('.')
        if len(parts) >= 3:
            env_db = parts[0].lower()  # prod_sales or stage_sales
            table_name = parts[2].lower()  # customers, invoices, etc.
            name_suffix = f"{env_db}_{table_name}"
        else:
            # Fallback if pattern doesn't match expected format
            name_suffix = dataset_id.replace('.', '_').lower()

        return name_suffix

    def execute_dq_checks(self, dataset_id: str, connector_type: str) -> Dict[str, Any]:
        """
        Execute all data quality checks and return consolidated results.

        NOTE: This method is only used as a fallback when check results cannot be
        extracted from the existing Smart DQ Check output. In most cases, we avoid
        duplicate execution by parsing the comprehensive report.

        Args:
            dataset_id: The dataset to analyze
            connector_type: Database connector type ('postgres', 'snowflake')

        Returns:
            Dictionary containing all check results
        """
        from src.data_quality.checks import (
            check_dataset_duplicates,
            check_dataset_null_values,
            check_dataset_descriptive_stats
        )

        print("   Running duplicate check...")
        duplicate_results = check_dataset_duplicates(dataset_id, connector_type=connector_type)

        print("   Running null values check...")
        null_results = check_dataset_null_values(dataset_id, connector_type=connector_type)

        print("   Running descriptive statistics...")
        stats_results = check_dataset_descriptive_stats(dataset_id, connector_type=connector_type)

        return {
            'duplicates': duplicate_results,
            'null_values': null_results,
            'descriptive_stats': stats_results
        }



    def generate_all_reports(self, assessment_results: Dict[str, Any],
                           base_filename: str) -> Dict[str, str]:
        """
        Generate reports in all formats (Markdown, HTML, JSON).

        Args:
            assessment_results: The assessment data
            base_filename: Base filename without extension

        Returns:
            Dictionary mapping format to file path
        """
        generated_files = {}

        # 1. Save as Markdown (.md) using template
        md_file = f"{self.reports_dir}/{base_filename}.md"
        md_content = self.generator.generate_markdown_report(assessment_results)
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(md_content)
        generated_files['markdown'] = md_file
        print(f"   MARKDOWN: {md_file}")

        # 2. Save as HTML (.html) using template
        html_file = f"{self.reports_dir}/{base_filename}.html"
        html_content = self.generator.generate_html_report(assessment_results)
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        generated_files['html'] = html_file
        print(f"   HTML: {html_file}")

        # 3. Save as JSON (.json) using generator
        json_file = f"{self.reports_dir}/{base_filename}.json"
        json_content = self.generator.generate_json_report(assessment_results)
        with open(json_file, 'w', encoding='utf-8') as f:
            f.write(json_content)
        generated_files['json'] = json_file
        print(f"   JSON: {json_file}")

        return generated_files

    def process_comprehensive_report(self, comprehensive_report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main processing method that orchestrates the entire report generation workflow.

        Args:
            comprehensive_report: The response from run_smart_dq_check()

        Returns:
            Dictionary containing processing results and file paths
        """
        # Step 1: Validate the response
        is_valid, report_output = self.validate_smart_dq_response(comprehensive_report)

        # print(f"Report content preview: {report_output[:200]}...")

        if not is_valid:
            print("Smart DQ check failed due to low relevance scores.")
            print("Skipping report generation - no valid table found for analysis.")
            print("Try using a more specific table name in your query.")
            return {
                'status': 'failed',
                'reason': 'low_relevance',
                'files_generated': {}
            }

        print("Smart DQ check successful - proceeding with report generation...")

        # Step 2: Extract metadata
        dataset_id, connector_type = self.extract_dataset_metadata(report_output)

        if not dataset_id:
            print("Could not extract dataset information from agent response.")
            print("The agent response format may have changed or uses unexpected table naming patterns.")
            print("Please check the agent output format or update the extraction patterns.")
            return {
                'status': 'failed',
                'reason': 'metadata_extraction_failed',
                'files_generated': {}
            }

        print(f"Extracted info:")
        print(f"   Dataset: {dataset_id}")
        print(f"   Connector: {connector_type}")

        # Step 3: Generate filename
        name_suffix = self.generate_filename_suffix(dataset_id)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"comprehensive_dq_report_{name_suffix}_{timestamp}"

        # Step 4: Use run_full_assessment to ensure data consistency
        # This bypasses unreliable agent output parsing and uses DQ check functions directly
        print(f"\nRunning comprehensive DQ assessment with reliable data sources...")

        # Use run_full_assessment to get consistent results
        assessment_results = self.generator.run_full_assessment(
            dataset_id=dataset_id,
            connector_type=connector_type
        )

        print("    Assessment completed using direct DQ check execution!")
        print("    Data consistency guaranteed - no parsing errors!")

        # Step 6: Generate all reports
        print(f"\nGenerating reports using standardized templates...")
        generated_files = self.generate_all_reports(assessment_results, base_filename)

        return {
            'status': 'success',
            'dataset_id': dataset_id,
            'connector_type': connector_type,
            'assessment_results': assessment_results,
            'files_generated': generated_files
        }
