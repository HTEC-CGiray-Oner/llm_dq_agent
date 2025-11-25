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

        # Look for patterns like "stage_sales.public.customers" or "PROD_SALES.PUBLIC.CUSTOMERS"
        dataset_patterns = [
            r'`([^`]+\.public\.[^`]+)`',  # Pattern: `schema.public.table`
            r'([A-Z_]+\.[A-Z_]+\.[A-Z_]+)',  # Pattern: SCHEMA.PUBLIC.TABLE
            r'([a-z_]+\.public\.[a-z_]+)',   # Pattern: schema.public.table
        ]

        for pattern in dataset_patterns:
            match = re.search(pattern, report_output)
            if match:
                dataset_id = match.group(1)
                break

        # Determine connector type based on dataset naming convention or explicit mention
        if dataset_id:
            if 'PROD' in dataset_id.upper() or 'snowflake' in report_output.lower():
                connector_type = 'snowflake'
            elif 'STAGE' in dataset_id.upper() or 'postgres' in report_output.lower():
                connector_type = 'postgres'
            else:
                # Default based on case - uppercase typically Snowflake, lowercase typically Postgres
                connector_type = 'snowflake' if dataset_id.isupper() else 'postgres'

        return dataset_id, connector_type

    def extract_check_results_from_report(self, report_output: str) -> Optional[Dict[str, Any]]:
        """
        Extract structured check results from the Smart DQ Check text output.

        This method parses the agent's text output in order to create a structured dictionary
        of check results.

        Args:
            report_output: The text output from Smart DQ Check

        Returns:
            Dictionary containing extracted check results or None if parsing fails
        """
        try:
            import json

            # Look for JSON data in the output - the agent often includes structured results
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

            # If JSON parsing fails, try to extract information from text patterns
            check_results = {}

            # Extract duplicate check results
            duplicate_pattern = r'Duplicate.*?(\d+)\s*duplicate'
            duplicate_match = re.search(duplicate_pattern, report_output, re.IGNORECASE)
            if duplicate_match:
                duplicate_qty = int(duplicate_match.group(1))
                check_results['duplicates'] = {
                    'status': 'success',
                    'duplicate_qty': duplicate_qty,
                    'message': f'Found {duplicate_qty} duplicate records'
                }

            # Extract null value check results
            null_pattern = r'Null.*?(\d+).*?column'
            null_match = re.search(null_pattern, report_output, re.IGNORECASE)
            if null_match:
                columns_with_nulls = int(null_match.group(1))
                check_results['null_values'] = {
                    'status': 'success',
                    'columns_with_nulls': columns_with_nulls,
                    'message': f'Found {columns_with_nulls} columns with null values'
                }

            # Extract descriptive stats (harder to parse from text, so we'll flag it as available)
            if 'descriptive' in report_output.lower() or 'statistic' in report_output.lower():
                check_results['descriptive_stats'] = {
                    'status': 'success',
                    'descriptive_stats': {},
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

        print(f"Report content preview: {report_output[:200]}...")

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
            print("Could not extract dataset from successful report - this shouldn't happen!")
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

        # Step 4: Try to extract existing check results from the comprehensive report
        print(f"\nExtracting check results from Smart DQ Check output...")
        extracted_results = self.extract_check_results_from_report(report_output)

        if extracted_results:
            print("   Using existing check results from Smart DQ Check - no duplicate execution!")
            check_results = extracted_results
        else:
            print("   Could not extract structured results - running fresh DQ checks...")
            check_results = self.execute_dq_checks(dataset_id, connector_type)

        # Step 5: Create assessment
        assessment_results = self.generator.create_assessment_from_results(
            check_results=check_results,
            dataset_id=dataset_id,
            connector_type=connector_type
        )

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
