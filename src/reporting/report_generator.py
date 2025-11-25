# src/reporting/report_generator.py
"""
Main data quality report generator that orchestrates check execution and report creation.
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from src.data_quality.checks import check_dataset_duplicates, check_dataset_null_values, check_dataset_descriptive_stats
from .report_templates import ReportTemplates
from .remediation_advisor import RemediationAdvisor


class DataQualityReportGenerator:
    """
    Comprehensive data quality report generator that executes checks and creates reports.
    """

    def __init__(self):
        self.templates = ReportTemplates()
        self.advisor = RemediationAdvisor()
        self.available_checks = {
            'duplicates': check_dataset_duplicates,
            'null_values': check_dataset_null_values,
            'descriptive_stats': check_dataset_descriptive_stats
        }

    def run_full_assessment(self, dataset_id: str, connector_type: str = 'postgres',
                           checks: List[str] = None) -> Dict[str, Any]:
        """
        Execute a comprehensive data quality assessment by running all DQ checks directly.

        This method ensures data consistency by executing DQ checks once and using those
        results as the single source of truth for all report sections.

        Args:
            dataset_id: The dataset identifier (table name)
            connector_type: Database connector type ('postgres', 'snowflake')
            checks: List of checks to run (default: all available checks)

        Returns:
            Dict containing comprehensive assessment results
        """
        if checks is None:
            checks = list(self.available_checks.keys())

        # Execute DQ checks directly
        check_results = {}
        for check_name in checks:
            if check_name in self.available_checks:
                print(f"   Executing {check_name} check...")
                check_results[check_name] = self.available_checks[check_name](
                    dataset_id, connector_type=connector_type
                )

        # Create assessment from these reliable results
        return self.create_assessment_from_results(check_results, dataset_id, connector_type)

    def create_assessment_from_results(self, check_results: Dict[str, Any], dataset_id: str,
                                     connector_type: str = 'postgres') -> Dict[str, Any]:
        """
        Create a comprehensive assessment structure from pre-computed check results.

        This method allows bypassing the DQ function execution when results are already available,
        eliminating duplicate execution and improving performance.

        Args:
            check_results: Pre-computed results from DQ checks
            dataset_id: The dataset identifier (table name)
            connector_type: Database connector type ('postgres', 'snowflake')

        Returns:
            Dict containing all check results and metadata
        """
        assessment_results = {
            'metadata': {
                'dataset_id': dataset_id,
                'connector_type': connector_type,
                'timestamp': datetime.now().isoformat(),
                'checks_requested': list(check_results.keys()),
                'total_checks': len(check_results)
            },
            'check_results': check_results,
            'summary': {
                'passed_checks': 0,
                'failed_checks': 0,
                'error_checks': 0
            }
        }

        # Analyze pre-computed results to build summary
        for check_name, result in check_results.items():
            if result['status'] == 'success':
                if check_name == 'duplicates' and result.get('duplicate_qty', 0) == 0:
                    assessment_results['summary']['passed_checks'] += 1
                elif check_name == 'null_values' and result.get('columns_with_nulls', 0) == 0:
                    assessment_results['summary']['passed_checks'] += 1
                elif check_name == 'descriptive_stats':
                    assessment_results['summary']['passed_checks'] += 1
                else:
                    assessment_results['summary']['failed_checks'] += 1
            elif result['status'] == 'failure':
                assessment_results['summary']['failed_checks'] += 1
            else:
                assessment_results['summary']['error_checks'] += 1

        # Generate recommendations
        assessment_results['recommendations'] = self.advisor.generate_recommendations(
            assessment_results['check_results']
        )

        return assessment_results


    def generate_markdown_report(self, assessment_results: Dict[str, Any]) -> str:
        """Generate a human-readable Markdown report."""
        return self.templates.render_markdown_template(assessment_results)

    def generate_html_report(self, assessment_results: Dict[str, Any]) -> str:
        """Generate an HTML report with styling."""
        return self.templates.render_html_template(assessment_results)

    def generate_json_report(self, assessment_results: Dict[str, Any]) -> str:
        """Generate a structured JSON report for pipeline integration."""
        return json.dumps(assessment_results, indent=2, default=str)

    def save_report(self, assessment_results: Dict[str, Any], output_dir: str = "./reports",
                   formats: List[str] = None) -> Dict[str, str]:
        """
        Save reports to files in specified formats.

        Args:
            assessment_results: Results from create_assessment_from_results
            output_dir: Directory to save reports
            formats: List of formats to generate ('markdown', 'html', 'json')

        Returns:
            Dict mapping format names to file paths
        """
        if formats is None:
            formats = ['markdown', 'html', 'json']

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Generate timestamp for unique filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dataset_name = assessment_results['metadata']['dataset_id'].replace('.', '_')

        saved_files = {}

        for format_name in formats:
            if format_name == 'markdown':
                content = self.generate_markdown_report(assessment_results)
                filename = f"dq_report_{dataset_name}_{timestamp}.md"
                file_path = os.path.join(output_dir, filename)

            elif format_name == 'html':
                content = self.generate_html_report(assessment_results)
                filename = f"dq_report_{dataset_name}_{timestamp}.html"
                file_path = os.path.join(output_dir, filename)

            elif format_name == 'json':
                content = self.generate_json_report(assessment_results)
                filename = f"dq_report_{dataset_name}_{timestamp}.json"
                file_path = os.path.join(output_dir, filename)

            else:
                print(f"Warning: Unknown format '{format_name}' skipped")
                continue

            # Write file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            saved_files[format_name] = file_path
            print(f"{format_name.upper()} report saved: {file_path}")

        return saved_files


if __name__ == '__main__':
    # Example usage - optimized approach only
    generator = DataQualityReportGenerator()

    print("=== Optimized DQ Assessment Workflow ===")
    from src.data_quality.checks import check_dataset_duplicates, check_dataset_null_values, check_dataset_descriptive_stats

    # Step 1: Execute DQ checks once
    check_results = {}
    check_functions = {
        'duplicates': check_dataset_duplicates,
        'null_values': check_dataset_null_values,
        'descriptive_stats': check_dataset_descriptive_stats
    }

    dataset_id = "stage_sales.public.customers"
    connector_type = "postgres"

    for check_name, check_function in check_functions.items():
        print(f"Running {check_name} check...")
        check_results[check_name] = check_function(dataset_id, connector_type=connector_type)

    # Step 2: Create assessment from cached results (single source of truth)
    assessment = generator.create_assessment_from_results(
        check_results=check_results,
        dataset_id=dataset_id,
        connector_type=connector_type
    )

    # Step 3: Generate multiple reports from same cached results (no re-execution)
    print("\n=== Generating Reports from Cached Results ===")
    markdown_report = generator.generate_markdown_report(assessment)
    html_report = generator.generate_html_report(assessment)
    saved_files = generator.save_report(assessment)

    print(f"Reports generated: {list(saved_files.keys())}")
    print("Optimized workflow: DQ checks executed once, multiple reports generated from cache")
    print("No duplicate execution - maximum efficiency achieved!")
