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
                          checks_to_run: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run a comprehensive data quality assessment on a dataset.

        Args:
            dataset_id: The dataset identifier (table name)
            connector_type: Database connector type ('postgres', 'snowflake')
            checks_to_run: List of check names to run. If None, runs all available checks.

        Returns:
            Dict containing all check results and metadata
        """
        if checks_to_run is None:
            checks_to_run = list(self.available_checks.keys())

        assessment_results = {
            'metadata': {
                'dataset_id': dataset_id,
                'connector_type': connector_type,
                'timestamp': datetime.now().isoformat(),
                'checks_requested': checks_to_run,
                'total_checks': len(checks_to_run)
            },
            'check_results': {},
            'summary': {
                'passed_checks': 0,
                'failed_checks': 0,
                'error_checks': 0
            }
        }

        # Execute each requested check
        for check_name in checks_to_run:
            if check_name not in self.available_checks:
                print(f"⚠️ Warning: Unknown check '{check_name}' skipped")
                continue

            print(f"🔍 Running {check_name} check...")
            try:
                check_function = self.available_checks[check_name]
                result = check_function(dataset_id, connector_type=connector_type)
                assessment_results['check_results'][check_name] = result

                # Update summary based on check status
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
                    # This is a legitimate failed check (e.g., duplicates found, nulls found)
                    assessment_results['summary']['failed_checks'] += 1
                else:
                    # Only treat 'error' status as actual errors
                    assessment_results['summary']['error_checks'] += 1

            except Exception as e:
                print(f"Error running {check_name}: {e}")
                assessment_results['check_results'][check_name] = {
                    'status': 'error',
                    'error': str(e)
                }
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
            assessment_results: Results from run_full_assessment
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
                print(f"⚠️ Warning: Unknown format '{format_name}' skipped")
                continue

            # Write file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            saved_files[format_name] = file_path
            print(f"✅ {format_name.upper()} report saved: {file_path}")

        return saved_files


if __name__ == '__main__':
    # Example usage
    generator = DataQualityReportGenerator()

    # Run assessment
    results = generator.run_full_assessment(
        dataset_id="stage_sales.public.customers",
        connector_type="postgres"
    )

    # Save reports
    saved_files = generator.save_report(results)
    print(f"Reports generated: {list(saved_files.keys())}")
