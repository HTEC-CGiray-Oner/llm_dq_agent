# src/agent/reporting_tools.py
"""
Data Quality Reporting Tools for Agent Integration
"""
from typing import Dict, Any, List, Optional
from src.reporting import DataQualityReportGenerator


def generate_comprehensive_dq_report(dataset_id: str, connector_type: str = 'postgres',
                                   output_format: str = 'markdown') -> Dict[str, Any]:
    """
    Generate a comprehensive data quality report for a dataset.

    This tool runs all available data quality checks (duplicates, null values, descriptive stats)
    and generates a comprehensive report with recommendations. Perfect for getting a complete
    overview of data quality issues in a dataset.

    Args:
        dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for PostgreSQL)
        connector_type: Database type - 'snowflake' or 'postgres' (REQUIRED - use the connector type from the context)
        output_format: Report format - 'markdown', 'html', or 'summary' (default: 'markdown')

    Returns:
        Dictionary containing the report content and summary statistics
    """
    try:
        # Initialize report generator
        generator = DataQualityReportGenerator()

        # Run comprehensive assessment
        assessment_results = generator.run_full_assessment(
            dataset_id=dataset_id,
            connector_type=connector_type
        )

        # Generate report in requested format
        if output_format.lower() == 'html':
            report_content = generator.generate_html_report(assessment_results)
        elif output_format.lower() == 'summary':
            # Create a concise summary for agent responses
            report_content = _create_summary_report(assessment_results)
        else:  # Default to markdown
            report_content = generator.generate_markdown_report(assessment_results)

        # Prepare response
        summary = assessment_results['summary']

        # Ensure all values are integers
        passed_checks = int(summary['passed_checks'])
        failed_checks = int(summary['failed_checks'])
        error_checks = int(summary['error_checks'])

        return {
            'dataset_id': dataset_id,
            'connector_type': connector_type,
            'report_format': output_format,
            'report_content': report_content,
            'summary': {
                'total_checks': passed_checks + failed_checks + error_checks,
                'passed_checks': passed_checks,
                'failed_checks': failed_checks,
                'error_checks': error_checks,
                'pass_rate': f"{(passed_checks / max(1, passed_checks + failed_checks)) * 100:.1f}%"
            },
            'recommendations_count': len(assessment_results.get('recommendations', [])),
            'status': 'success'
        }

    except Exception as e:
        return {
            'dataset_id': dataset_id,
            'error': str(e),
            'status': 'error'
        }


def save_dq_report_to_file(dataset_id: str, connector_type: str = 'postgres',
                          formats: str = 'markdown,html,json') -> Dict[str, Any]:
    """
    Generate and save comprehensive data quality reports to files.

    This tool creates data quality reports and saves them to the reports directory
    in the specified formats. Useful when you need persistent reports for sharing
    or archival purposes.

    Args:
        dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for PostgreSQL)
        connector_type: Database type - 'snowflake' or 'postgres' (REQUIRED - use the connector type from the context)
        formats: Comma-separated list of formats to generate: 'markdown', 'html', 'json' (default: 'markdown,html,json')

    Returns:
        Dictionary containing file paths and summary information
    """
    try:
        # Initialize report generator
        generator = DataQualityReportGenerator()

        # Run comprehensive assessment
        assessment_results = generator.run_full_assessment(
            dataset_id=dataset_id,
            connector_type=connector_type
        )

        # Parse formats
        format_list = [f.strip().lower() for f in formats.split(',')]

        # Save reports
        saved_files = generator.save_report(
            assessment_results=assessment_results,
            output_dir="./reports",
            formats=format_list
        )

        # Prepare response
        summary = assessment_results['summary']

        # Ensure all values are integers
        passed_checks = int(summary['passed_checks'])
        failed_checks = int(summary['failed_checks'])
        error_checks = int(summary['error_checks'])

        return {
            'dataset_id': dataset_id,
            'connector_type': connector_type,
            'saved_files': saved_files,
            'formats_generated': list(saved_files.keys()),
            'summary': {
                'total_checks': passed_checks + failed_checks + error_checks,
                'passed_checks': passed_checks,
                'failed_checks': failed_checks,
                'error_checks': error_checks
            },
            'recommendations_count': len(assessment_results.get('recommendations', [])),
            'status': 'success'
        }

    except Exception as e:
        return {
            'dataset_id': dataset_id,
            'error': str(e),
            'status': 'error'
        }


def _create_summary_report(assessment_results: Dict[str, Any]) -> str:
    """Create a concise summary report for agent responses."""
    metadata = assessment_results['metadata']
    check_results = assessment_results['check_results']
    summary = assessment_results['summary']
    recommendations = assessment_results.get('recommendations', [])

    # Header
    report = f"""📊 DATA QUALITY ASSESSMENT SUMMARY
Dataset: {metadata['dataset_id']} ({metadata['connector_type'].upper()})

🎯 OVERALL RESULTS:
✅ Passed: {summary['passed_checks']} checks
❌ Failed: {summary['failed_checks']} checks
🚫 Errors: {summary['error_checks']} checks

📋 CHECK DETAILS:
"""

    # Individual check results
    for check_name, result in check_results.items():
        status_emoji = "✅" if result['status'] == 'success' else "❌"
        check_title = check_name.replace('_', ' ').title()

        if result['status'] == 'success':
            if check_name == 'duplicates':
                duplicate_qty = result.get('duplicate_qty', 0)
                status_text = "PASS" if duplicate_qty == 0 else "FAIL"
                report += f"{status_emoji} {check_title}: {status_text} ({duplicate_qty:,} duplicates)\n"

            elif check_name == 'null_values':
                columns_with_nulls = result.get('columns_with_nulls', 0)
                status_text = "PASS" if columns_with_nulls == 0 else "FAIL"
                report += f"{status_emoji} {check_title}: {status_text} ({columns_with_nulls} columns with nulls)\n"

            elif check_name == 'descriptive_stats':
                stats_count = len(result.get('descriptive_stats', {}))
                report += f"{status_emoji} {check_title}: PASS ({stats_count} columns analyzed)\n"
        else:
            report += f"{status_emoji} {check_title}: ERROR ({result.get('error', 'Unknown error')})\n"

    # Top recommendations
    if recommendations:
        report += f"\n🎯 TOP RECOMMENDATIONS:\n"
        for i, rec in enumerate(recommendations[:3], 1):  # Show top 3
            report += f"{i}. [{rec['priority']}] {rec['title']}\n"

    return report


# Export the tools for agent integration
REPORTING_TOOLS = [
    generate_comprehensive_dq_report,
    save_dq_report_to_file
]
