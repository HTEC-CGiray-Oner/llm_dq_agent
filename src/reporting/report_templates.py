# src/reporting/report_templates.py
"""
Templates for generating different report formats (Markdown, HTML).
"""
from datetime import datetime
from typing import Dict, Any


class ReportTemplates:
    """
    Contains templates for rendering data quality reports in different formats.
    """

    def render_markdown_template(self, assessment_results: Dict[str, Any]) -> str:
        """Generate a Markdown report from assessment results."""
        metadata = assessment_results['metadata']
        check_results = assessment_results['check_results']
        summary = assessment_results['summary']
        recommendations = assessment_results.get('recommendations', [])

        # Header
        markdown = f"""# Data Quality Assessment Report

## Dataset Information
- **Dataset**: `{metadata['dataset_id']}`
- **Connector**: {metadata['connector_type'].upper()}
- **Assessment Date**: {datetime.fromisoformat(metadata['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}
- **Total Checks**: {metadata['total_checks']}

## Executive Summary
- ✅ **Passed**: {summary['passed_checks']} checks
- ❌ **Failed**: {summary['failed_checks']} checks
- 🚫 **Errors**: {summary['error_checks']} checks

---

## Detailed Check Results

"""

        # Individual check results
        for check_name, result in check_results.items():
            status_emoji = "✅" if result['status'] == 'success' else "❌"
            markdown += f"### {status_emoji} {check_name.replace('_', ' ').title()}\n\n"

            if result['status'] == 'success':
                if check_name == 'duplicates':
                    duplicate_qty = result.get('duplicate_qty', 0)
                    total_rows = result.get('total_rows', 0)
                    duplicate_percentage = (duplicate_qty / total_rows * 100) if total_rows > 0 else 0
                    status_text = "PASS" if duplicate_qty == 0 else "FAIL"
                    markdown += f"- **Status**: {status_text}\n"
                    markdown += f"- **Total Rows**: {total_rows:,}\n"
                    markdown += f"- **Duplicate Records**: {duplicate_qty:,} ({duplicate_percentage:.2f}% of data)\n"

                    if duplicate_qty > 0:
                        markdown += f"- **Impact**: Data redundancy detected - {duplicate_qty:,} rows are duplicated\n"
                        duplicate_examples = result.get('duplicate_examples', [])
                        if duplicate_examples:
                            markdown += f"- **Sample Duplicate Records**:\n"
                            for i, example in enumerate(duplicate_examples[:3], 1):  # Show first 3 examples
                                markdown += f"  {i}. Row appears {example.get('count', 1)} times\n"
                    else:
                        markdown += f"- **Quality**: Excellent - No duplicate records found\n"

                elif check_name == 'null_values':
                    total_rows = result.get('total_rows', 0)
                    columns_with_nulls = result.get('columns_with_nulls', 0)
                    total_columns = result.get('total_columns', 0)
                    status_text = "PASS" if columns_with_nulls == 0 else "FAIL"
                    markdown += f"- **Status**: {status_text}\n"
                    markdown += f"- **Dataset Size**: {total_rows:,} rows × {total_columns} columns\n"
                    markdown += f"- **Columns with Missing Data**: {columns_with_nulls} out of {total_columns} columns\n"

                    if columns_with_nulls > 0:
                        null_coverage = (columns_with_nulls / total_columns * 100) if total_columns > 0 else 0
                        markdown += f"- **Missing Data Coverage**: {null_coverage:.1f}% of columns affected\n"

                        null_analysis = result.get('null_analysis', [])
                        if null_analysis:
                            markdown += f"\n**Detailed Null Analysis:**\n"
                            for col_info in null_analysis:
                                col_name = col_info['column_name']
                                null_count = col_info['null_count']
                                null_pct = col_info['null_percentage']
                                markdown += f"- `{col_name}`: {null_count:,} nulls ({null_pct:.1f}%)\n"
                    else:
                        markdown += f"- **Quality**: Excellent - No missing values detected in any column\n"

                elif check_name == 'descriptive_stats':
                    markdown += f"- **Status**: PASS (Statistics Generated)\n"
                    stats = result.get('descriptive_stats', {})
                    markdown += f"- **Columns Analyzed**: {len(stats)} columns\n"

                    # Categorize columns by type
                    numeric_cols = []
                    categorical_cols = []

                    for col_name, col_stats in stats.items():
                        if 'mean' in col_stats:  # Numeric column
                            numeric_cols.append(col_name)
                        else:  # Categorical column
                            categorical_cols.append(col_name)

                    markdown += f"- **Numerical Columns**: {len(numeric_cols)} ({', '.join(numeric_cols[:5])}{'...' if len(numeric_cols) > 5 else ''})\n"
                    markdown += f"- **Categorical Columns**: {len(categorical_cols)} ({', '.join(categorical_cols[:5])}{'...' if len(categorical_cols) > 5 else ''})\n"

                    # Show key statistics for a few important columns
                    if numeric_cols:
                        markdown += f"\n**Key Numerical Statistics:**\n"
                        for col_name in numeric_cols[:3]:  # Show top 3 numeric columns
                            col_stats = stats[col_name]
                            mean_val = col_stats.get('mean', 'N/A')
                            std_val = col_stats.get('std', 'N/A')
                            min_val = col_stats.get('min', 'N/A')
                            max_val = col_stats.get('max', 'N/A')
                            markdown += f"- `{col_name}`: Mean={mean_val}, Std={std_val}, Range=[{min_val} to {max_val}]\n"

                    if categorical_cols:
                        markdown += f"\n**Key Categorical Statistics:**\n"
                        for col_name in categorical_cols[:3]:  # Show top 3 categorical columns
                            col_stats = stats[col_name]
                            unique_count = col_stats.get('unique', 'N/A')
                            top_val = col_stats.get('top', 'N/A')
                            freq_val = col_stats.get('freq', 'N/A')
                            markdown += f"- `{col_name}`: {unique_count} unique values, Most common: '{top_val}' (appears {freq_val} times)\n"

            else:
                markdown += f"- **Status**: ERROR\n"
                markdown += f"- **Error**: {result.get('error', 'Unknown error')}\n"

            markdown += "\n"

        # Recommendations
        if recommendations:
            markdown += "---\n\n## 🎯 Recommendations\n\n"
            for i, recommendation in enumerate(recommendations, 1):
                markdown += f"{i}. **{recommendation['title']}**\n"
                markdown += f"   - {recommendation['description']}\n"
                markdown += f"   - *Priority*: {recommendation['priority']}\n\n"

        # Footer
        markdown += f"""---

## Report Generation
- **Generated by**: LLM Data Quality Agent
- **Generation Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Report Format**: Markdown

*This report was automatically generated. Please review recommendations and take appropriate action.*
"""

        return markdown

    def render_html_template(self, assessment_results: Dict[str, Any]) -> str:
        """Generate an HTML report from assessment results."""
        metadata = assessment_results['metadata']
        check_results = assessment_results['check_results']
        summary = assessment_results['summary']
        recommendations = assessment_results.get('recommendations', [])

        # CSS styles
        css_styles = """
        <style>
            body { font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }
            .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
            .summary-card { background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; border-left: 4px solid #007bff; }
            .passed { border-left-color: #28a745; }
            .failed { border-left-color: #dc3545; }
            .error { border-left-color: #ffc107; }
            .check-result { background: white; border: 1px solid #e9ecef; border-radius: 8px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .check-title { font-size: 1.2em; font-weight: bold; margin-bottom: 10px; }
            .status-pass { color: #28a745; }
            .status-fail { color: #dc3545; }
            .status-error { color: #ffc107; }
            .recommendations { background: #e8f4f8; padding: 20px; border-radius: 8px; margin: 20px 0; }
            .recommendation-item { margin: 15px 0; padding: 15px; background: white; border-radius: 5px; }
            .priority-high { border-left: 4px solid #dc3545; }
            .priority-medium { border-left: 4px solid #ffc107; }
            .priority-low { border-left: 4px solid #28a745; }
            .metadata-table { width: 100%; border-collapse: collapse; margin: 20px 0; }
            .metadata-table th, .metadata-table td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
            .metadata-table th { background: #f8f9fa; }
            code { background: #f8f9fa; padding: 2px 4px; border-radius: 3px; font-family: 'Courier New', monospace; }
        </style>
        """

        # Header
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Assessment Report</title>
    {css_styles}
</head>
<body>
    <div class="header">
        <h1>📊 Data Quality Assessment Report</h1>
        <p>Comprehensive analysis of dataset: <code>{metadata['dataset_id']}</code></p>
    </div>

    <div class="summary-grid">
        <div class="summary-card passed">
            <h3>✅ Passed</h3>
            <div style="font-size: 2em; font-weight: bold;">{summary['passed_checks']}</div>
            <p>Checks passed</p>
        </div>
        <div class="summary-card failed">
            <h3>❌ Failed</h3>
            <div style="font-size: 2em; font-weight: bold;">{summary['failed_checks']}</div>
            <p>Checks failed</p>
        </div>
        <div class="summary-card error">
            <h3>🚫 Errors</h3>
            <div style="font-size: 2em; font-weight: bold;">{summary['error_checks']}</div>
            <p>Checks errored</p>
        </div>
    </div>

    <h2>📋 Dataset Information</h2>
    <table class="metadata-table">
        <tr><th>Property</th><th>Value</th></tr>
        <tr><td>Dataset</td><td><code>{metadata['dataset_id']}</code></td></tr>
        <tr><td>Connector</td><td>{metadata['connector_type'].upper()}</td></tr>
        <tr><td>Assessment Date</td><td>{datetime.fromisoformat(metadata['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
        <tr><td>Total Checks</td><td>{metadata['total_checks']}</td></tr>
    </table>

    <h2>🔍 Detailed Check Results</h2>
"""

        # Individual check results
        for check_name, result in check_results.items():
            # Determine status class and emoji based on actual results
            if result['status'] == 'success':
                if check_name == 'duplicates' and result.get('duplicate_qty', 0) == 0:
                    status_class = "status-pass"
                    status_emoji = "✅"
                elif check_name == 'null_values' and result.get('columns_with_nulls', 0) == 0:
                    status_class = "status-pass"
                    status_emoji = "✅"
                elif check_name == 'descriptive_stats':
                    status_class = "status-pass"
                    status_emoji = "✅"
                else:
                    status_class = "status-fail"
                    status_emoji = "❌"
            elif result['status'] == 'failure':
                status_class = "status-fail"
                status_emoji = "❌"
            else:  # error status
                status_class = "status-error"
                status_emoji = "❌"

            html += f"""
    <div class="check-result">
        <div class="check-title {status_class}">
            {status_emoji} {check_name.replace('_', ' ').title()}
        </div>
"""

            if result['status'] == 'success' or (result['status'] == 'failure' and check_name == 'duplicates'):
                if check_name == 'duplicates':
                    duplicate_qty = result.get('duplicate_qty', 0)
                    total_rows = result.get('total_rows', 0)
                    duplicate_percentage = (duplicate_qty / total_rows * 100) if total_rows > 0 else 0
                    status_text = "PASS" if duplicate_qty == 0 else "FAIL"
                    status_class = "status-pass" if duplicate_qty == 0 else "status-fail"
                    html += f"""
        <p><strong>Status:</strong> <span class="{status_class}">{status_text}</span></p>
        <p><strong>Total Rows:</strong> {total_rows:,}</p>
        <p><strong>Duplicate Records:</strong> {duplicate_qty:,} ({duplicate_percentage:.2f}% of data)</p>
"""
                    if duplicate_qty > 0:
                        html += f"<p><strong>Impact:</strong> Data redundancy detected - {duplicate_qty:,} rows are duplicated</p>"
                        duplicate_examples = result.get('duplicate_examples', [])
                        if duplicate_examples:
                            html += "<p><strong>Sample Duplicate Records:</strong></p><ul>"
                            for i, example in enumerate(duplicate_examples[:3], 1):  # Show first 3 examples
                                html += f"<li>Row pattern appears {example.get('count', 1)} times</li>"
                            html += "</ul>"
                    else:
                        html += "<p><strong>Quality:</strong> <span style='color: green;'>Excellent - No duplicate records found</span></p>"

                elif check_name == 'null_values' and result['status'] == 'success':
                    total_rows = result.get('total_rows', 0)
                    columns_with_nulls = result.get('columns_with_nulls', 0)
                    total_columns = result.get('total_columns', 0)
                    status_text = "PASS" if columns_with_nulls == 0 else "FAIL"
                    status_class = "status-pass" if columns_with_nulls == 0 else "status-fail"
                    html += f"""
        <p><strong>Status:</strong> <span class="{status_class}">{status_text}</span></p>
        <p><strong>Dataset Size:</strong> {total_rows:,} rows × {total_columns} columns</p>
        <p><strong>Columns with Missing Data:</strong> {columns_with_nulls} out of {total_columns} columns</p>
"""

                    if columns_with_nulls > 0:
                        null_coverage = (columns_with_nulls / total_columns * 100) if total_columns > 0 else 0
                        html += f"<p><strong>Missing Data Coverage:</strong> {null_coverage:.1f}% of columns affected</p>"

                        null_analysis = result.get('null_analysis', [])
                        if null_analysis:
                            html += "<p><strong>Detailed Null Analysis:</strong></p><ul>"
                            for col_info in null_analysis:
                                col_name = col_info['column_name']
                                null_count = col_info['null_count']
                                null_pct = col_info['null_percentage']
                                html += f"<li><code>{col_name}</code>: {null_count:,} nulls ({null_pct:.1f}%)</li>"
                            html += "</ul>"
                    else:
                        html += "<p><strong>Quality:</strong> <span style='color: green;'>Excellent - No missing values detected in any column</span></p>"

                elif check_name == 'descriptive_stats':
                    stats = result.get('descriptive_stats', {})
                    html += f"""
        <p><strong>Status:</strong> <span class="status-pass">PASS (Statistics Generated)</span></p>
        <p><strong>Columns Analyzed:</strong> {len(stats)} columns</p>
"""

                    # Show key statistics for all columns (without classification)
                    html += "<p><strong>Column Statistics (Count, Unique, Top, Freq, Min, Max):</strong></p>"
                    html += "<div style='max-height: 400px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; margin: 10px 0;'>"
                    html += "<table style='width: 100%; border-collapse: collapse;'>"
                    html += "<tr style='background: #f8f9fa; font-weight: bold;'>"
                    html += "<th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Column</th>"
                    html += "<th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Count</th>"
                    html += "<th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Unique</th>"
                    html += "<th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Top</th>"
                    html += "<th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Freq</th>"
                    html += "<th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Min</th>"
                    html += "<th style='border: 1px solid #ddd; padding: 8px; text-align: left;'>Max</th>"
                    html += "</tr>"

                    for col_name, col_stats in stats.items():
                        count_val = col_stats.get('count', 'N/A')
                        unique_val = col_stats.get('unique', 'N/A')
                        top_val = col_stats.get('top', 'N/A')
                        freq_val = col_stats.get('freq', 'N/A')
                        min_val = col_stats.get('min', 'N/A')
                        max_val = col_stats.get('max', 'N/A')

                        # Format numeric values for better readability
                        if isinstance(count_val, (int, float)) and count_val != 'N/A':
                            count_val = f"{count_val:,.0f}" if count_val == int(count_val) else f"{count_val:,.2f}"
                        if isinstance(min_val, (int, float)) and min_val != 'N/A':
                            min_val = f"{min_val:,.2f}" if abs(min_val) < 1000 else f"{min_val:,.0f}"
                        if isinstance(max_val, (int, float)) and max_val != 'N/A':
                            max_val = f"{max_val:,.2f}" if abs(max_val) < 1000 else f"{max_val:,.0f}"

                        # Truncate long text values for display
                        if isinstance(top_val, str) and len(top_val) > 30:
                            top_val = top_val[:27] + "..."

                        html += f"<tr>"
                        html += f"<td style='border: 1px solid #ddd; padding: 8px;'><code>{col_name}</code></td>"
                        html += f"<td style='border: 1px solid #ddd; padding: 8px;'>{count_val}</td>"
                        html += f"<td style='border: 1px solid #ddd; padding: 8px;'>{unique_val}</td>"
                        html += f"<td style='border: 1px solid #ddd; padding: 8px;'>{top_val}</td>"
                        html += f"<td style='border: 1px solid #ddd; padding: 8px;'>{freq_val}</td>"
                        html += f"<td style='border: 1px solid #ddd; padding: 8px;'>{min_val}</td>"
                        html += f"<td style='border: 1px solid #ddd; padding: 8px;'>{max_val}</td>"
                        html += f"</tr>"

                    html += "</table>"
                    html += "</div>"

            else:
                html += f"""
        <p><strong>Status:</strong> <span class="status-error">ERROR</span></p>
        <p><strong>Error:</strong> {result.get('error', 'Unknown error')}</p>
"""

            html += "    </div>\n"

        # Recommendations
        if recommendations:
            html += """
    <div class="recommendations">
        <h2>🎯 Recommendations</h2>
"""
            for i, recommendation in enumerate(recommendations, 1):
                priority_class = f"priority-{recommendation['priority'].lower()}"
                html += f"""
        <div class="recommendation-item {priority_class}">
            <h4>{i}. {recommendation['title']}</h4>
            <p>{recommendation['description']}</p>
            <p><em>Priority: {recommendation['priority']}</em></p>
        </div>
"""
            html += "    </div>\n"

        # Footer
        html += f"""
    <div style="margin-top: 40px; padding: 20px; background: #f8f9fa; border-radius: 8px; text-align: center;">
        <p><strong>Report Generated by:</strong> LLM Data Quality Agent</p>
        <p><strong>Generation Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><em>This report was automatically generated. Please review recommendations and take appropriate action.</em></p>
    </div>
</body>
</html>
"""

        return html
