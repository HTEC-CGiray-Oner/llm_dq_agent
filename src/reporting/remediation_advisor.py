# src/reporting/remediation_advisor.py
"""
Intelligent remediation advisor that provides actionable recommendations based on data quality check results.
"""
from typing import Dict, List, Any


class RemediationAdvisor:
    """
    Generates intelligent recommendations for data quality remediation based on check results.
    """

    def generate_recommendations(self, check_results: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Generate actionable recommendations based on data quality check results.

        Args:
            check_results: Dictionary containing results from all executed checks

        Returns:
            List of recommendation dictionaries with title, description, and priority
        """
        recommendations = []

        # Analyze duplicate check results
        if 'duplicates' in check_results:
            duplicate_result = check_results['duplicates']
            if duplicate_result.get('status') == 'success':
                duplicate_qty = duplicate_result.get('duplicate_qty', 0)
                if duplicate_qty > 0:
                    recommendations.append({
                        'title': 'Remove Duplicate Records',
                        'description': f'Found {duplicate_qty:,} duplicate records. Consider implementing deduplication logic using business keys or primary key constraints. Use SQL DISTINCT, ROW_NUMBER() window functions, or unique constraints.',
                        'priority': 'High' if duplicate_qty > 1000 else 'Medium'
                    })

        # Analyze null values check results
        if 'null_values' in check_results:
            null_result = check_results['null_values']
            if null_result.get('status') == 'success':
                columns_with_nulls = null_result.get('columns_with_nulls', 0)
                null_analysis = null_result.get('null_analysis', [])

                if columns_with_nulls > 0:
                    high_null_columns = [col for col in null_analysis if col['null_percentage'] > 20]
                    medium_null_columns = [col for col in null_analysis if 5 < col['null_percentage'] <= 20]

                    if high_null_columns:
                        col_names = ', '.join([f"`{col['column_name']}`" for col in high_null_columns[:3]])
                        recommendations.append({
                            'title': 'Address High Null Value Columns',
                            'description': f'Columns {col_names} have >20% null values. Consider: 1) Data source validation, 2) Default value strategies, 3) Imputation techniques, or 4) Column removal if not critical.',
                            'priority': 'High'
                        })

                    if medium_null_columns:
                        col_names = ', '.join([f"`{col['column_name']}`" for col in medium_null_columns[:3]])
                        recommendations.append({
                            'title': 'Implement Null Handling Strategy',
                            'description': f'Columns {col_names} have 5-20% null values. Consider implementing NOT NULL constraints, data validation rules, or imputation strategies based on business logic.',
                            'priority': 'Medium'
                        })

                    if len(null_analysis) > 5:
                        recommendations.append({
                            'title': 'Comprehensive Data Governance',
                            'description': f'{len(null_analysis)} columns have null values. Implement data governance policies including mandatory field validation, data entry training, and upstream data quality monitoring.',
                            'priority': 'Medium'
                        })

        # Analyze descriptive statistics results
        if 'descriptive_stats' in check_results:
            stats_result = check_results['descriptive_stats']
            if stats_result.get('status') == 'success':
                stats = stats_result.get('descriptive_stats', {})

                # Check for potential outliers or data anomalies
                outlier_recommendations = self._analyze_statistical_outliers(stats)
                recommendations.extend(outlier_recommendations)

                # General data profiling recommendations
                if len(stats) > 10:
                    recommendations.append({
                        'title': 'Data Documentation and Profiling',
                        'description': f'Dataset has {len(stats)} columns. Consider creating data dictionaries, establishing column-level monitoring, and implementing regular statistical profiling to track data drift over time.',
                        'priority': 'Low'
                    })

        # Check for missing or errored checks
        error_checks = [name for name, result in check_results.items() if result.get('status') == 'error']
        if error_checks:
            recommendations.append({
                'title': 'Fix Check Execution Errors',
                'description': f'Data quality checks {", ".join(error_checks)} failed to execute. Review database connectivity, permissions, and data availability. Ensure stable monitoring infrastructure.',
                'priority': 'High'
            })

        # Overall data quality maturity recommendations
        total_checks = len(check_results)
        passed_checks = sum(1 for result in check_results.values()
                          if result.get('status') == 'success' and self._is_check_passed(result))

        if total_checks > 0:
            pass_rate = passed_checks / total_checks
            if pass_rate < 0.5:
                recommendations.append({
                    'title': 'Establish Data Quality Framework',
                    'description': f'Only {pass_rate:.0%} of checks passed. Consider implementing: 1) Data validation at ingestion, 2) Regular quality monitoring, 3) Data stewardship roles, 4) Quality metrics dashboards.',
                    'priority': 'High'
                })
            elif pass_rate < 0.8:
                recommendations.append({
                    'title': 'Enhance Data Quality Processes',
                    'description': f'{pass_rate:.0%} pass rate indicates room for improvement. Implement automated quality gates, exception handling, and proactive monitoring for sustained quality.',
                    'priority': 'Medium'
                })

        # Sort by priority (High -> Medium -> Low)
        priority_order = {'High': 1, 'Medium': 2, 'Low': 3}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 4))

        return recommendations

    def _analyze_statistical_outliers(self, stats: Dict[str, Any]) -> List[Dict[str, str]]:
        """Analyze statistical data for potential outliers and anomalies."""
        recommendations = []

        for column_name, column_stats in stats.items():
            if not isinstance(column_stats, dict):
                continue

            # Check for extreme standard deviation (potential outliers)
            mean_val = column_stats.get('mean')
            std_val = column_stats.get('std')

            if mean_val is not None and std_val is not None and mean_val != 0:
                cv = std_val / abs(mean_val)  # Coefficient of variation
                if cv > 2:  # High variability
                    recommendations.append({
                        'title': f'Investigate High Variability in {column_name}',
                        'description': f'Column `{column_name}` has high variability (CV={cv:.2f}). Review for outliers, data entry errors, or consider data transformation/normalization techniques.',
                        'priority': 'Low'
                    })

            # Check for potential categorical data issues
            unique_count = column_stats.get('unique')
            count = column_stats.get('count')

            if unique_count is not None and count is not None and count > 0:
                # Convert to numeric values to handle string inputs
                try:
                    unique_count = int(float(unique_count))
                    count = int(float(count))
                    uniqueness_ratio = unique_count / count
                except (ValueError, TypeError):
                    continue  # Skip this column if conversion fails
                if uniqueness_ratio > 0.95 and unique_count > 100:  # Too many unique values
                    recommendations.append({
                        'title': f'High Cardinality in {column_name}',
                        'description': f'Column `{column_name}` has very high cardinality ({unique_count:,} unique values). Consider data normalization, categorization, or indexing strategies for performance.',
                        'priority': 'Low'
                    })

        return recommendations

    def _is_check_passed(self, check_result: Dict[str, Any]) -> bool:
        """Determine if a check result represents a passing status."""
        if check_result.get('status') != 'success':
            return False

        # Check-specific pass criteria
        if 'duplicate_qty' in check_result:
            return check_result['duplicate_qty'] == 0

        if 'columns_with_nulls' in check_result:
            return check_result['columns_with_nulls'] == 0

        if 'descriptive_stats' in check_result:
            return True  # Descriptive stats always pass if successful

        return True  # Default to pass for unknown check types


if __name__ == '__main__':
    # Example usage
    advisor = RemediationAdvisor()

    sample_results = {
        'duplicates': {'status': 'success', 'duplicate_qty': 1500},
        'null_values': {
            'status': 'success',
            'columns_with_nulls': 3,
            'null_analysis': [
                {'column_name': 'email', 'null_percentage': 25.0},
                {'column_name': 'phone', 'null_percentage': 15.0}
            ]
        }
    }

    recommendations = advisor.generate_recommendations(sample_results)
    for rec in recommendations:
        print(f"[{rec['priority']}] {rec['title']}: {rec['description']}")
