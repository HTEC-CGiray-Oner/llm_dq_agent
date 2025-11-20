# src/reporting/__init__.py
"""
Data Quality Reporting Module

This module provides comprehensive data quality reporting capabilities including:
- Execution tracking of data quality checks
- Pass/Fail status analysis
- Summary statistics generation
- Remediation recommendations
- Multiple output formats (Markdown, HTML, JSON)
"""

from .report_generator import DataQualityReportGenerator
from .report_templates import ReportTemplates
from .remediation_advisor import RemediationAdvisor

__all__ = [
    'DataQualityReportGenerator',
    'ReportTemplates',
    'RemediationAdvisor'
]
