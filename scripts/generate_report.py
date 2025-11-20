# Example script to generate comprehensive data quality reports
"""
This script demonstrates how to use the reporting module to generate
comprehensive data quality reports in multiple formats.
"""
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.reporting import DataQualityReportGenerator


def main():
    """Generate sample data quality reports."""
    print("🚀 Starting Data Quality Report Generation...")

    # Initialize the report generator
    generator = DataQualityReportGenerator()

    # Run comprehensive assessment on PostgreSQL customers table
    print("\n📊 Running comprehensive assessment on PostgreSQL customers table...")
    results = generator.run_full_assessment(
        dataset_id="stage_sales.public.customers",
        connector_type="postgres"
    )

    # Save reports in all formats
    print("\n💾 Saving reports in multiple formats...")
    saved_files = generator.save_report(
        assessment_results=results,
        output_dir="./reports",
        formats=['markdown', 'html', 'json']
    )

    print("\n✅ Report Generation Complete!")
    print("📁 Generated files:")
    for format_name, file_path in saved_files.items():
        print(f"   {format_name.upper()}: {file_path}")

    # Display summary
    summary = results['summary']
    print(f"\n📈 Assessment Summary:")
    print(f"   ✅ Passed: {summary['passed_checks']}")
    print(f"   ❌ Failed: {summary['failed_checks']}")
    print(f"   🚫 Errors: {summary['error_checks']}")

    # Show recommendations
    recommendations = results.get('recommendations', [])
    if recommendations:
        print(f"\n🎯 Key Recommendations ({len(recommendations)}):")
        for i, rec in enumerate(recommendations[:3], 1):  # Show top 3
            print(f"   {i}. [{rec['priority']}] {rec['title']}")


if __name__ == "__main__":
    main()
