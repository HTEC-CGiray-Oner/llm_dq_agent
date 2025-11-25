# Example script to generate comprehensive data quality reports
"""
This script demonstrates how to use the optimized reporting approach to generate
comprehensive data quality reports in multiple formats efficiently.
"""
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.reporting import DataQualityReportGenerator
from src.data_quality.checks import check_dataset_duplicates, check_dataset_null_values, check_dataset_descriptive_stats


def main():
    """Generate sample data quality reports using optimized workflow."""
    print("🚀 Starting Optimized Data Quality Report Generation...")

    # Initialize the report generator
    generator = DataQualityReportGenerator()

    # Step 1: Execute DQ checks once
    print("\n📊 Running DQ checks on PostgreSQL customers table...")

    dataset_id = "stage_sales.public.customers"
    connector_type = "postgres"

    check_functions = {
        'duplicates': check_dataset_duplicates,
        'null_values': check_dataset_null_values,
        'descriptive_stats': check_dataset_descriptive_stats
    }

    check_results = {}
    for check_name, check_function in check_functions.items():
        print(f"   Running {check_name} check...")
        check_results[check_name] = check_function(dataset_id, connector_type=connector_type)

    # Step 2: Create assessment from cached results
    print("\nCreating assessment structure...")
    assessment_results = generator.create_assessment_from_results(
        check_results=check_results,
        dataset_id=dataset_id,
        connector_type=connector_type
    )

    # Step 3: Save reports in all formats from cached results
    print("\n💾 Saving reports in multiple formats...")
    saved_files = generator.save_report(
        assessment_results=assessment_results,
        output_dir="./reports",
        formats=['markdown', 'html', 'json']
    )

    print("\n✅ Optimized Report Generation Complete!")
    print("📁 Generated files:")
    for format_name, file_path in saved_files.items():
        print(f"   {format_name.upper()}: {file_path}")

    # Display summary
    summary = assessment_results['summary']
    print(f"\n📈 Assessment Summary:")
    print(f"   ✅ Passed: {summary['passed_checks']}")
    print(f"   ❌ Failed: {summary['failed_checks']}")
    print(f"   🚫 Errors: {summary['error_checks']}")

    # Show recommendations
    recommendations = assessment_results.get('recommendations', [])
    if recommendations:
        print(f"\nTop Recommendations:")
        for i, rec in enumerate(recommendations[:3], 1):
            print(f"   {i}. [{rec['priority']}] {rec['title']}")

    print(f"\n🚀 Performance Benefits:")
    print(f"   ✅ DQ checks executed once only")
    print(f"   ✅ Multiple reports generated from cached results")
    print(f"   ✅ No duplicate database connections or data loading")


if __name__ == '__main__':
    main()
        print(f"\nKey Recommendations ({len(recommendations)}):")
        for i, rec in enumerate(recommendations[:3], 1):  # Show top 3
            print(f"   {i}. [{rec['priority']}] {rec['title']}")


if __name__ == "__main__":
    main()
