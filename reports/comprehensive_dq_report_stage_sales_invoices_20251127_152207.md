# Data Quality Assessment Report

## Dataset Information
- **Dataset**: `stage_sales.public.invoices`
- **Connector**: POSTGRES
- **Assessment Date**: 2025-11-27 15:22:22
- **Total Checks**: 3

## Executive Summary
- ✅ **Passed**: 1 checks
- ❌ **Failed**: 2 checks
- 🚫 **Errors**: 0 checks

---

## Detailed Check Results

### ❌ Duplicates

- **Status**: ERROR
- **Error**: Unknown error

### ✅ Null Values

- **Status**: FAIL
- **Dataset Size**: 200,000 rows × 20 columns
- **Columns with Missing Data**: 9 out of 20 columns
- **Missing Data Coverage**: 45.0% of columns affected

**Detailed Null Analysis:**
- `notes`: 149,954 nulls (75.0%)
- `discount_amount`: 129,896 nulls (65.0%)
- `customer_company`: 39,906 nulls (19.9%)
- `shipping_address`: 35,994 nulls (18.0%)
- `billing_address`: 29,786 nulls (14.9%)
- `due_date`: 23,790 nulls (11.9%)
- `payment_method`: 15,718 nulls (7.9%)
- `tax_rate`: 10,284 nulls (5.1%)
- `tax_amount`: 10,284 nulls (5.1%)

### ✅ Descriptive Stats

- **Status**: PASS (Statistics Generated)\n- **Columns Analyzed**: 20 columns\n\n**📊 Complete Statistical Summary:**\n\n| Column | Count | Mean | Std | Min | 25% | 50% | 75% | Max | Unique | Top | Freq |\n|--------|-------|------|-----|-----|-----|-----|-----|-----|--------|-----|------|\n| `invoice_id` | 200,000.00 | - | - | - | - | - | - | - | 99,999.00 | 1.00 | 4.00 |\n| `customer_id` | 200,000.00 | - | - | - | - | - | - | - | 39,775.00 | 8,339.00 | 22.00 |\n| `product_id` | 200,000.00 | - | - | - | - | - | - | - | 26,595.00 | 24,312.00 | 34.00 |\n| `invoice_date` | 200,000 | - | - | - | - | - | - | - | 734 | 2023-12-15 | 346 |\n| `due_date` | 176,210 | - | - | - | - | - | - | - | 775 | 2024-10-18 | 306 |\n| `quantity` | 200,000.00 | 230.20 | 1,822.65 | -10.00 | 3.00 | 6.00 | 8.00 | 15,000.00 | - | - | - |\n| `unit_price` | 200,000.00 | 1,206.30 | 8,407.38 | -100.00 | 126.07 | 253.78 | 380.76 | 75,000.00 | - | - | - |\n| `subtotal` | 200,000.00 | 320,459.82 | 17,066,686.36 | -1,500,000.00 | 420.41 | 1,069.94 | 2,176.91 | 1,125,000,000.00 | - | - | - |\n| `discount_amount` | 70,104.00 | 39,452.65 | 2,642,945.09 | -357,117.34 | 54.91 | 143.82 | 312.17 | 275,585,449.80 | - | - | - |\n| `tax_rate` | 189,716.00 | 1.15 | 9.48 | -10.00 | 0.07 | 0.10 | 0.13 | 75.00 | - | - | - |\n| `tax_amount` | 189,716.00 | -33,390.50 | 36,825,764.45 | -11,250,000,000.00 | 35.37 | 95.18 | 202.44 | 546,491,250.00 | - | - | - |\n| `total_amount` | 200,000.00 | 279,924.64 | 36,821,213.44 | -10,125,000,000.00 | 406.07 | 1,099.46 | 2,327.41 | 1,290,487,500.00 | - | - | - |\n| `invoice_status` | 200,000 | - | - | - | - | - | - | - | 6 | Overdue | 33,530 |\n| `payment_method` | 184,282 | - | - | - | - | - | - | - | 6 | Bank Transfer | 31,148 |\n| `customer_company` | 160,094 | - | - | - | - | - | - | - | 52103 | Smith Group | 214 |\n| `billing_address` | 170,214 | - | - | - | - | - | - | - | 78312 | 181 Johnson Cou | 4 |\n| `shipping_address` | 164,006 | - | - | - | - | - | - | - | 75438 | 79402 Peterson  | 4 |\n| `notes` | 50,046 | - | - | - | - | - | - | - | 23054 | Act support sen | 4 |\n| `created_at` | 200,000 | 2025-05-20 12:4 | - | 2023-11-21 14:1 | 2025-02-12 08:0 | 2025-07-05 11:1 | 2025-10-01 12:5 | 2025-11-20 11:1 | - | - | - |\n| `updated_at` | 200,000 | 2025-05-20 20:3 | - | 2023-11-20 07:0 | 2025-02-12 17:5 | 2025-07-05 18:3 | 2025-09-30 23:0 | 2025-11-20 11:1 | - | - | - |\n
---

## Recommendations

1. **Address High Null Value Columns**
   - Columns `notes`, `discount_amount` have >20% null values. Consider: 1) Data source validation, 2) Default value strategies, 3) Imputation techniques, or 4) Column removal if not critical.
   - *Priority*: High

2. **Establish Data Quality Framework**
   - Only 33% of checks passed. Consider implementing: 1) Data validation at ingestion, 2) Regular quality monitoring, 3) Data stewardship roles, 4) Quality metrics dashboards.
   - *Priority*: High

3. **Implement Null Handling Strategy**
   - Columns `customer_company`, `shipping_address`, `billing_address` have 5-20% null values. Consider implementing NOT NULL constraints, data validation rules, or imputation strategies based on business logic.
   - *Priority*: Medium

4. **Comprehensive Data Governance**
   - 9 columns have null values. Implement data governance policies including mandatory field validation, data entry training, and upstream data quality monitoring.
   - *Priority*: Medium

5. **Investigate High Variability in quantity**
   - Column `quantity` has high variability (CV=7.92). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

6. **Investigate High Variability in unit_price**
   - Column `unit_price` has high variability (CV=6.97). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

7. **Investigate High Variability in subtotal**
   - Column `subtotal` has high variability (CV=53.26). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

8. **Investigate High Variability in discount_amount**
   - Column `discount_amount` has high variability (CV=66.99). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

9. **Investigate High Variability in tax_rate**
   - Column `tax_rate` has high variability (CV=8.28). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

10. **Investigate High Variability in tax_amount**
   - Column `tax_amount` has high variability (CV=1102.88). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

11. **Investigate High Variability in total_amount**
   - Column `total_amount` has high variability (CV=131.54). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

12. **Data Documentation and Profiling**
   - Dataset has 20 columns. Consider creating data dictionaries, establishing column-level monitoring, and implementing regular statistical profiling to track data drift over time.
   - *Priority*: Low

---

## Report Generation
- **Generated by**: LLM Data Quality Agent
- **Generation Time**: 2025-11-27 15:22:22
- **Report Format**: Markdown

*This report was automatically generated. Please review recommendations and take appropriate action.*
