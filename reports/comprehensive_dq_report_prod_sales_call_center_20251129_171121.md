# Data Quality Assessment Report

## Dataset Information
- **Dataset**: `PROD_SALES.DATA_MART_MARKETING.CALL_CENTER`
- **Connector**: SNOWFLAKE
- **Assessment Date**: 2025-11-29 17:11:26
- **Total Checks**: 3

## Executive Summary
- ✅ **Passed**: 2 checks
- ❌ **Failed**: 1 checks
- 🚫 **Errors**: 0 checks

---

## Detailed Check Results

### ✅ Duplicates

- **Status**: PASS
- **Total Rows**: 60
- **Duplicate Records**: 0 (0.00% of data)
- **Quality**: Excellent - No duplicate records found

### ✅ Null Values

- **Status**: FAIL
- **Dataset Size**: 60 rows × 31 columns
- **Columns with Missing Data**: 2 out of 31 columns
- **Missing Data Coverage**: 6.5% of columns affected

**Detailed Null Analysis:**
- `CC_CLOSED_DATE_SK`: 60 nulls (100.0%)
- `CC_REC_END_DATE`: 30 nulls (50.0%)

### ✅ Descriptive Stats

- **Status**: PASS (Statistics Generated)\n- **Columns Analyzed**: 31 columns\n\n**📊 Complete Statistical Summary:**\n\n| Column | Count | Mean | Std | Min | 25% | 50% | 75% | Max | Unique | Top | Freq |\n|--------|-------|------|-----|-----|-----|-----|-----|-----|--------|-----|------|\n| `CC_CALL_CENTER_SK` | 60.00 | 30.50 | 17.46 | 1.00 | 15.75 | 30.50 | 45.25 | 60.00 | - | - | - |\n| `CC_CALL_CENTER_ID` | 60 | - | - | - | - | - | - | - | 30 | AAAAAAAAABAAAAA | 3 |\n| `CC_REC_START_DATE` | 60 | - | - | - | - | - | - | - | 4 | 1998-01-01 | 30 |\n| `CC_REC_END_DATE` | 30 | - | - | - | - | - | - | - | 3 | 2000-12-31 | 10 |\n| `CC_CLOSED_DATE_SK` | 0 | - | - | - | - | - | - | - | 0 | - | - |\n| `CC_OPEN_DATE_SK` | 60.00 | 2,451,003.92 | 109.55 | 2,450,794.00 | 2,450,938.00 | 2,451,059.00 | 2,451,082.50 | 2,451,146.00 | - | - | - |\n| `CC_NAME` | 60 | - | - | - | - | - | - | - | 30 | Hawaii/Alaska_4 | 3 |\n| `CC_CLASS` | 60 | - | - | - | - | - | - | - | 3 | medium | 26 |\n| `CC_EMPLOYEES` | 60.00 | 601,547,103.23 | 336,928,994.64 | 5,412,266.00 | 371,533,763.75 | 628,355,057.00 | 886,144,828.50 | 1,275,612,983.00 | - | - | - |\n| `CC_SQ_FT` | 60.00 | 156,394,745.05 | 1,300,064,061.90 | -2,108,783,316.00 | -684,143,870.50 | 475,079,344.00 | 1,081,694,084.50 | 2,108,552,432.00 | - | - | - |\n| `CC_HOURS` | 60 | - | - | - | - | - | - | - | 3 | 8AM-4PM | 48 |\n| `CC_MANAGER` | 60 | - | - | - | - | - | - | - | 42 | Kendall Jones | 3 |\n| `CC_MKT_ID` | 60.00 | - | - | - | - | - | - | - | 6.00 | 6.00 | 16.00 |\n| `CC_MKT_CLASS` | 60 | - | - | - | - | - | - | - | 52 | Citizens change | 2 |\n| `CC_MKT_DESC` | 60 | - | - | - | - | - | - | - | 48 | New, surprised  | 3 |\n| `CC_MARKET_MANAGER` | 60 | - | - | - | - | - | - | - | 48 | Todd Smith | 3 |\n| `CC_DIVISION` | 60.00 | 3.42 | 1.68 | 1.00 | 2.00 | 3.50 | 5.00 | 6.00 | - | - | - |\n| `CC_DIVISION_NAME` | 60 | - | - | - | - | - | - | - | 6 | able | 19 |\n| `CC_COMPANY` | 60.00 | 3.68 | 1.62 | 1.00 | 2.75 | 4.00 | 5.00 | 6.00 | - | - | - |\n| `CC_COMPANY_NAME` | 60 | - | - | - | - | - | - | - | 6 | anti | 15 |\n| `CC_STREET_NUMBER` | 60.00 | 568.60 | 286.29 | 38.00 | 406.00 | 611.00 | 772.25 | 999.00 | - | - | - |\n| `CC_STREET_NAME` | 60 | - | - | - | - | - | - | - | 29 | Main | 4 |\n| `CC_STREET_TYPE` | 60 | - | - | - | - | - | - | - | 14 | Ct. | 12 |\n| `CC_SUITE_NUMBER` | 60 | - | - | - | - | - | - | - | 26 | Suite B | 6 |\n| `CC_CITY` | 60 | - | - | - | - | - | - | - | 25 | Buena Vista | 6 |\n| `CC_COUNTY` | 60 | - | - | - | - | - | - | - | 25 | Gage County | 5 |\n| `CC_STATE` | 60 | - | - | - | - | - | - | - | 19 | TN | 13 |\n| `CC_ZIP` | 60.00 | 49,001.05 | 22,021.33 | 9,903.00 | 33,451.00 | 45,906.00 | 66,614.00 | 96,192.00 | - | - | - |\n| `CC_COUNTRY` | 60 | - | - | - | - | - | - | - | 1 | United States | 60 |\n| `CC_GMT_OFFSET` | 60 | - | - | - | - | - | - | - | 4 | -5.00 | 29 |\n| `CC_TAX_PERCENTAGE` | 60 | - | - | - | - | - | - | - | 13 | 0.11 | 10 |\n
---

## Recommendations

1. **Address High Null Value Columns**
   - Columns `CC_CLOSED_DATE_SK`, `CC_REC_END_DATE` have >20% null values. Consider: 1) Data source validation, 2) Default value strategies, 3) Imputation techniques, or 4) Column removal if not critical.
   - *Priority*: High

2. **Enhance Data Quality Processes**
   - 67% pass rate indicates room for improvement. Implement automated quality gates, exception handling, and proactive monitoring for sustained quality.
   - *Priority*: Medium

3. **Investigate High Variability in CC_CALL_CENTER_SK**
   - Column `CC_CALL_CENTER_SK` has high variability (CV=57.26). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

4. **Investigate High Variability in CC_EMPLOYEES**
   - Column `CC_EMPLOYEES` has high variability (CV=56.01). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

5. **Investigate High Variability in CC_SQ_FT**
   - Column `CC_SQ_FT` has high variability (CV=831.27). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

6. **Investigate High Variability in CC_DIVISION**
   - Column `CC_DIVISION` has high variability (CV=49.18). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

7. **Investigate High Variability in CC_COMPANY**
   - Column `CC_COMPANY` has high variability (CV=44.00). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

8. **Investigate High Variability in CC_STREET_NUMBER**
   - Column `CC_STREET_NUMBER` has high variability (CV=50.35). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

9. **Investigate High Variability in CC_ZIP**
   - Column `CC_ZIP` has high variability (CV=44.94). Review for outliers, data entry errors, or consider data transformation/normalization techniques.
   - *Priority*: Low

10. **Data Documentation and Profiling**
   - Dataset has 31 columns. Consider creating data dictionaries, establishing column-level monitoring, and implementing regular statistical profiling to track data drift over time.
   - *Priority*: Low

---

## Report Generation
- **Generated by**: LLM Data Quality Agent
- **Generation Time**: 2025-11-29 17:11:26
- **Report Format**: Markdown

*This report was automatically generated. Please review recommendations and take appropriate action.*
