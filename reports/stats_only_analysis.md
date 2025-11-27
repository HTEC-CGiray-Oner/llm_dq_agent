# Descriptive Statistics Analysis Report
Generated: 2025-11-27 13:54:05.763074
Query: Check descriptive statistics for prod customers table

## Results:
### Descriptive Statistics for Table: `PROD_SALES.DATA_MART_MARKETING.CUSTOMERS`

Below is the detailed statistical breakdown for the `CUSTOMERS` table:

#### Numerical Columns:
| Column Name       | Count   | Mean       | Std Dev    | Min   | 25%   | 50%   | 75%   | Max   |
|-------------------|---------|------------|------------|-------|-------|-------|-------|-------|
| ZIP_CODE          | 37,811  | 50,206.64  | 28,724.82  | 504   | 25,341| 50,261| 75,131| 99,945|
| AGE               | 37,811  | 51.45      | 19.64      | 18    | 34    | 51    | 68    | 85    |
| INCOME            | 37,811  | 114,325.32 | 45,228.85  | 4,171 | 80,369| 114,162|146,245|255,659|
| CREDIT_SCORE      | 37,811  | 575.36     | 159.33     | 300   | 437   | 575   | 714   | 850   |

#### Categorical Columns:
| Column Name       | Count   | Unique | Top Value                  | Frequency |
|-------------------|---------|--------|----------------------------|-----------|
| CUSTOMER_ID       | 37,811  | 36,483 | 718                        | 3         |
| FIRST_NAME        | 37,811  | 690    | Michael                    | 846       |
| LAST_NAME         | 37,811  | 1,000  | Smith                      | 863       |
| EMAIL             | 37,811  | 34,131 | pbrown@example.com         | 8         |
| PHONE             | 32,017  | 31,315 | +1-493-339-0626x16177      | 3         |
| ADDRESS           | 34,681  | 33,731 | 68814 Cooper Lane, Raymondside, SD 84458 | 3         |
| CITY              | 37,811  | 20,395 | West Michael               | 39        |
| STATE             | 37,811  | 50     | Alaska                     | 801       |
| COUNTRY           | 37,811  | 243    | Korea                      | 345       |
| DATE_OF_BIRTH     | 37,811  | 19,125 | 1964-12-02                 | 10        |
| GENDER            | 37,811  | 3      | Other                      | 12,639    |
| COMPANY           | 30,884  | 23,649 | Smith and Sons             | 45        |
| JOB_TITLE         | 37,811  | 639    | Commercial art gallery manager | 93        |
| ACCOUNT_BALANCE   | 37,811  | 36,362 | 48185.59                   | 3         |
| REGISTRATION_DATE | 37,811  | 1,827  | 2024-08-23                 | 42        |
| LAST_LOGIN        | 37,811  | 90     | 2025-10-09                 | 470       |
| IS_ACTIVE         | 37,811  | 2      | False                      | 18,926    |
| CUSTOMER_SEGMENT  | 37,811  | 4      | Standard                   | 9,531     |

#### Observations:
- **Numerical Data**: Columns like `AGE`, `INCOME`, and `CREDIT_SCORE` show a wide range of values, with `INCOME` having the highest variability.
- **Categorical Data**: The most frequent values for columns like `FIRST_NAME`, `LAST_NAME`, and `CUSTOMER_SEGMENT` indicate common patterns in customer demographics.
- **Missing Data**: Some columns like `PHONE` and `COMPANY` have fewer counts compared to the total rows, indicating missing values.

Let me know if you need further analysis or additional checks!
