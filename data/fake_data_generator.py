"""
Fake Data Generator for Testing Data Quality Checks

This module provides classes to generate large-scale fake datasets
with predefined data quality flaws for testing purposes.

Includes generators for:
- Customer data (FakeCustomerGenerator)
- Product data (FakeProductGenerator)
- Invoice data (FakeInvoiceGenerator) - with customer/product relationships
"""

from faker import Faker
import pandas as pd
import random
from typing import Optional, List, Dict
from datetime import datetime, timedelta


class FakeCustomerGenerator:
    """
    Generates fake customer datasets with configurable data quality issues.

    Features:
    - Configurable dataset size
    - Null value injection on specific columns
    - Duplicate record generation
    - Outlier value injection on numeric/date columns
    """

    def __init__(self, seed: Optional[int] = 42):
        """
        Initialize the fake data generator.

        Args:
            seed: Random seed for reproducibility
        """
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

    def generate_customers(
        self,
        num_records: int = 10000,
        duplicate_rate: float = 0.05,
        null_rates: Optional[Dict[str, float]] = None,
        outlier_config: Optional[Dict[str, dict]] = None
    ) -> pd.DataFrame:
        """
        Generate a fake customer dataset with data quality issues.

        Args:
            num_records: Number of customer records to generate
            duplicate_rate: Percentage of duplicate records (0.0 to 1.0)
            null_rates: Dictionary mapping column names to null percentages
                       Example: {'email': 0.1, 'phone': 0.15}
            outlier_config: Dictionary mapping column names to outlier configurations
                           Example: {'age': {'rate': 0.02, 'min': 150, 'max': 200},
                                    'income': {'rate': 0.03, 'value': -999}}

        Returns:
            DataFrame with customer data including quality issues
        """
        # Default null rates if not provided
        if null_rates is None:
            null_rates = {
                'email': 0.08,
                'phone': 0.10,
                'address': 0.05,
                'company': 0.12
            }

        # Default outlier config if not provided
        if outlier_config is None:
            outlier_config = {
                'age': {'rate': 0.02, 'min': 150, 'max': 999},
                'income': {'rate': 0.03, 'min': -50000, 'max': -1000},
                'account_balance': {'rate': 0.025, 'value': 999999999.99}
            }

        # Calculate how many unique records we need
        num_duplicates = int(num_records * duplicate_rate)
        num_unique = num_records - num_duplicates

        # Generate unique customer records
        customers = []
        for i in range(num_unique):
            customer = self._generate_single_customer(i + 1)
            customers.append(customer)

        # Create DataFrame
        df = pd.DataFrame(customers)

        # Add duplicate records (exact copies)
        if num_duplicates > 0:
            duplicate_indices = random.choices(range(len(df)), k=num_duplicates)
            duplicates = df.iloc[duplicate_indices].copy()
            df = pd.concat([df, duplicates], ignore_index=True)
            df = df.sample(frac=1).reset_index(drop=True)  # Shuffle

        # Inject null values
        df = self._inject_nulls(df, null_rates)

        # Inject outliers
        df = self._inject_outliers(df, outlier_config)

        return df

    def _generate_single_customer(self, customer_id: int) -> dict:
        """
        Generate a single customer record with realistic fake data.

        Args:
            customer_id: Unique customer identifier

        Returns:
            Dictionary containing customer information
        """
        # Generate registration date within last 5 years
        registration_date = self.fake.date_between(
            start_date='-5y',
            end_date='today'
        )

        # Generate last login within last 90 days
        last_login = self.fake.date_between(
            start_date='-90d',
            end_date='today'
        )

        # Generate age between 18 and 85
        age = random.randint(18, 85)

        # Generate income based on age (simplified correlation)
        base_income = random.randint(25000, 150000)
        income = base_income + (age - 30) * random.randint(500, 2000)
        income = max(0, income)  # Ensure non-negative

        # Generate account balance
        account_balance = round(random.uniform(-5000, 50000), 2)

        return {
            'customer_id': customer_id,
            'first_name': self.fake.first_name(),
            'last_name': self.fake.last_name(),
            'email': self.fake.email(),
            'phone': self.fake.phone_number(),
            'address': self.fake.address().replace('\n', ', '),
            'city': self.fake.city(),
            'state': self.fake.state(),
            'zip_code': self.fake.zipcode(),
            'country': self.fake.country(),
            'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=85),
            'age': age,
            'gender': random.choice(['M', 'F', 'Other']),
            'company': self.fake.company(),
            'job_title': self.fake.job(),
            'income': income,
            'account_balance': account_balance,
            'credit_score': random.randint(300, 850),
            'registration_date': registration_date,
            'last_login': last_login,
            'is_active': random.choice([True, False]),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic', 'Trial'])
        }

    def _inject_nulls(self, df: pd.DataFrame, null_rates: Dict[str, float]) -> pd.DataFrame:
        """
        Inject null values into specified columns.

        Args:
            df: DataFrame to modify
            null_rates: Dictionary mapping column names to null percentages

        Returns:
            Modified DataFrame with null values
        """
        df_copy = df.copy()

        for column, rate in null_rates.items():
            if column in df_copy.columns:
                # Calculate number of nulls to inject
                num_nulls = int(len(df_copy) * rate)
                if num_nulls > 0:
                    # Randomly select indices to set as null
                    null_indices = random.sample(range(len(df_copy)), num_nulls)
                    df_copy.loc[null_indices, column] = None

        return df_copy

    def _inject_outliers(self, df: pd.DataFrame, outlier_config: Dict[str, dict]) -> pd.DataFrame:
        """
        Inject outlier values into specified columns.

        Args:
            df: DataFrame to modify
            outlier_config: Dictionary mapping column names to outlier configurations

        Returns:
            Modified DataFrame with outlier values
        """
        df_copy = df.copy()

        for column, config in outlier_config.items():
            if column in df_copy.columns:
                rate = config.get('rate', 0.02)
                num_outliers = int(len(df_copy) * rate)

                if num_outliers > 0:
                    # Randomly select indices for outliers
                    outlier_indices = random.sample(range(len(df_copy)), num_outliers)

                    # Generate outlier values
                    if 'value' in config:
                        # Fixed outlier value
                        df_copy.loc[outlier_indices, column] = config['value']
                    elif 'min' in config and 'max' in config:
                        # Random outlier values within range
                        outlier_values = [
                            random.uniform(config['min'], config['max'])
                            if isinstance(df_copy[column].iloc[0], (int, float))
                            else random.randint(int(config['min']), int(config['max']))
                            for _ in range(num_outliers)
                        ]
                        df_copy.loc[outlier_indices, column] = outlier_values

        return df_copy

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """
        Save the generated dataset to a CSV file.

        Args:
            df: DataFrame to save
            filepath: Path where CSV file should be saved
        """
        df.to_csv(filepath, index=False)
        print(f"Dataset saved to: {filepath}")

    def generate_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate a report of data quality issues in the dataset.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary containing data quality metrics
        """
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'duplicate_count': df.duplicated().sum(),
            'duplicate_rate': df.duplicated().sum() / len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'null_rates': (df.isnull().sum() / len(df)).to_dict(),
            'column_stats': {}
        }

        # Add statistics for numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            report['column_stats'][col] = {
                'mean': df[col].mean(),
                'median': df[col].median(),
                'min': df[col].min(),
                'max': df[col].max(),
                'std': df[col].std()
            }

        return report


class FakeProductGenerator:
    """
    Generates fake product datasets with configurable data quality issues.

    Features:
    - Configurable dataset size
    - Null value injection on specific columns
    - Duplicate record generation
    - Outlier value injection on numeric columns
    """

    def __init__(self, seed: Optional[int] = 42):
        """
        Initialize the fake product data generator.

        Args:
            seed: Random seed for reproducibility
        """
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

        # Product categories and their typical price ranges
        self.categories = {
            'Electronics': (50, 2000),
            'Clothing': (10, 300),
            'Home & Garden': (15, 500),
            'Sports & Outdoors': (20, 800),
            'Books': (5, 50),
            'Toys & Games': (10, 150),
            'Beauty & Health': (8, 200),
            'Automotive': (25, 1000),
            'Food & Beverages': (2, 100),
            'Office Supplies': (5, 300)
        }

    def generate_products(
        self,
        num_records: int = 10000,
        duplicate_rate: float = 0.05,
        null_rates: Optional[Dict[str, float]] = None,
        outlier_config: Optional[Dict[str, dict]] = None
    ) -> pd.DataFrame:
        """
        Generate a fake product dataset with data quality issues.

        Args:
            num_records: Number of product records to generate
            duplicate_rate: Percentage of duplicate records (0.0 to 1.0)
            null_rates: Dictionary mapping column names to null percentages
                       Example: {'description': 0.1, 'brand': 0.15}
            outlier_config: Dictionary mapping column names to outlier configurations
                           Example: {'price': {'rate': 0.02, 'min': -100, 'max': -10},
                                    'stock_quantity': {'rate': 0.03, 'value': -999}}

        Returns:
            DataFrame with product data including quality issues
        """
        # Default null rates if not provided
        if null_rates is None:
            null_rates = {
                'description': 0.10,
                'brand': 0.08,
                'supplier': 0.12,
                'weight': 0.06
            }

        # Default outlier config if not provided
        if outlier_config is None:
            outlier_config = {
                'price': {'rate': 0.02, 'min': -500, 'max': -10},
                'stock_quantity': {'rate': 0.03, 'min': -1000, 'max': -1},
                'weight': {'rate': 0.025, 'value': 999999.99},
                'rating': {'rate': 0.02, 'min': 10, 'max': 100}
            }

        # Calculate how many unique records we need
        num_duplicates = int(num_records * duplicate_rate)
        num_unique = num_records - num_duplicates

        # Generate unique product records
        products = []
        for i in range(num_unique):
            product = self._generate_single_product(i + 1)
            products.append(product)

        # Create DataFrame
        df = pd.DataFrame(products)

        # Add duplicate records (exact copies)
        if num_duplicates > 0:
            duplicate_indices = random.choices(range(len(df)), k=num_duplicates)
            duplicates = df.iloc[duplicate_indices].copy()
            df = pd.concat([df, duplicates], ignore_index=True)
            df = df.sample(frac=1).reset_index(drop=True)  # Shuffle

        # Inject null values
        df = self._inject_nulls(df, null_rates)

        # Inject outliers
        df = self._inject_outliers(df, outlier_config)

        return df

    def _generate_single_product(self, product_id: int) -> dict:
        """
        Generate a single product record with realistic fake data.

        Args:
            product_id: Unique product identifier

        Returns:
            Dictionary containing product information
        """
        # Select category and get price range
        category = random.choice(list(self.categories.keys()))
        min_price, max_price = self.categories[category]

        # Generate base price
        price = round(random.uniform(min_price, max_price), 2)

        # Generate cost (70-90% of price)
        cost = round(price * random.uniform(0.70, 0.90), 2)

        # Generate dates
        created_date = self.fake.date_between(start_date='-3y', end_date='today')
        last_updated = self.fake.date_between(start_date=created_date, end_date='today')

        # Generate stock quantity
        stock_quantity = random.randint(0, 1000)

        # Generate weight (in kg)
        weight = round(random.uniform(0.1, 50.0), 2)

        # Generate rating
        rating = round(random.uniform(1.0, 5.0), 1)

        # Generate number of reviews
        review_count = random.randint(0, 5000)

        return {
            'product_id': product_id,
            'sku': f"SKU-{random.randint(100000, 999999)}",
            'product_name': self.fake.catch_phrase(),
            'description': self.fake.text(max_nb_chars=200) if random.random() > 0.1 else self.fake.sentence(),
            'category': category,
            'subcategory': self.fake.word().capitalize(),
            'brand': self.fake.company(),
            'supplier': self.fake.company(),
            'price': price,
            'cost': cost,
            'currency': random.choice(['USD', 'EUR', 'GBP']),
            'stock_quantity': stock_quantity,
            'reorder_level': random.randint(10, 100),
            'weight': weight,
            'dimensions': f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 100)} cm",
            'color': self.fake.color_name(),
            'size': random.choice(['XS', 'S', 'M', 'L', 'XL', 'XXL', 'N/A']),
            'material': random.choice(['Cotton', 'Plastic', 'Metal', 'Wood', 'Glass', 'Leather', 'Synthetic']),
            'rating': rating,
            'review_count': review_count,
            'is_available': stock_quantity > 0,
            'is_featured': random.choice([True, False]),
            'discount_percentage': round(random.uniform(0, 30), 1),
            'created_date': created_date,
            'last_updated': last_updated
        }

    def _inject_nulls(self, df: pd.DataFrame, null_rates: Dict[str, float]) -> pd.DataFrame:
        """
        Inject null values into specified columns.

        Args:
            df: DataFrame to modify
            null_rates: Dictionary mapping column names to null percentages

        Returns:
            Modified DataFrame with null values
        """
        df_copy = df.copy()

        for column, rate in null_rates.items():
            if column in df_copy.columns:
                # Calculate number of nulls to inject
                num_nulls = int(len(df_copy) * rate)
                if num_nulls > 0:
                    # Randomly select indices to set as null
                    null_indices = random.sample(range(len(df_copy)), num_nulls)
                    df_copy.loc[null_indices, column] = None

        return df_copy

    def _inject_outliers(self, df: pd.DataFrame, outlier_config: Dict[str, dict]) -> pd.DataFrame:
        """
        Inject outlier values into specified columns.

        Args:
            df: DataFrame to modify
            outlier_config: Dictionary mapping column names to outlier configurations

        Returns:
            Modified DataFrame with outlier values
        """
        df_copy = df.copy()

        for column, config in outlier_config.items():
            if column in df_copy.columns:
                rate = config.get('rate', 0.02)
                num_outliers = int(len(df_copy) * rate)

                if num_outliers > 0:
                    # Randomly select indices for outliers
                    outlier_indices = random.sample(range(len(df_copy)), num_outliers)

                    # Generate outlier values
                    if 'value' in config:
                        # Fixed outlier value
                        df_copy.loc[outlier_indices, column] = config['value']
                    elif 'min' in config and 'max' in config:
                        # Random outlier values within range
                        outlier_values = [
                            random.uniform(config['min'], config['max'])
                            if isinstance(df_copy[column].iloc[0], (int, float))
                            else random.randint(int(config['min']), int(config['max']))
                            for _ in range(num_outliers)
                        ]
                        df_copy.loc[outlier_indices, column] = outlier_values

        return df_copy

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """
        Save the generated dataset to a CSV file.

        Args:
            df: DataFrame to save
            filepath: Path where CSV file should be saved
        """
        df.to_csv(filepath, index=False)
        print(f"Dataset saved to: {filepath}")

    def generate_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate a report of data quality issues in the dataset.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary containing data quality metrics
        """
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'duplicate_count': df.duplicated().sum(),
            'duplicate_rate': df.duplicated().sum() / len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'null_rates': (df.isnull().sum() / len(df)).to_dict(),
            'column_stats': {}
        }

        # Add statistics for numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            report['column_stats'][col] = {
                'mean': df[col].mean(),
                'median': df[col].median(),
                'min': df[col].min(),
                'max': df[col].max(),
                'std': df[col].std()
            }

        return report


class FakeInvoiceGenerator:
    """
    Generates fake invoice datasets with configurable data quality issues.

    Features:
    - Links to existing customers and products
    - Configurable dataset size
    - Null value injection on specific columns
    - Duplicate record generation
    - Outlier value injection on numeric/date columns
    """

    def __init__(self, seed: Optional[int] = 42):
        """
        Initialize the fake invoice data generator.

        Args:
            seed: Random seed for reproducibility
        """
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

        # Invoice status options
        self.invoice_statuses = ['Paid', 'Pending', 'Overdue', 'Cancelled', 'Processing', 'Refunded']

        # Payment methods
        self.payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash', 'Check']

    def generate_invoices(
        self,
        num_records: int = 10000,
        customer_ids: Optional[List[int]] = None,
        product_ids: Optional[List[int]] = None,
        duplicate_rate: float = 0.05,
        null_rates: Optional[Dict[str, float]] = None,
        outlier_config: Optional[Dict[str, dict]] = None
    ) -> pd.DataFrame:
        """
        Generate a fake invoice dataset with data quality issues.

        Args:
            num_records: Number of invoice records to generate
            customer_ids: List of valid customer IDs to reference
            product_ids: List of valid product IDs to reference
            duplicate_rate: Percentage of duplicate records (0.0 to 1.0)
            null_rates: Dictionary mapping column names to null percentages
            outlier_config: Dictionary with outlier configuration per column

        Returns:
            DataFrame containing generated invoice data with quality issues
        """

        # Default customer and product IDs if not provided
        if customer_ids is None:
            customer_ids = list(range(1, 50001))  # Default to 50k customers
        if product_ids is None:
            product_ids = list(range(1, 20001))   # Default to 20k products

        # Configure null rates
        default_null_rates = {
            'customer_company': 0.15,      # 15% missing company info
            'billing_address': 0.08,       # 8% missing billing address
            'shipping_address': 0.12,      # 12% missing shipping address
            'payment_method': 0.05,        # 5% missing payment method
            'discount_amount': 0.60,       # 60% no discount applied
            'tax_amount': 0.03,           # 3% missing tax calculation
            'notes': 0.70,                # 70% no notes
            'due_date': 0.10              # 10% missing due dates
        }

        if null_rates:
            default_null_rates.update(null_rates)

        # Configure outliers
        default_outlier_config = {
            'quantity': {'rate': 0.02, 'min_val': 100, 'max_val': 10000},      # Extreme quantities
            'unit_price': {'rate': 0.015, 'min_val': -50, 'max_val': 50000},   # Negative/extreme prices
            'total_amount': {'rate': 0.01, 'min_val': -1000, 'max_val': 500000}, # Extreme totals
            'discount_amount': {'rate': 0.008, 'min_val': -100, 'max_val': 1000}, # Negative discounts
            'tax_rate': {'rate': 0.02, 'min_val': -5, 'max_val': 50}           # Invalid tax rates
        }

        if outlier_config:
            default_outlier_config.update(outlier_config)

        print(f"Generating {num_records:,} invoice records...")

        # Calculate number of unique records vs duplicates
        num_duplicates = int(num_records * duplicate_rate)
        num_unique = num_records - num_duplicates

        # Generate unique records
        records = []
        for i in range(num_unique):
            record = self._generate_single_invoice(
                i + 1, customer_ids, product_ids, default_null_rates, default_outlier_config
            )
            records.append(record)

        # Generate duplicates by repeating some records
        duplicate_indices = random.sample(range(len(records)), min(num_duplicates, len(records)))
        for idx in duplicate_indices:
            # Create a slight variation of the duplicate
            duplicate_record = records[idx].copy()
            duplicate_record['invoice_id'] = len(records) + 1
            # Small variations to make duplicates realistic
            if random.random() < 0.3:  # 30% chance to vary invoice date slightly
                base_date = datetime.strptime(duplicate_record['invoice_date'], '%Y-%m-%d')
                variation_days = random.randint(1, 3)
                new_date = base_date + timedelta(days=variation_days)
                duplicate_record['invoice_date'] = new_date.strftime('%Y-%m-%d')

            records.append(duplicate_record)

        # Convert to DataFrame
        df = pd.DataFrame(records)

        print(f"Generated {len(df):,} invoice records")
        print(f"  - Unique records: {num_unique:,}")
        print(f"  - Duplicate records: {num_duplicates:,}")

        return df

    def _generate_single_invoice(self, invoice_id: int, customer_ids: List[int], product_ids: List[int],
                                null_rates: Dict[str, float], outlier_config: Dict[str, dict]) -> dict:
        """Generate a single invoice record with potential quality issues."""

        # Basic invoice information
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)

        # Invoice dates
        invoice_date = self.fake.date_between(start_date='-2y', end_date='today')
        due_date = None
        if random.random() > null_rates.get('due_date', 0):
            due_date = invoice_date + timedelta(days=random.randint(15, 60))

        # Quantity and pricing
        quantity = self._apply_outliers(random.randint(1, 10), 'quantity', outlier_config)
        unit_price = self._apply_outliers(round(random.uniform(5.0, 500.0), 2), 'unit_price', outlier_config)

        # Calculate amounts
        subtotal = quantity * unit_price

        # Discount
        discount_amount = None
        if random.random() > null_rates.get('discount_amount', 0):
            discount_pct = random.uniform(0.05, 0.25)  # 5-25% discount
            discount_amount = round(subtotal * discount_pct, 2)
            discount_amount = self._apply_outliers(discount_amount, 'discount_amount', outlier_config)

        # Tax
        tax_amount = None
        tax_rate = None
        if random.random() > null_rates.get('tax_amount', 0):
            tax_rate = self._apply_outliers(round(random.uniform(0.05, 0.15), 4), 'tax_rate', outlier_config)  # 5-15%
            tax_amount = round((subtotal - (discount_amount or 0)) * tax_rate, 2)

        # Total amount
        total_amount = subtotal - (discount_amount or 0) + (tax_amount or 0)
        total_amount = self._apply_outliers(round(total_amount, 2), 'total_amount', outlier_config)

        record = {
            'invoice_id': invoice_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'invoice_date': invoice_date.strftime('%Y-%m-%d'),
            'due_date': due_date.strftime('%Y-%m-%d') if due_date else None,
            'quantity': quantity,
            'unit_price': unit_price,
            'subtotal': round(subtotal, 2),
            'discount_amount': discount_amount,
            'tax_rate': tax_rate,
            'tax_amount': tax_amount,
            'total_amount': total_amount,
            'invoice_status': random.choice(self.invoice_statuses),
            'payment_method': None if random.random() < null_rates.get('payment_method', 0) else random.choice(self.payment_methods),
            'customer_company': None if random.random() < null_rates.get('customer_company', 0) else self.fake.company(),
            'billing_address': None if random.random() < null_rates.get('billing_address', 0) else self.fake.address().replace('\n', ', '),
            'shipping_address': None if random.random() < null_rates.get('shipping_address', 0) else self.fake.address().replace('\n', ', '),
            'notes': None if random.random() < null_rates.get('notes', 0) else self.fake.text(max_nb_chars=200),
            'created_at': self.fake.date_time_between(start_date=invoice_date, end_date='now').isoformat(),
            'updated_at': self.fake.date_time_between(start_date=invoice_date, end_date='now').isoformat()
        }

        return record

    def _apply_outliers(self, value, column: str, outlier_config: Dict[str, dict]):
        """Apply outlier injection to a specific value."""
        if column in outlier_config:
            config = outlier_config[column]
            if random.random() < config['rate']:
                # Generate outlier
                if random.random() < 0.5:
                    return config['min_val']  # Minimum outlier
                else:
                    return config['max_val']  # Maximum outlier
        return value

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """Save DataFrame to CSV file."""
        df.to_csv(filename, index=False)
        print(f"Saved {len(df):,} records to {filename}")

    def generate_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """Generate a basic data quality report for the dataset."""
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'duplicates': df.duplicated().sum(),
            'null_counts': df.isnull().sum().to_dict(),
            'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB"
        }

        # Add column statistics for numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            report[f'{col}_stats'] = {
                'mean': df[col].mean(),
                'std': df[col].std()
            }

        return report


# Example usage
if __name__ == "__main__":
    # Full generation (original workflow)
    # Initialize generators
    customer_generator = FakeCustomerGenerator(seed=42)
    product_generator = FakeProductGenerator(seed=42)
    invoice_generator = FakeInvoiceGenerator(seed=42)

    print("=" * 60)
    print("GENERATING RAW DATA (with quality issues)")
    print("=" * 60)

    # Generate RAW customer dataset with 50,000 records (high quality issues)
    print("Generating RAW customer data...")
    customers_raw = customer_generator.generate_customers(
        num_records=50000,
        duplicate_rate=0.08,  # 8% duplicates (higher for raw)
        null_rates={
            'email': 0.12,     # 12% nulls (higher for raw)
            'phone': 0.15,     # 15% nulls
            'address': 0.08,   # 8% nulls
            'company': 0.18    # 18% nulls
        },
        outlier_config={
            'age': {'rate': 0.05, 'min': 150, 'max': 999},        # 5% outliers
            'income': {'rate': 0.04, 'min': -50000, 'max': -1000}, # 4% outliers
            'account_balance': {'rate': 0.03, 'value': 999999999.99} # 3% outliers
        }
    )

    # Save RAW customers to CSV
    customer_generator.save_to_csv(customers_raw, 'data/fake_customers_raw.csv')

    # Generate customer quality report
    customer_raw_report = customer_generator.generate_data_quality_report(customers_raw)
    print("\n=== RAW CUSTOMERS Data Quality Report ===")
    print(f"Total Records: {customer_raw_report['total_records']:,}")
    print(f"Duplicate Count: {customer_raw_report['duplicate_count']:,} ({customer_raw_report['duplicate_rate']:.2%})")
    print("\nNull Rates by Column:")
    for col, rate in customer_raw_report['null_rates'].items():
        if rate > 0:
            print(f"  {col}: {rate:.2%}")

    # Generate RAW product dataset with 30,000 records (high quality issues)
    print("\n\nGenerating RAW product data...")
    products_raw = product_generator.generate_products(
        num_records=30000,
        duplicate_rate=0.08,  # 8% duplicates (higher for raw)
        null_rates={
            'description': 0.15,  # 15% nulls (higher for raw)
            'brand': 0.12,        # 12% nulls
            'supplier': 0.18,     # 18% nulls
            'weight': 0.10        # 10% nulls
        },
        outlier_config={
            'price': {'rate': 0.05, 'min': -500, 'max': -10},       # 5% outliers
            'stock_quantity': {'rate': 0.04, 'min': -1000, 'max': -1}, # 4% outliers
            'weight': {'rate': 0.03, 'value': 999999.99},           # 3% outliers
            'rating': {'rate': 0.05, 'min': 10.0, 'max': 99.9}     # 5% outliers (fixed precision)
        }
    )

    # Save RAW products to CSV
    product_generator.save_to_csv(products_raw, 'data/fake_products_raw.csv')

    # Generate RAW invoice dataset with 100,000 records (high quality issues)
    print("\n\nGenerating RAW invoice data...")
    # Extract customer and product IDs for referential integrity
    customer_ids = customers_raw['customer_id'].unique().tolist()
    product_ids = products_raw['product_id'].unique().tolist()

    invoices_raw = invoice_generator.generate_invoices(
        num_records=100000,
        customer_ids=customer_ids,
        product_ids=product_ids,
        duplicate_rate=0.08,  # 8% duplicates (higher for raw)
        null_rates={
            'customer_company': 0.20,     # 20% nulls (higher for raw)
            'billing_address': 0.15,      # 15% nulls
            'shipping_address': 0.18,     # 18% nulls
            'payment_method': 0.08,       # 8% nulls
            'discount_amount': 0.65,      # 65% no discount
            'tax_amount': 0.05,          # 5% missing tax
            'notes': 0.75,               # 75% no notes
            'due_date': 0.12             # 12% missing due dates
        },
        outlier_config={
            'quantity': {'rate': 0.03, 'min_val': -10, 'max_val': 15000},
            'unit_price': {'rate': 0.025, 'min_val': -100, 'max_val': 75000},
            'total_amount': {'rate': 0.02, 'min_val': -2000, 'max_val': 800000},
            'discount_amount': {'rate': 0.015, 'min_val': -500, 'max_val': 2000},
            'tax_rate': {'rate': 0.03, 'min_val': -10, 'max_val': 75}
        }
    )

    # Save RAW invoices to CSV
    invoice_generator.save_to_csv(invoices_raw, 'data/fake_invoices_raw.csv')

    # Generate product quality report
    product_raw_report = product_generator.generate_data_quality_report(products_raw)
    print("\n=== RAW PRODUCTS Data Quality Report ===")
    print(f"Total Records: {product_raw_report['total_records']:,}")
    print(f"Duplicate Count: {product_raw_report['duplicate_count']:,} ({product_raw_report['duplicate_rate']:.2%})")
    print("\nNull Rates by Column:")
    for col, rate in product_raw_report['null_rates'].items():
        if rate > 0:
            print(f"  {col}: {rate:.2%}")

    # Generate invoice quality report
    invoice_raw_report = invoice_generator.generate_data_quality_report(invoices_raw)
    print("\n=== RAW INVOICES Data Quality Report ===")
    print(f"Total Records: {invoice_raw_report['total_records']:,}")
    print(f"Duplicates: {invoice_raw_report['duplicates']:,}")
    print(f"Memory Usage: {invoice_raw_report['memory_usage']}")
    print("\nNull Counts by Column:")
    for col, count in invoice_raw_report['null_counts'].items():
        if count > 0:
            print(f"  {col}: {count:,} ({count/invoice_raw_report['total_records']:.2%})")

    print("\n" + "=" * 60)
    print("GENERATING PRODUCTION DATA (after quality filtering)")
    print("=" * 60)

    # ==========================================
    # CREATE PRODUCTION VERSIONS (Filtered)
    # ==========================================

    def create_production_customers(raw_df):
        """Apply data quality filters to create production dataset"""
        prod_df = raw_df.copy()

        print(f"Starting with {len(prod_df):,} raw customer records")

        # 1. Remove duplicates
        before_dedup = len(prod_df)
        prod_df = prod_df.drop_duplicates()
        print(f"Removed {before_dedup - len(prod_df):,} duplicate records")

        # 2. Remove records with critical null values
        before_null_filter = len(prod_df)
        prod_df = prod_df[prod_df['email'].notna()]  # Email required
        prod_df = prod_df[prod_df['customer_id'].notna()]  # ID required
        print(f"Removed {before_null_filter - len(prod_df):,} records with critical nulls")

        # 3. Remove age outliers
        before_age_filter = len(prod_df)
        prod_df = prod_df[(prod_df['age'] >= 18) & (prod_df['age'] <= 100)]
        print(f"Removed {before_age_filter - len(prod_df):,} records with invalid ages")

        # 4. Remove negative income outliers
        before_income_filter = len(prod_df)
        prod_df = prod_df[prod_df['income'] >= 0]
        print(f"Removed {before_income_filter - len(prod_df):,} records with negative income")

        # 5. Remove extreme account balance outliers
        before_balance_filter = len(prod_df)
        prod_df = prod_df[prod_df['account_balance'] <= 1000000]  # Max 1M balance
        print(f"Removed {before_balance_filter - len(prod_df):,} records with extreme account balances")

        return prod_df

    def create_production_products(raw_df):
        """Apply data quality filters to create production dataset"""
        prod_df = raw_df.copy()

        print(f"Starting with {len(prod_df):,} raw product records")

        # 1. Remove duplicates
        before_dedup = len(prod_df)
        prod_df = prod_df.drop_duplicates()
        print(f"Removed {before_dedup - len(prod_df):,} duplicate records")

        # 2. Remove records with critical null values
        before_null_filter = len(prod_df)
        prod_df = prod_df[prod_df['product_name'].notna()]  # Name required
        prod_df = prod_df[prod_df['product_id'].notna()]  # ID required
        prod_df = prod_df[prod_df['sku'].notna()]  # SKU required
        print(f"Removed {before_null_filter - len(prod_df):,} records with critical nulls")

        # 3. Remove negative price outliers
        before_price_filter = len(prod_df)
        prod_df = prod_df[prod_df['price'] > 0]
        print(f"Removed {before_price_filter - len(prod_df):,} records with negative prices")

        # 4. Remove negative stock outliers
        before_stock_filter = len(prod_df)
        prod_df = prod_df[prod_df['stock_quantity'] >= 0]
        print(f"Removed {before_stock_filter - len(prod_df):,} records with negative stock")

        # 5. Remove invalid rating outliers
        before_rating_filter = len(prod_df)
        prod_df = prod_df[(prod_df['rating'] >= 1.0) & (prod_df['rating'] <= 5.0)]
        print(f"Removed {before_rating_filter - len(prod_df):,} records with invalid ratings")

        # 6. Remove extreme weight outliers
        before_weight_filter = len(prod_df)
        prod_df = prod_df[(prod_df['weight'].isna()) | (prod_df['weight'] <= 1000)]  # Max 1000kg
        print(f"Removed {before_weight_filter - len(prod_df):,} records with extreme weights")

        return prod_df

    def create_production_invoices(raw_df):
        """Apply quality filters to create production-ready invoice data."""
        inv_df = raw_df.copy()
        print(f"Starting with {len(inv_df):,} raw invoice records")

        # 1. Remove duplicate invoices
        before_dedup = len(inv_df)
        inv_df = inv_df.drop_duplicates()
        print(f"Removed {before_dedup - len(inv_df):,} duplicate records")

        # 2. Remove records with critical null values
        before_null_filter = len(inv_df)
        inv_df = inv_df[inv_df['invoice_id'].notna()]  # Invoice ID required
        inv_df = inv_df[inv_df['customer_id'].notna()]  # Customer ID required
        inv_df = inv_df[inv_df['product_id'].notna()]   # Product ID required
        inv_df = inv_df[inv_df['invoice_date'].notna()]  # Invoice date required
        print(f"Removed {before_null_filter - len(inv_df):,} records with critical nulls")

        # 3. Remove negative quantity outliers
        before_quantity_filter = len(inv_df)
        inv_df = inv_df[inv_df['quantity'] > 0]
        print(f"Removed {before_quantity_filter - len(inv_df):,} records with negative quantities")

        # 4. Remove negative unit price outliers
        before_price_filter = len(inv_df)
        inv_df = inv_df[inv_df['unit_price'] > 0]
        print(f"Removed {before_price_filter - len(inv_df):,} records with negative unit prices")

        # 5. Remove negative total amount outliers
        before_total_filter = len(inv_df)
        inv_df = inv_df[inv_df['total_amount'] > 0]
        print(f"Removed {before_total_filter - len(inv_df):,} records with negative total amounts")

        # 6. Remove invalid tax rate outliers
        before_tax_filter = len(inv_df)
        inv_df = inv_df[(inv_df['tax_rate'].isna()) | ((inv_df['tax_rate'] >= 0) & (inv_df['tax_rate'] <= 1.0))]
        print(f"Removed {before_tax_filter - len(inv_df):,} records with invalid tax rates")

        # 7. Remove negative discount outliers
        before_discount_filter = len(inv_df)
        inv_df = inv_df[(inv_df['discount_amount'].isna()) | (inv_df['discount_amount'] >= 0)]
        print(f"Removed {before_discount_filter - len(inv_df):,} records with negative discounts")

        # 8. Remove extreme quantity outliers (> 1000)
        before_extreme_qty_filter = len(inv_df)
        inv_df = inv_df[inv_df['quantity'] <= 1000]
        print(f"Removed {before_extreme_qty_filter - len(inv_df):,} records with extreme quantities")

        return inv_df

    # Create production customers
    print("\nApplying quality filters to CUSTOMERS...")
    customers_prod = create_production_customers(customers_raw)
    customer_generator.save_to_csv(customers_prod, 'data/fake_customers_prod.csv')

    # Create production products
    print("\nApplying quality filters to PRODUCTS...")
    products_prod = create_production_products(products_raw)
    product_generator.save_to_csv(products_prod, 'data/fake_products_prod.csv')

    # Create production invoices
    print("\nApplying quality filters to INVOICES...")
    invoices_prod = create_production_invoices(invoices_raw)
    invoice_generator.save_to_csv(invoices_prod, 'data/fake_invoices_prod.csv')

    # Generate production quality reports
    customer_prod_report = customer_generator.generate_data_quality_report(customers_prod)
    product_prod_report = product_generator.generate_data_quality_report(products_prod)
    invoice_prod_report = invoice_generator.generate_data_quality_report(invoices_prod)

    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)

    print(f"\nCUSTOMERS:")
    print(f"  Raw Dataset:        {customer_raw_report['total_records']:,} records")
    print(f"  Production Dataset: {customer_prod_report['total_records']:,} records")
    print(f"  Filtered Out:       {customer_raw_report['total_records'] - customer_prod_report['total_records']:,} records ({((customer_raw_report['total_records'] - customer_prod_report['total_records']) / customer_raw_report['total_records'] * 100):.1f}%)")

    print(f"\nPRODUCTS:")
    print(f"  Raw Dataset:        {product_raw_report['total_records']:,} records")
    print(f"  Production Dataset: {product_prod_report['total_records']:,} records")
    print(f"  Filtered Out:       {product_raw_report['total_records'] - product_prod_report['total_records']:,} records ({((product_raw_report['total_records'] - product_prod_report['total_records']) / product_raw_report['total_records'] * 100):.1f}%)")

    print(f"\nINVOICES:")
    print(f"  Raw Dataset:        {invoice_raw_report['total_records']:,} records")
    print(f"  Production Dataset: {invoice_prod_report['total_records']:,} records")
    print(f"  Filtered Out:       {invoice_raw_report['total_records'] - invoice_prod_report['total_records']:,} records ({((invoice_raw_report['total_records'] - invoice_prod_report['total_records']) / invoice_raw_report['total_records'] * 100):.1f}%)")

    print(f"\nFiles created:")
    print(f"  - fake_customers_raw.csv  (raw data with quality issues)")
    print(f"  - fake_customers_prod.csv (production-ready data)")
    print(f"  - fake_products_raw.csv   (raw data with quality issues)")
    print(f"  - fake_products_prod.csv  (production-ready data)")
    print(f"  - fake_invoices_raw.csv   (raw data with quality issues)")
    print(f"  - fake_invoices_prod.csv  (production-ready data)")
