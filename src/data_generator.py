"""
Author : Prince Clumson-Eklu
Generate synthetic data for deduplication testing.
Creates a CSV with intentional duplicates for demonstration purposes.
"""

import polars as pl
import numpy as np
from datetime import datetime, timedelta
import os

def generate_sample_data(num_rows: int = 1_000_000, duplicate_ratio: float = 0.15) -> pl.DataFrame:
    """
    Generate sample CRM-like data with intentional duplicates.

    Args:
        num_rows: Total number of rows to generate
        duplicate_ratio: Percentage of rows that will be duplicates

    Returns:
        Polars DataFrame with sample data
    """
    np.random.seed(42)

    # Generate base unique records
    unique_count = int(num_rows * (1 - duplicate_ratio))

    # Simulate CRM data fields
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Chris', 'Lisa']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'company.com', 'enterprise.net']

    # Create unique records
    df_unique = pl.DataFrame({
        'customer_id': range(1, unique_count + 1),
        'first_name': np.random.choice(first_names, unique_count),
        'last_name': np.random.choice(last_names, unique_count),
        'email': [f"{np.random.choice(first_names).lower()}.{np.random.choice(last_names).lower()}@{np.random.choice(domains)}"
                  for _ in range(unique_count)],
        'phone': [f"+1-{np.random.randint(200,999)}-{np.random.randint(100,999)}-{np.random.randint(1000,9999)}"
                  for _ in range(unique_count)],
        'created_at': [datetime.now() - timedelta(days=np.random.randint(1, 365)) for _ in range(unique_count)],
        'company': np.random.choice(['Acme Corp', 'TechStart', 'GlobalInc', 'DataFlow', 'CloudSys'], unique_count),
        'status': np.random.choice(['active', 'inactive', 'pending'], unique_count)
    })

    # Create duplicates (same email, slightly modified metadata)
    duplicate_count = num_rows - unique_count
    df_duplicates = df_unique.sample(n=duplicate_count, with_replacement=True)

    # Slightly modify duplicates to simulate real-world data variations
    df_duplicates = df_duplicates.with_columns([
        pl.col('created_at') + pl.duration(days=pl.int_range(duplicate_count) % 7),
        pl.lit('duplicate_record').alias('source')
    ])

    # Add this line before pl.concat()
    df_duplicates = df_duplicates.with_columns(pl.lit('duplicate_record').alias('source'))
    df_unique = df_unique.with_columns(pl.lit('original').alias('source'))

    # Combine and shuffle
    df_combined = pl.concat([df_unique, df_duplicates])
    #df_combined = pl.concat([df_unique, df_duplicates]).shuffle(seed=42)

    return df_combined


def save_to_csv(df: pl.DataFrame, filepath: str = 'data/sample_data.csv') -> None:
    """Save DataFrame to CSV, creating directory if needed."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.write_csv(filepath)
    print(f"Saved {len(df)} rows to {filepath}")


if __name__ == '__main__':
    # Generate and save sample data
    df = generate_sample_data(num_rows=1_000_000)
    save_to_csv(df)
    print(f"\nSample data generated:")
    print(f"  - Total rows: {len(df):,}")
    print(f"  - Columns: {df.columns}")
    print(f"  - Memory usage: {df.estimated_size('mb'):.2f} MB")
