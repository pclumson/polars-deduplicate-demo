""
High-Performance Data Deduplication using Polars.
Demonstrates lazy evaluation, memory efficiency, and deduplication strategies.
"""

"""
High-Performance Data Deduplication using Polars.
Compatible with Polars 0.20+ (uses unique() for LazyFrames).
"""

import polars as pl
from typing import Tuple, Optional, List
import time
from pathlib import Path


class DataDeduplicator:
    """
    Handles deduplication of CRM data using Polars for optimal performance.
    """

    def __init__(self, chunk_size: int = 100_000):
        self.chunk_size = chunk_size

    def deduplicate_by_email(self, df: pl.DataFrame,
                             keep_strategy: str = 'first') -> Tuple[pl.DataFrame, int]:
        """
        Remove duplicate records based on email address.
        Preserves the 'source' column if it exists.
        """
        original_count = len(df)

        # Check if 'email' exists
        if 'email' not in df.columns:
            raise ValueError("DataFrame must contain an 'email' column.")

        # Lazy evaluation for optimization
        # FIX: Use unique() instead of drop_duplicates() for LazyFrames
        deduped_df = (df.lazy()
                      .unique(subset=['email'], keep=keep_strategy)  # ← FIXED
                      .collect())

        removed_count = original_count - len(deduped_df)

        return deduped_df, removed_count

    def deduplicate_by_fuzzy_match(self, df: pl.DataFrame,
                                    name_threshold: float = 0.8) -> Tuple[pl.DataFrame, int]:
        """
        Advanced deduplication using normalized name combinations.
        """
        original_count = len(df)

        if 'first_name' not in df.columns or 'last_name' not in df.columns:
            raise ValueError("DataFrame must contain 'first_name' and 'last_name' columns.")

        # Normalize names and group
        # FIX: Use unique() for LazyFrames
        deduped_df = (df.lazy()
                      .with_columns([
                          pl.col('first_name').str.to_lowercase().str.strip_chars().alias('first_name_norm'),
                          pl.col('last_name').str.to_lowercase().str.strip_chars().alias('last_name_norm')
                      ])
                      # Group by normalized names and keep the first record in each group
                      .group_by(['first_name_norm', 'last_name_norm'])
                      .agg([
                          pl.col('*').first()  # Keeps all original columns including 'source'
                      ])
                      .drop(['first_name_norm', 'last_name_norm'])  # Drop helper columns
                      .collect())

        removed_count = original_count - len(deduped_df)

        return deduped_df, removed_count

    def deduplicate_large_file(self, filepath: str,
                               output_path: str,
                               key_column: str = 'email') -> Tuple[int, int]:
        """
        Process large CSV files using chunked lazy evaluation.
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        # Scan CSV lazily
        lf = pl.scan_csv(filepath)

        # Apply deduplication
        # FIX: Use unique() instead of drop_duplicates() for LazyFrames
        deduped_lf = lf.unique(subset=[key_column], keep='first')  # ← FIXED

        # Collect and save
        deduped_df = deduped_lf.collect()
        deduped_df.write_csv(output_path)

        # Calculate stats
        original_count = len(lf.collect())
        removed_count = original_count - len(deduped_df)

        return original_count, removed_count

    def get_duplicate_statistics(self, df: pl.DataFrame,
                                  key_column: str = 'email') -> dict:
        """
        Analyze duplicate patterns in the dataset.
        """
        if key_column not in df.columns:
            raise ValueError(f"Column '{key_column}' not found in DataFrame.")

        stats = (df.lazy()
                 .group_by(key_column)
                 .agg([
                     pl.len().alias('occurrence_count'),
                     pl.col('customer_id').first().alias('first_customer_id')
                 ])
                 .filter(pl.col('occurrence_count') > 1)
                 .collect())

        return {
            'total_records': len(df),
            'unique_keys': df.n_unique(key_column),
            'duplicate_groups': len(stats),
            'total_duplicates': len(df) - df.n_unique(key_column),
            'max_occurrences': stats['occurrence_count'].max() if len(stats) > 0 else 0
        }


def benchmark_performance(df: pl.DataFrame, iterations: int = 3) -> dict:
    """
    Benchmark deduplication performance.
    """
    times = []

    for i in range(iterations):
        start = time.perf_counter()
        deduplicator = DataDeduplicator()
        deduped_df, _ = deduplicator.deduplicate_by_email(df)
        end = time.perf_counter()
        times.append(end - start)

    avg_time = sum(times) / len(times)
    return {
        'avg_time_seconds': avg_time,
        'min_time_seconds': min(times),
        'max_time_seconds': max(times),
        'rows_per_second': len(df) / avg_time if avg_time > 0 else 0
    }


if __name__ == '__main__':
    # Example usage
    from data_generator import generate_sample_data, save_to_csv

    print("=" * 60)
    print("Polars Deduplication Demo - ActivePrime Application")
    print("=" * 60)

    # Generate sample data
    print("\n[1] Generating sample data...")
    df = generate_sample_data(num_rows=500_000)
    save_to_csv(df, 'data/sample_data.csv')

    # Get duplicate statistics
    print("\n[2] Analyzing duplicate patterns...")
    deduplicator = DataDeduplicator()
    stats = deduplicator.get_duplicate_statistics(df)

    print(f"\nDuplicate Statistics:")
    print(f"  - Total Records: {stats['total_records']:,}")
    print(f"  - Unique Emails: {stats['unique_keys']:,}")
    print(f"  - Duplicate Groups: {stats['duplicate_groups']:,}")
    print(f"  - Total Duplicates Removed: {stats['total_duplicates']:,}")

    # Perform deduplication
    print("\n[3] Performing deduplication...")
    deduped_df, removed_count = deduplicator.deduplicate_by_email(df)
    print(f"  - Records After Deduplication: {len(deduped_df):,}")
    print(f"  - Records Removed: {removed_count:,}")

    # Verify 'source' column is preserved
    if 'source' in deduped_df.columns:
        print(f"  - 'source' column preserved: {deduped_df['source'].unique().to_list()}")

    # Benchmark
    print("\n[4] Running performance benchmark...")
    benchmark_results = benchmark_performance(df)
    print(f"  - Average Time: {benchmark_results['avg_time_seconds']:.3f}s")
    print(f"  - Rows Processed/Second: {benchmark_results['rows_per_second']:,.0f}")

    # Save results
    print("\n[5] Saving deduplicated data...")
    deduped_df.write_csv('data/deduplicated_output.csv')
    print(f"  - Output saved to: data/deduplicated_output.csv")

    print("\n" + "=" * 60)
    print("Demo Complete!")
    print("=" * 60)
