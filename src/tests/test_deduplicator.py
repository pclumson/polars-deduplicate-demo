
Unit tests for DataDeduplicator using pytest.
Demonstrates TDD practices and test coverage.
"""

import pytest
import polars as pl
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from deduplicator import DataDeduplicator, benchmark_performance


@pytest.fixture
def sample_dataframe():
    # Create a sample DataFrame with known duplicates for testing. """
    return pl.DataFrame({
        'customer_id': [1, 2, 3, 4, 5],
        'first_name': ['John', 'Jane', 'John', 'Alice', 'Bob'],
        'last_name': ['Smith', 'Doe', 'Smith', 'Johnson', 'Brown'],
        'email': ['john@example.com', 'jane@example.com', 'john@example.com',
                  'alice@example.com', 'bob@example.com'],
        'phone': ['+1-555-0001', '+1-555-0002', '+1-555-0003',
                  '+1-555-0004', '+1-555-0005'],
        'status': ['active', 'active', 'inactive', 'active', 'pending']
    })


class TestDataDeduplicator:
    #"""Test suite for DataDeduplicator class."""

    def test_init_default_chunk_size(self):
       # """Test default initialization.
        deduplicator = DataDeduplicator()
        assert deduplicator.chunk_size == 100_000

    def test_init_custom_chunk_size(self):
        #"""Test custom chunk size initialization.
        deduplicator = DataDeduplicator(chunk_size=50_000)
        assert deduplicator.chunk_size == 50_000

    def test_deduplicate_by_email_removes_duplicates(self, sample_dataframe):
       # """Test that email deduplication removes duplicate records.
        deduplicator = DataDeduplicator()
        deduped_df, removed_count = deduplicator.deduplicate_by_email(sample_dataframe)

        assert removed_count == 1  # One duplicate email
        assert len(deduped_df) == 4  # 5 original - 1 duplicate
        assert deduped_df['email'].n_unique() == len(deduped_df)

    def test_deduplicate_by_email_keeps_first(self, sample_dataframe):
        #"""Test that first occurrence is kept by default."""
        deduplicator = DataDeduplicator()
        deduped_df, _ = deduplicator.deduplicate_by_email(sample_dataframe, keep_strategy='first')

        # Should keep customer_id 1 (first John Smith)
        john_records = deduped_df.filter(pl.col('first_name') == 'John')
        assert len(john_records) == 1
        assert john_records['customer_id'].item() == 1

    def test_deduplicate_empty_dataframe(self):
       # """Test deduplication on empty DataFrame.
        deduplicator = DataDeduplicator()
        empty_df = pl.DataFrame({'email': [], 'customer_id': []})
        deduped_df, removed_count = deduplicator.deduplicate_by_email(empty_df)

        assert len(deduped_df) == 0
        assert removed_count == 0

    def test_get_duplicate_statistics(self, sample_dataframe):
       # """Test duplicate statistics calculation.
        deduplicator = DataDeduplicator()
        stats = deduplicator.get_duplicate_statistics(sample_dataframe)

        assert stats['total_records'] == 5
        assert stats['unique_keys'] == 4  # 4 unique emails
        assert stats['duplicate_groups'] == 1  # 1 email has duplicates
        assert stats['total_duplicates'] == 1

    def test_no_duplicates_returns_same_count(self):
        #"""Test DataFrame with no duplicates."""
        deduplicator = DataDeduplicator()
        unique_df = pl.DataFrame({
            'email': ['a@test.com', 'b@test.com', 'c@test.com'],
            'customer_id': [1, 2, 3]
        })

        deduped_df, removed_count = deduplicator.deduplicate_by_email(unique_df)

        assert removed_count == 0
        assert len(deduped_df) == 3


class TestBenchmarkPerformance:
    """Test suite for benchmarking functions."""

    def test_benchmark_returns_valid_metrics(self):
        """Test that benchmark returns expected metrics."""
        df = pl.DataFrame({
            'email': [f'user{i}@test.com' for i in range(1000)],
            'customer_id': range(1000)
        })

        results = benchmark_performance(df, iterations=2)

        assert 'avg_time_seconds' in results
        assert 'rows_per_second' in results
        assert results['avg_time_seconds'] > 0
        assert results['rows_per_second'] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
