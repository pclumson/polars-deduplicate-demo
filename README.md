# Polars High-Performance Data Deduplication

Demonstration project showcasing **Polars** for enterprise-scale data deduplication, designed to solve complex data cleansing challenges.

## Overview

This project implements a production-ready deduplication system using **Polars**, a lightning-fast DataFrame library written in Rust. It demonstrates:

- **Memory-efficient lazy evaluation** for processing billions of records
- **Multiple deduplication strategies** (exact match, fuzzy matching)
- **Chunked processing** for files exceeding available RAM
- **Comprehensive test coverage** using pytest (TDD practices)
- **Performance benchmarking** with detailed metrics

## Installation

```bash
pip install -r requirements.txt

# Run the demo
python src/data_generator.py # on the first run data will be generated
python src/deduplicator.py

# Run tests
pytest src/tests/ -v


## Basic Usage
from src.deduplicator import DataDeduplicator
from src.data_generator import generate_sample_data

# Generate sample data
df = generate_sample_data(num_rows=1_000_000)

# Create deduplicator
deduplicator = DataDeduplicator()

# Remove duplicates by email
deduped_df, removed_count = deduplicator.deduplicate_by_email(df)

print(f"Removed {removed_count} duplicate records")

```

## python src/data_generator.py # on the first run data will be generated
<img width="963" height="112" alt="Screenshot_20260411_040018" src="https://github.com/user-attachments/assets/d9312e78-d7ad-46d4-af93-375d7770eae7" />
