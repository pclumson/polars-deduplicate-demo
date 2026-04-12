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
