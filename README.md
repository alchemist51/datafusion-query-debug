# DataFusion Parquet Query

A DataFusion utility to query Parquet files across multiple shards, designed for OpenSearch data structures.

## Features

- Query Parquet files using SQL
- Auto-detect shards and partitions
- Memory statistics with mimalloc
- Multiple output formats (table, JSON, CSV)

## Installation

```bash
cargo build --release
```

## Usage

```bash
# Basic query
cargo run -- -d /path/to/data -q "SELECT * FROM hits LIMIT 10"

# With verbose logging
cargo run -- -d /path/to/data -q "SELECT count(*) FROM hits" -v

# Specify target partitions
cargo run -- -d /path/to/data -q "SELECT * FROM hits" -t 8
```

## Options

- `-d, --data-path <PATH>`: Path to data directory containing shards
- `-q, --query <SQL>`: SQL query to execute
- `-t, --target-partitions <N>`: Number of target partitions
- `-o, --output-format <FORMAT>`: Output format (table, json, csv)
- `-v, --verbose`: Enable verbose logging

## License

MIT
