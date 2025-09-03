# Redshift Query Builder

A Python library for constructing queries for Amazon Redshift using the builder pattern.

## Installation

```bash
pip install git+https://github.com/wingkitlee0/redshift-query-builder.git
```

## Quick Start

```python
from redshift_query_builder import UnloadQueryBuilder

# Build a basic CSV unload query
query_and_params = (
    UnloadQueryBuilder()
    .add_select_template("SELECT * FROM my_table WHERE date = %(date)s")
    .add_select_params({"date": "2024-01-01"})
    .add_to_path("s3://my-bucket/data/")
    .add_iam_role_authorization([("123456789012", "MyRedshiftRole")])
    .set_format("CSV")
    .set_header()
    .set_compression("GZIP")
    .build()
)

print(query_and_params.query)
# Output: UNLOAD ('SELECT * FROM my_table WHERE date = %(date)s') TO 's3://my-bucket/data/' IAM_ROLE arn:aws:iam::123456789012:role/MyRedshiftRole FORMAT AS CSV, HEADER, GZIP

print(query_and_params.params)
# Output: {'date': '2024-01-01'}
```

## Supported UNLOAD Options

The library uses a Pydantic BaseModel (`UnloadQueryOption`) to handle UNLOAD options with validation. Please consult AWS documentation for the detailed explanations. The following is the consolidated list supported by this library:

- `FORMAT` (str) - Output format: CSV, PARQUET, or JSON
- `PARTITION_BY` (PartitionByOption) - Partition data by columns with include option, e.g., `{"columns": ["column1"], "include": False}`
- `MANIFEST` (ManifestOption) - Create manifest file with verbose option, e.g., `{"enable": True, "verbose": True}`
- `HEADER` (bool) - Include header row
- `DELIMITER` (str) - Field delimiter character
- `FIXEDWIDTH` (str) - Fixed width specification
- `ENCRYPTED` (bool) - Encrypt output
- `COMPRESSION` (str) - Compression method: BZIP2, GZIP, or ZSTD; The value is
- `ADDQUOTES` (bool) - Add quotes around fields
- `NULL` (str) - NULL value representation
- `ESCAPE` (bool) - Escape special characters
- `ALLOWOVERWRITE` (bool) - Allow overwriting existing files
- `CLEANPATH` (bool) - Clean the output path
- `PARALLEL` (bool) - Enable parallel processing
- `MAXFILESIZE` (str) - Maximum file size
- `ROWGROUPSIZE` (str) - Row group size for Parquet
- `REGION` (str) - AWS region, if the destination is located in a different region
- `EXTENSION` (str) - File extension

## Examples

### Parquet Export with Partitioning

```python
query_and_params = (
    UnloadQueryBuilder()
    .add_select_template("SELECT id, name, value FROM analytics_table")
    .add_select_params({})
    .add_to_path("s3://data-lake/analytics/")
    .add_default_authorization()
    .set_format("PARQUET")
    .set_partition_by(["year", "month"], include=True)
    .set_manifest(verbose=True)
    .set_maxfilesize("500 MB")
    .build()
)
```

### JSON Export with Compression

```python
query_and_params = (
    UnloadQueryBuilder()
    .add_select_template("SELECT * FROM events WHERE event_date = %(date)s")
    .add_select_params({"date": "2024-01-01"})
    .add_to_path("s3://events-bucket/json/")
    .add_iam_role_authorization([("123456789012", "EventsRole")])
    .set_format("JSON")
    .set_compression("ZSTD")
    .set_region("us-west-2")
    .set_extension("json")
    .build()
)
```

### Delimited Export with Advanced Options

```python
query_and_params = (
    UnloadQueryBuilder()
    .add_select_template("SELECT * FROM sales_data")
    .add_select_params({})
    .add_to_path("s3://sales-bucket/export/")
    .add_iam_role_authorization([("123456789012", "SalesRole")])
    .set_delimiter(",")
    .set_header()
    .set_escape()
    .set_addquotes()
    .set_null_as("NULL")
    .set_allowoverwrite()
    .set_parallel(True)
    .build()
)
```

## Validation Rules

The library enforces *most* of the validation rules:

- **Format Conflicts**: CSV cannot be used with ESCAPE, FIXEDWIDTH, or ADDQUOTES
- **Parquet Conflicts**: PARQUET cannot be used with DELIMITER, FIXEDWIDTH, ADDQUOTES, ESCAPE, NULL, HEADER, or compression
- **JSON Conflicts**: JSON cannot be used with DELIMITER, FIXEDWIDTH, ADDQUOTES, ESCAPE, or NULL
- **Compression**: Only one compression method can be used
- **Delimiter**: Must be a single ASCII character
- **File Sizes**: Must be in valid format (e.g., "100 MB", "2 GB")
- **Regions**: Must be valid AWS region format
- **Extensions**: Cannot start with a dot

When using `UnloadQueryBuilder`, the validation of the options are done at the build stage, i.e., when `build()` is called.


## Testing

Run the tests:

```bash
pytest tests/
```

## License

Apache License 2.0 - see LICENSE file for details.

## References

- [AWS Redshift UNLOAD Documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html)