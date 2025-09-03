#!/usr/bin/env python3

from redshift_query_builder.core import UnloadQueryBuilder


def test_basic_unload():
    """Test basic UNLOAD query building."""
    builder = UnloadQueryBuilder()

    query_and_params = (
        builder.add_select_template("SELECT * FROM my_table WHERE date = %(date)s")
        .add_select_params({"date": "2024-01-01"})
        .add_to_path("s3://my-bucket/data/")
        .add_iam_role_authorization([("123456789012", "MyRedshiftRole")])
        .set_format("CSV")
        .set_header()
        .set_compression("GZIP")
        .build()
    )

    print("Basic UNLOAD query:")
    print(query_and_params.query)
    print("Parameters:", query_and_params.params)
    print()


def test_parquet_unload():
    """Test Parquet format UNLOAD query."""
    builder = UnloadQueryBuilder()

    query_and_params = (
        builder.add_select_template("SELECT id, name, value FROM analytics_table")
        .add_select_params({})
        .add_to_path("s3://data-lake/analytics/")
        .add_default_authorization()
        .set_format("PARQUET")
        .set_partition_by(["year", "month"], include=True)
        .set_manifest(verbose=True)
        .set_maxfilesize("500 MB")
        .build()
    )

    print("Parquet UNLOAD query:")
    print(query_and_params.query)
    print("Parameters:", query_and_params.params)
    print()


def test_json_unload():
    """Test JSON format UNLOAD query."""
    builder = UnloadQueryBuilder()

    query_and_params = (
        builder.add_select_template("SELECT * FROM events WHERE event_date = %(date)s")
        .add_select_params({"date": "2024-01-01"})
        .add_to_path("s3://events-bucket/json/")
        .add_iam_role_authorization([("123456789012", "EventsRole")])
        .set_format("JSON")
        .set_compression("ZSTD")
        .set_region("us-west-2")
        .set_extension("json")
        .build()
    )

    print("JSON UNLOAD query:")
    print(query_and_params.query)
    print("Parameters:", query_and_params.params)
    print()


def test_delimited_unload():
    """Test delimited format UNLOAD query."""
    builder = UnloadQueryBuilder()

    query_and_params = (
        builder.add_select_template("SELECT * FROM sales_data")
        .add_select_params({})
        .add_to_path("s3://sales-bucket/export/")
        .add_iam_role_authorization([("123456789012", "SalesRole")])
        .set_delimiter(",")
        .set_header()
        .set_escape()
        .set_addquotes()
        .set_null("NULL")
        .set_allowoverwrite()
        .set_parallel(True)
        .build()
    )

    print("Delimited UNLOAD query:")
    print(query_and_params.query)
    print("Parameters:", query_and_params.params)
    print()


if __name__ == "__main__":
    print("Testing UNLOAD Query Builder")
    print("=" * 50)

    try:
        test_basic_unload()
        test_parquet_unload()
        test_json_unload()
        test_delimited_unload()
        print("All tests completed successfully!")
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback

        traceback.print_exc()
