#!/usr/bin/env python3
"""
Test script to verify the set_once decorator functionality
"""

from redshift_query_builder.core import UnloadQueryBuilder


def test_set_once_decorator():
    """Test that the set_once decorator prevents setting the same option twice."""
    print("Testing set_once decorator...")

    # Test 1: Try to set format twice
    try:
        builder = UnloadQueryBuilder()
        builder.set_format("CSV")
        builder.set_format("PARQUET")  # This should raise an error
        print("❌ Test failed: Should have raised an error for setting format twice")
    except ValueError as e:
        print(f"✅ Test passed: Correctly raised error: {e}")

    # Test 2: Try to set header twice
    try:
        builder = UnloadQueryBuilder()
        builder.set_header()
        builder.set_header()  # This should raise an error
        print("❌ Test failed: Should have raised an error for setting header twice")
    except ValueError as e:
        print(f"✅ Test passed: Correctly raised error: {e}")

    # Test 3: Try to set compression twice
    try:
        builder = UnloadQueryBuilder()
        builder.set_compression("GZIP")
        builder.set_compression("BZIP2")  # This should raise an error
        print(
            "❌ Test failed: Should have raised an error for setting compression twice"
        )
    except ValueError as e:
        print(f"✅ Test passed: Correctly raised error: {e}")

    # Test 4: Try to set delimiter twice
    try:
        builder = UnloadQueryBuilder()
        builder.set_delimiter(",")
        builder.set_delimiter("|")  # This should raise an error
        print("❌ Test failed: Should have raised an error for setting delimiter twice")
    except ValueError as e:
        print(f"✅ Test passed: Correctly raised error: {e}")

    # Test 5: Try to set region twice
    try:
        builder = UnloadQueryBuilder()
        builder.set_region("us-east-1")
        builder.set_region("us-west-2")  # This should raise an error
        print("❌ Test failed: Should have raised an error for setting region twice")
    except ValueError as e:
        print(f"✅ Test passed: Correctly raised error: {e}")

    # Test 6: Verify that different options can be set together
    try:
        builder = UnloadQueryBuilder()
        builder.set_format("CSV")
        builder.set_header()
        builder.set_compression("GZIP")
        builder.set_delimiter(",")
        builder.set_region("us-east-1")
        print("✅ Test passed: Different options can be set together")
    except Exception as e:
        print(f"❌ Test failed: Should allow setting different options: {e}")


def test_builder_functionality():
    """Test that the builder still works correctly with the decorator."""
    print("\nTesting builder functionality with decorator...")

    try:
        builder = UnloadQueryBuilder()
        query_and_params = (
            builder.add_select_template("SELECT * FROM test_table")
            .add_select_params({})
            .add_to_path("s3://test-bucket/")
            .add_default_authorization()
            .set_format("CSV")
            .set_header()
            .set_compression("GZIP")
            .set_delimiter(",")
            .set_region("us-east-1")
            .build()
        )

        print("✅ Builder works correctly with decorator")
        print(f"Generated query: {query_and_params.query}")

    except Exception as e:
        print(f"❌ Builder test failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    print("Testing set_once Decorator")
    print("=" * 40)

    test_set_once_decorator()
    test_builder_functionality()

    print("\n" + "=" * 40)
    print("Decorator tests completed!")
