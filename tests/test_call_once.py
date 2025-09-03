#!/usr/bin/env python3

from redshift_query_builder.core import UnloadQueryBuilder


def test_call_once_decorator():
    """Test that the @call_once decorator works correctly."""

    print("Testing @call_once decorator...")

    # Create a builder instance
    builder = UnloadQueryBuilder()

    # Test that first call works
    print("1. Testing first call...")
    try:
        builder.set_format("CSV")
        print("✓ First call to set_format() succeeded")
    except Exception as e:
        print(f"✗ First call failed: {e}")
        return

    # Test that second call fails
    print("2. Testing second call...")
    try:
        builder.set_format("JSON")
        print("✗ Second call should have failed")
    except ValueError as e:
        print(f"✓ Second call correctly failed: {e}")

    # Test that other methods still work
    print("3. Testing other methods...")
    try:
        builder.set_compression("GZIP")
        print("✓ First call to set_compression() succeeded")
    except Exception as e:
        print(f"✗ set_compression() failed: {e}")
        return

    # Test that second call to different method also fails
    try:
        builder.set_compression("BZIP2")
        print("✗ Second call to set_compression() should have failed")
    except ValueError as e:
        print(f"✓ Second call to set_compression() correctly failed: {e}")

    print("\nAll tests passed! The @call_once decorator is working correctly.")


if __name__ == "__main__":
    test_call_once_decorator()
