#!/usr/bin/env python3

from pydantic import ValidationError

from redshift_query_builder.core import UnloadQueryOption


def test_compression_validation():
    """Test that COMPRESSION field validates correctly with Literal type."""

    # Test valid compression values (uppercase)
    print("Testing valid compression values (uppercase)...")
    valid_options = [
        {"COMPRESSION": "GZIP"},
        {"COMPRESSION": "BZIP2"},
        {"COMPRESSION": "ZSTD"},
    ]

    for option in valid_options:
        try:
            unload_option = UnloadQueryOption(**option)
            print(f"✓ Valid: {option['COMPRESSION']}")
        except ValidationError as e:
            print(f"✗ Unexpected error for {option['COMPRESSION']}: {e}")

    # Test valid compression values (lowercase - should be converted to uppercase)
    print("\nTesting valid compression values (lowercase - should be converted)...")
    lowercase_options = [
        {"COMPRESSION": "gzip"},
        {"COMPRESSION": "bzip2"},
        {"COMPRESSION": "zstd"},
    ]

    for option in lowercase_options:
        try:
            unload_option = UnloadQueryOption(**option)
            print(
                f"✓ Valid (converted): {option['COMPRESSION']} -> {unload_option.COMPRESSION}"
            )
        except ValidationError as e:
            print(f"✗ Unexpected error for {option['COMPRESSION']}: {e}")

    # Test invalid compression values
    print("\nTesting invalid compression values...")
    invalid_options = [
        {"COMPRESSION": "LZMA"},  # not supported
        {"COMPRESSION": "NONE"},  # not supported
        {"COMPRESSION": "COMPRESS"},  # not supported
    ]

    for option in invalid_options:
        try:
            unload_option = UnloadQueryOption(**option)
            print(f"✗ Should have failed for {option['COMPRESSION']}")
        except ValidationError as e:
            print(f"✓ Correctly rejected {option['COMPRESSION']}: {e}")

    # Test None value (should be valid)
    print("\nTesting None value...")
    try:
        unload_option = UnloadQueryOption(COMPRESSION=None)
        print("✓ None value is valid")
    except ValidationError as e:
        print(f"✗ None value should be valid: {e}")


if __name__ == "__main__":
    test_compression_validation()
