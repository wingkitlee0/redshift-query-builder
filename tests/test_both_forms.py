#!/usr/bin/env python3

from redshift_query_builder.core import call_once


class TestBuilder:
    def __init__(self):
        self.called_functions = set()
        self.options = {}

    @call_once
    def set_format_simple(self, format: str):
        """Test @call_once form"""
        self.options["FORMAT"] = format
        return self

    @call_once()
    def set_compression_with_parens(self, compression: str):
        """Test @call_once() form"""
        self.options["COMPRESSION"] = compression
        return self


def test_both_forms():
    """Test that both @call_once and @call_once() work."""

    print("Testing both decorator forms...")

    # Test @call_once form
    print("\n1. Testing @call_once form:")
    builder1 = TestBuilder()

    try:
        builder1.set_format_simple("CSV")
        print("✓ First call to set_format_simple() succeeded")
    except Exception as e:
        print(f"✗ First call failed: {e}")
        return

    try:
        builder1.set_format_simple("JSON")
        print("✗ Second call should have failed")
    except ValueError as e:
        print(f"✓ Second call correctly failed: {e}")

    # Test @call_once() form
    print("\n2. Testing @call_once() form:")
    builder2 = TestBuilder()

    try:
        builder2.set_compression_with_parens("GZIP")
        print("✓ First call to set_compression_with_parens() succeeded")
    except Exception as e:
        print(f"✗ First call failed: {e}")
        return

    try:
        builder2.set_compression_with_parens("BZIP2")
        print("✗ Second call should have failed")
    except ValueError as e:
        print(f"✓ Second call correctly failed: {e}")

    print("\n✓ Both forms work correctly!")


if __name__ == "__main__":
    test_both_forms()
