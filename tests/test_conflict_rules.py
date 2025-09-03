#!/usr/bin/env python3
"""Test script to verify conflict rules validation in UnloadQueryOption."""

import pytest

from redshift_query_builder.core import UnloadQueryOption


def test_cleanpath_allowoverwrite_conflict():
    """Test CLEANPATH and ALLOWOVERWRITE conflict."""
    with pytest.raises(
        ValueError,
        match="Conflicting options cannot be used together: ALLOWOVERWRITE, CLEANPATH",
    ):
        UnloadQueryOption(CLEANPATH=True, ALLOWOVERWRITE=True)


def test_fixedwidth_delimiter_conflict():
    """Test FIXEDWIDTH and DELIMITER conflict."""
    with pytest.raises(
        ValueError,
        match="Conflicting options cannot be used together: DELIMITER, FIXEDWIDTH",
    ):
        UnloadQueryOption(FIXEDWIDTH="0:10,1:20", DELIMITER=",")


def test_fixedwidth_header_conflict():
    """Test FIXEDWIDTH and HEADER conflict."""
    with pytest.raises(
        ValueError,
        match="Conflicting options cannot be used together: FIXEDWIDTH, HEADER",
    ):
        UnloadQueryOption(FIXEDWIDTH="0:10,1:20", HEADER=True)


def test_valid_combination():
    """Test valid combination of non-conflicting options."""
    options = UnloadQueryOption(
        FORMAT="CSV", DELIMITER=",", HEADER=True, COMPRESSION="GZIP"
    )
    assert options.FORMAT == "CSV"
    assert options.DELIMITER == ","
    assert options.HEADER is True
    assert options.COMPRESSION == "GZIP"


def test_single_options():
    """Test that single options work correctly."""
    # Test CLEANPATH alone
    options1 = UnloadQueryOption(CLEANPATH=True)
    assert options1.CLEANPATH is True
    assert options1.ALLOWOVERWRITE is False

    # Test ALLOWOVERWRITE alone
    options2 = UnloadQueryOption(ALLOWOVERWRITE=True)
    assert options2.ALLOWOVERWRITE is True
    assert options2.CLEANPATH is False

    # Test FIXEDWIDTH alone
    options3 = UnloadQueryOption(FIXEDWIDTH="0:10,1:20")
    assert options3.FIXEDWIDTH == "0:10,1:20"
    assert options3.DELIMITER is None

    # Test DELIMITER alone
    options4 = UnloadQueryOption(DELIMITER=",")
    assert options4.DELIMITER == ","
    assert options4.FIXEDWIDTH is None

    # Test HEADER alone
    options5 = UnloadQueryOption(HEADER=True)
    assert options5.HEADER is True
    assert options5.FIXEDWIDTH is None


def test_multiple_conflicts():
    """Test multiple conflicts at once."""
    with pytest.raises(
        ValueError, match="Conflicting options cannot be used together:"
    ):
        UnloadQueryOption(FIXEDWIDTH="0:10,1:20", DELIMITER=",", HEADER=True)


def test_falsy_values_ignored():
    """Test that falsy values are properly ignored."""
    # Test with None values for string fields
    options1 = UnloadQueryOption(
        CLEANPATH=True,
        FIXEDWIDTH=None,  # Should be ignored
    )
    assert options1.CLEANPATH is True
    assert options1.FIXEDWIDTH is None

    # Test with False values for boolean fields
    options2 = UnloadQueryOption(
        FIXEDWIDTH="0:10,1:20",
        HEADER=False,  # Should be ignored
    )
    assert options2.FIXEDWIDTH == "0:10,1:20"
    assert options2.HEADER is False

    # Test with None for string field
    options3 = UnloadQueryOption(
        HEADER=True,
        DELIMITER=None,  # Should be ignored (None is falsy)
    )
    assert options3.HEADER is True
    assert options3.DELIMITER is None


def test_all_options_falsy():
    """Test all options set to False/None."""
    options = UnloadQueryOption(
        CLEANPATH=False,
        ALLOWOVERWRITE=False,
        FIXEDWIDTH=None,
        DELIMITER=None,
        HEADER=False,
    )
    assert options.CLEANPATH is False
    assert options.ALLOWOVERWRITE is False
    assert options.FIXEDWIDTH is None
    assert options.DELIMITER is None
    assert options.HEADER is False


def test_csv_format_conflicts():
    """Test CSV format conflicts."""
    with pytest.raises(ValueError, match="CSV cannot be used with ESCAPE"):
        UnloadQueryOption(FORMAT="CSV", ESCAPE=True)


def test_parquet_format_conflicts():
    """Test PARQUET format conflicts."""
    with pytest.raises(
        ValueError, match="PARQUET cannot be used with DELIMITER, COMPRESSION"
    ):
        UnloadQueryOption(FORMAT="PARQUET", DELIMITER=",", COMPRESSION="GZIP")


def test_json_format_conflicts():
    """Test JSON format conflicts."""
    with pytest.raises(ValueError, match="JSON cannot be used with DELIMITER"):
        UnloadQueryOption(FORMAT="JSON", DELIMITER=",")


def test_conflict_rules_edge_cases():
    """Test edge cases for conflict rules."""
    # Test that None values are treated as falsy
    options = UnloadQueryOption(FIXEDWIDTH=None, DELIMITER=",")
    assert options.FIXEDWIDTH is None
    assert options.DELIMITER == ","
