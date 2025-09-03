#!/usr/bin/env python3
"""
Comprehensive test suite for UNLOAD query validation rules.
Tests all validation methods in UnloadQueryOption class.
"""

import pytest
from pydantic import ValidationError

from redshift_query_builder.core import (
    ManifestOption,
    PartitionByOption,
    UnloadQueryOption,
)


class TestFormatAsValidation:
    """Test FORMAT AS validation rules."""

    def test_valid_csv_format(self):
        """Test that CSV format is accepted."""
        options = UnloadQueryOption(FORMAT="CSV")
        assert options.FORMAT == "CSV"

    def test_valid_parquet_format(self):
        """Test that PARQUET format is accepted."""
        options = UnloadQueryOption(FORMAT="PARQUET")
        assert options.FORMAT == "PARQUET"

    def test_valid_json_format(self):
        """Test that JSON format is accepted."""
        options = UnloadQueryOption(FORMAT="JSON")
        assert options.FORMAT == "JSON"

    def test_invalid_format(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(
            ValidationError, match="Input should be 'CSV', 'PARQUET' or 'JSON'"
        ):
            UnloadQueryOption(FORMAT="INVALID")  # type: ignore

    def test_csv_with_escape_conflict(self):
        """Test that CSV cannot be used with ESCAPE."""
        with pytest.raises(ValidationError, match="CSV cannot be used with ESCAPE"):
            UnloadQueryOption(FORMAT="CSV", ESCAPE=True)

    def test_csv_with_fixedwidth_conflict(self):
        """Test that CSV cannot be used with FIXEDWIDTH."""
        with pytest.raises(ValidationError, match="CSV cannot be used with FIXEDWIDTH"):
            UnloadQueryOption(FORMAT="CSV", FIXEDWIDTH="0:10,1:20")

    def test_csv_with_addquotes_conflict(self):
        """Test that CSV cannot be used with ADDQUOTES."""
        with pytest.raises(ValidationError, match="CSV cannot be used with ADDQUOTES"):
            UnloadQueryOption(FORMAT="CSV", ADDQUOTES=True)

    def test_parquet_with_delimiter_conflict(self):
        """Test that PARQUET cannot be used with DELIMITER."""
        with pytest.raises(
            ValidationError, match="PARQUET cannot be used with DELIMITER"
        ):
            UnloadQueryOption(FORMAT="PARQUET", DELIMITER=",")

    def test_parquet_with_header_conflict(self):
        """Test that PARQUET cannot be used with HEADER."""
        with pytest.raises(ValidationError, match="PARQUET cannot be used with HEADER"):
            UnloadQueryOption(FORMAT="PARQUET", HEADER=True)

    def test_json_with_delimiter_conflict(self):
        """Test that JSON cannot be used with DELIMITER."""
        with pytest.raises(ValidationError, match="JSON cannot be used with DELIMITER"):
            UnloadQueryOption(FORMAT="JSON", DELIMITER=",")


class TestPartitionByValidation:
    """Test PARTITION BY validation rules."""

    def test_partition_by_single_column(self):
        """Test partition by single column."""
        partition = PartitionByOption(columns=["year"], include=False)
        options = UnloadQueryOption(PARTITION_BY=partition)
        assert options.PARTITION_BY.columns == ["year"]  # type: ignore
        assert options.PARTITION_BY.include is False  # type: ignore

    def test_partition_by_multiple_columns(self):
        """Test partition by multiple columns."""
        partition = PartitionByOption(columns=["year", "month"], include=True)
        options = UnloadQueryOption(PARTITION_BY=partition)
        assert options.PARTITION_BY.columns == ["year", "month"]  # type: ignore
        assert options.PARTITION_BY.include is True  # type: ignore

    def test_partition_by_with_include(self):
        """Test partition by with include."""
        partition = PartitionByOption(columns=["year"], include=True)
        options = UnloadQueryOption(PARTITION_BY=partition)
        assert options.PARTITION_BY.include is True

    def test_partition_by_empty_columns(self):
        """Test that empty columns list raises error."""
        with pytest.raises(
            ValidationError, match="PARTITION_BY columns list cannot be empty"
        ):
            PartitionByOption(columns=[], include=False)

    def test_partition_by_invalid_columns(self):
        """Test that invalid columns raise error."""
        with pytest.raises(
            ValidationError, match="All PARTITION_BY columns must be non-empty strings"
        ):
            PartitionByOption(columns=["", "col2"], include=False)


class TestManifestValidation:
    """Test MANIFEST validation rules."""

    def test_manifest_false(self):
        """Test manifest false."""
        options = UnloadQueryOption(MANIFEST={"enable": False, "verbose": False})  # type: ignore
        assert options.MANIFEST.enable is False  # type: ignore
        assert options.MANIFEST.verbose is False  # type: ignore

    def test_manifest_true(self):
        """Test manifest true."""
        options = UnloadQueryOption(MANIFEST={"enable": True, "verbose": False})  # type: ignore
        assert options.MANIFEST.enable is True  # type: ignore
        assert options.MANIFEST.verbose is False  # type: ignore


class TestHeaderValidation:
    """Test HEADER validation rules."""

    def test_header_true(self):
        """Test header true."""
        options = UnloadQueryOption(HEADER=True)
        assert options.HEADER is True

    def test_header_false(self):
        """Test header false."""
        options = UnloadQueryOption(HEADER=False)
        assert options.HEADER is False


class TestDelimiterValidation:
    """Test DELIMITER validation rules."""

    def test_valid_single_character_delimiter(self):
        """Test valid single character delimiter."""
        options = UnloadQueryOption(DELIMITER=",")
        assert options.DELIMITER == ","

    def test_valid_pipe_delimiter(self):
        """Test valid pipe delimiter."""
        options = UnloadQueryOption(DELIMITER="|")
        assert options.DELIMITER == "|"

    def test_valid_tab_delimiter(self):
        """Test valid tab delimiter."""
        options = UnloadQueryOption(DELIMITER="\t")
        assert options.DELIMITER == "\t"

    def test_empty_delimiter(self):
        """Test empty delimiter."""
        with pytest.raises(
            ValidationError, match="String should have at least 1 character"
        ):
            UnloadQueryOption(DELIMITER="")

    def test_multiple_character_delimiter(self):
        """Test that multiple character delimiter raises error."""
        with pytest.raises(
            ValidationError, match="String should have at most 1 character"
        ):
            UnloadQueryOption(DELIMITER="invalid")


class TestFixedwidthValidation:
    """Test FIXEDWIDTH validation rules."""

    def test_valid_single_column_fixedwidth(self):
        """Test valid single column fixedwidth."""
        options = UnloadQueryOption(FIXEDWIDTH="0:10")
        assert options.FIXEDWIDTH == "0:10"

    def test_valid_multiple_columns_fixedwidth(self):
        """Test valid multiple columns fixedwidth."""
        options = UnloadQueryOption(FIXEDWIDTH="0:3,1:100,2:30")
        assert options.FIXEDWIDTH == "0:3,1:100,2:30"

    def test_valid_fixedwidth_example_from_redshift(self):
        """Test valid fixedwidth example from Redshift documentation."""
        options = UnloadQueryOption(FIXEDWIDTH="0:3,1:100,2:30,3:2,4:6")
        assert options.FIXEDWIDTH == "0:3,1:100,2:30,3:2,4:6"

    def test_valid_fixedwidth_large_numbers(self):
        """Test valid fixedwidth with large column IDs and widths."""
        options = UnloadQueryOption(FIXEDWIDTH="10:50,25:100,100:200")
        assert options.FIXEDWIDTH == "10:50,25:100,100:200"

    def test_empty_fixedwidth(self):
        """Test that empty fixedwidth raises error."""
        with pytest.raises(
            ValidationError, match="String should have at least 3 characters"
        ):
            UnloadQueryOption(FIXEDWIDTH="")

    def test_fixedwidth_too_short(self):
        """Test that fixedwidth shorter than 3 characters raises error."""
        with pytest.raises(
            ValidationError, match="String should have at least 3 characters"
        ):
            UnloadQueryOption(FIXEDWIDTH="0:")  # type: ignore

    def test_invalid_fixedwidth_missing_colon(self):
        """Test that fixedwidth without colon raises error."""
        with pytest.raises(
            ValidationError, match="String should have at least 3 characters"
        ):
            UnloadQueryOption(FIXEDWIDTH="01")

    def test_invalid_fixedwidth_missing_width(self):
        """Test that fixedwidth without width raises error."""
        with pytest.raises(
            ValidationError, match="String should have at least 3 characters"
        ):
            UnloadQueryOption(FIXEDWIDTH="0:")

    def test_invalid_fixedwidth_non_numeric_width(self):
        """Test that fixedwidth with non-numeric width raises error."""
        with pytest.raises(ValidationError, match="FIXEDWIDTH must be in format"):
            UnloadQueryOption(FIXEDWIDTH="0:abc")

    def test_invalid_fixedwidth_non_numeric_column_id(self):
        """Test that fixedwidth with non-numeric column ID raises error."""
        with pytest.raises(ValidationError, match="FIXEDWIDTH must be in format"):
            UnloadQueryOption(FIXEDWIDTH="col1:10")  # type: ignore

    def test_invalid_fixedwidth_wrong_separator(self):
        """Test that fixedwidth with wrong separator raises error."""
        with pytest.raises(ValidationError, match="FIXEDWIDTH must be in format"):
            UnloadQueryOption(FIXEDWIDTH="0:10;1:20")  # type: ignore

    def test_invalid_fixedwidth_missing_width_in_second_column(self):
        """Test that fixedwidth with missing width in second column raises error."""
        with pytest.raises(ValidationError, match="FIXEDWIDTH must be in format"):
            UnloadQueryOption(FIXEDWIDTH="0:10,1:")  # type: ignore


class TestEncryptedValidation:
    """Test ENCRYPTED validation rules."""

    def test_encrypted_true(self):
        """Test encrypted true."""
        options = UnloadQueryOption(ENCRYPTED=True)
        assert options.ENCRYPTED is True

    def test_encrypted_false(self):
        """Test encrypted false."""
        options = UnloadQueryOption(ENCRYPTED=False)
        assert options.ENCRYPTED is False


class TestCompressionValidation:
    """Test COMPRESSION validation rules."""

    def test_gzip_compression(self):
        """Test gzip compression."""
        options = UnloadQueryOption(COMPRESSION="GZIP")
        assert options.COMPRESSION == "GZIP"

    def test_bzip2_compression(self):
        """Test bzip2 compression."""
        options = UnloadQueryOption(COMPRESSION="BZIP2")
        assert options.COMPRESSION == "BZIP2"

    def test_zstd_compression(self):
        """Test zstd compression."""
        options = UnloadQueryOption(COMPRESSION="ZSTD")
        assert options.COMPRESSION == "ZSTD"

    def test_unsupported_compression_error(self):
        """Test that unsupported compression raises error."""
        with pytest.raises(
            ValidationError, match="Input should be 'GZIP', 'BZIP2' or 'ZSTD'"
        ):
            UnloadQueryOption(COMPRESSION="INVALID")  # type: ignore


class TestAddquotesValidation:
    """Test ADDQUOTES validation rules."""

    def test_addquotes_true(self):
        """Test addquotes true."""
        options = UnloadQueryOption(ADDQUOTES=True)
        assert options.ADDQUOTES is True

    def test_addquotes_false(self):
        """Test addquotes false."""
        options = UnloadQueryOption(ADDQUOTES=False)
        assert options.ADDQUOTES is False


class TestNullValidation:
    """Test NULL validation rules."""

    def test_null_with_string(self):
        """Test null with string."""
        options = UnloadQueryOption(NULL="NULL")
        assert options.NULL == "NULL"

    def test_null_with_empty_string(self):
        """Test null with empty string."""
        options = UnloadQueryOption(NULL="")
        assert options.NULL == ""


class TestEscapeValidation:
    """Test ESCAPE validation rules."""

    def test_escape_true(self):
        """Test escape true."""
        options = UnloadQueryOption(ESCAPE=True)
        assert options.ESCAPE is True

    def test_escape_false(self):
        """Test escape false."""
        options = UnloadQueryOption(ESCAPE=False)
        assert options.ESCAPE is False


class TestAllowoverwriteValidation:
    """Test ALLOWOVERWRITE validation rules."""

    def test_allowoverwrite_true(self):
        """Test allowoverwrite true."""
        options = UnloadQueryOption(ALLOWOVERWRITE=True)
        assert options.ALLOWOVERWRITE is True

    def test_allowoverwrite_false(self):
        """Test allowoverwrite false."""
        options = UnloadQueryOption(ALLOWOVERWRITE=False)
        assert options.ALLOWOVERWRITE is False


class TestCleanpathValidation:
    """Test CLEANPATH validation rules."""

    def test_cleanpath_true(self):
        """Test cleanpath true."""
        options = UnloadQueryOption(CLEANPATH=True)
        assert options.CLEANPATH is True

    def test_cleanpath_false(self):
        """Test cleanpath false."""
        options = UnloadQueryOption(CLEANPATH=False)
        assert options.CLEANPATH is False


class TestParallelValidation:
    """Test PARALLEL validation rules."""

    def test_parallel_true(self):
        """Test parallel true."""
        options = UnloadQueryOption(PARALLEL=True)
        assert options.PARALLEL is True

    def test_parallel_false(self):
        """Test parallel false."""
        options = UnloadQueryOption(PARALLEL=False)
        assert options.PARALLEL is False


class TestMaxfilesizeValidation:
    """Test MAXFILESIZE validation rules."""

    def test_valid_maxfilesize_mb(self):
        """Test valid maxfilesize with MB."""
        options = UnloadQueryOption(MAXFILESIZE="100 MB")
        assert options.MAXFILESIZE == "100 MB"

    def test_valid_maxfilesize_gb(self):
        """Test valid maxfilesize with GB."""
        options = UnloadQueryOption(MAXFILESIZE="1 GB")
        assert options.MAXFILESIZE == "1 GB"

    def test_valid_maxfilesize_no_unit(self):
        """Test valid maxfilesize without unit."""
        options = UnloadQueryOption(MAXFILESIZE="100")
        assert options.MAXFILESIZE == "100"

    def test_valid_maxfilesize_decimal(self):
        """Test valid maxfilesize with decimal."""
        options = UnloadQueryOption(MAXFILESIZE="1.5 GB")
        assert options.MAXFILESIZE == "1.5 GB"

    def test_empty_maxfilesize(self):
        """Test empty maxfilesize."""
        options = UnloadQueryOption(MAXFILESIZE="")
        assert options.MAXFILESIZE == ""

    def test_non_string_maxfilesize(self):
        """Test that non-string maxfilesize raises error."""
        # This should work since we're using Pydantic's type validation
        options = UnloadQueryOption(MAXFILESIZE="100")
        assert options.MAXFILESIZE == "100"


class TestRowgroupsizeValidation:
    """Test ROWGROUPSIZE validation rules."""

    def test_valid_rowgroupsize_mb(self):
        """Test valid rowgroupsize with MB."""
        options = UnloadQueryOption(ROWGROUPSIZE="64 MB")
        assert options.ROWGROUPSIZE == "64 MB"

    def test_valid_rowgroupsize_gb(self):
        """Test valid rowgroupsize with GB."""
        options = UnloadQueryOption(ROWGROUPSIZE="1 GB")
        assert options.ROWGROUPSIZE == "1 GB"

    def test_valid_rowgroupsize_no_unit(self):
        """Test valid rowgroupsize without unit."""
        options = UnloadQueryOption(ROWGROUPSIZE="128")
        assert options.ROWGROUPSIZE == "128"

    def test_valid_rowgroupsize_decimal(self):
        """Test valid rowgroupsize with decimal."""
        options = UnloadQueryOption(ROWGROUPSIZE="0.5 GB")
        assert options.ROWGROUPSIZE == "0.5 GB"

    def test_empty_rowgroupsize(self):
        """Test empty rowgroupsize."""
        options = UnloadQueryOption(ROWGROUPSIZE="")
        assert options.ROWGROUPSIZE == ""

    def test_non_string_rowgroupsize(self):
        """Test that non-string rowgroupsize raises error."""
        # This should work since we're using Pydantic's type validation
        options = UnloadQueryOption(ROWGROUPSIZE="64")
        assert options.ROWGROUPSIZE == "64"


class TestRegionValidation:
    """Test REGION validation rules."""

    def test_valid_region_us_east_1(self):
        """Test valid region us-east-1."""
        options = UnloadQueryOption(REGION="us-east-1")
        assert options.REGION == "us-east-1"

    def test_valid_region_eu_west_1(self):
        """Test valid region eu-west-1."""
        options = UnloadQueryOption(REGION="eu-west-1")
        assert options.REGION == "eu-west-1"

    def test_valid_region_ap_southeast_1(self):
        """Test valid region ap-southeast-1."""
        options = UnloadQueryOption(REGION="ap-southeast-1")
        assert options.REGION == "ap-southeast-1"

    def test_empty_region(self):
        """Test empty region."""
        options = UnloadQueryOption(REGION=None)
        assert options.REGION is None

    def test_non_string_region(self):
        """Test that non-string region raises error."""
        # This should work since we're using Pydantic's type validation
        options = UnloadQueryOption(REGION="us-east-1")
        assert options.REGION == "us-east-1"

    def test_invalid_region_format(self):
        """Test that invalid region format raises error."""
        with pytest.raises(
            ValidationError, match="REGION must be a valid AWS region format"
        ):
            UnloadQueryOption(REGION="invalid-region")


class TestExtensionValidation:
    """Test EXTENSION validation rules."""

    def test_valid_extension_csv(self):
        """Test valid extension csv."""
        options = UnloadQueryOption(EXTENSION="csv")
        assert options.EXTENSION == "csv"

    def test_valid_extension_json(self):
        """Test valid extension json."""
        options = UnloadQueryOption(EXTENSION="json")
        assert options.EXTENSION == "json"

    def test_valid_extension_parquet(self):
        """Test valid extension parquet."""
        options = UnloadQueryOption(EXTENSION="parquet")
        assert options.EXTENSION == "parquet"

    def test_empty_extension(self):
        """Test empty extension."""
        options = UnloadQueryOption(EXTENSION="")
        assert options.EXTENSION == ""

    def test_non_string_extension(self):
        """Test that non-string extension raises error."""
        # This should work since we're using Pydantic's type validation
        options = UnloadQueryOption(EXTENSION="csv")
        assert options.EXTENSION == "csv"

    def test_extension_with_dot(self):
        """Test that extension with dot raises error."""
        with pytest.raises(
            ValidationError, match="EXTENSION should not start with a dot"
        ):
            UnloadQueryOption(EXTENSION=".csv")


class TestModelValidation:
    """Test model validation rules."""

    def test_valid_options_dict(self):
        """Test valid options dictionary."""
        options = UnloadQueryOption.model_validate(
            {
                "FORMAT": "CSV",
                "HEADER": True,
                "DELIMITER": ",",
                "COMPRESSION": "GZIP",
            }
        )
        assert options.FORMAT == "CSV"
        assert options.HEADER is True
        assert options.DELIMITER == ","
        assert options.COMPRESSION == "GZIP"

    def test_invalid_option_key(self):
        """Test that invalid option key raises error."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            UnloadQueryOption.model_validate(
                {"INVALID_OPTION": "value", "FORMAT": "CSV"}
            )


class TestToOptionsString:
    """Test to_options_string method."""

    def test_empty_options(self):
        """Test empty options."""
        options = UnloadQueryOption()
        result = options.to_options_string()
        assert result == "PARALLEL OFF"  # Default value

    def test_single_option(self):
        """Test single option."""
        options = UnloadQueryOption(FORMAT="CSV")
        result = options.to_options_string()
        assert result == "FORMAT AS CSV\nPARALLEL OFF"  # Includes default

    def test_multiple_options(self):
        """Test multiple options."""
        options = UnloadQueryOption(
            FORMAT="CSV",
            HEADER=True,
            DELIMITER=",",
            COMPRESSION="GZIP",
        )
        result = options.to_options_string()
        assert "FORMAT AS CSV" in result
        assert "HEADER" in result
        assert "DELIMITER AS ','" in result
        assert "GZIP" in result
        assert "PARALLEL OFF" in result

    def test_boolean_options_only_true(self):
        """Test that only True boolean options appear."""
        options = UnloadQueryOption(
            HEADER=True,
            ADDQUOTES=False,  # Should not appear
            ESCAPE=True,
            ALLOWOVERWRITE=False,  # Should not appear
        )
        result = options.to_options_string()
        assert "HEADER" in result
        assert "ADDQUOTES" not in result
        assert "ESCAPE" in result
        assert "ALLOWOVERWRITE" not in result

    def test_partition_by_option(self):
        """Test partition by option."""
        partition = PartitionByOption(columns=["year", "month"], include=True)
        options = UnloadQueryOption(PARTITION_BY=partition)
        result = options.to_options_string()
        assert "PARTITION BY (year,month) INCLUDE" in result
