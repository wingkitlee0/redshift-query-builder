#!/usr/bin/env python3
"""
Comprehensive test suite for UnloadQueryBuilder functionality.
Tests all builder methods and their interactions.
"""

import pytest

from redshift_query_builder.core import UnloadQueryAndParams, UnloadQueryBuilder


class TestBuilderInitialization:
    """Test UnloadQueryBuilder initialization."""

    def test_builder_initialization(self):
        """Test that builder initializes with all attributes as None."""
        builder = UnloadQueryBuilder()
        assert builder.select_template is None
        assert builder.select_params == {}
        assert builder.to_path is None
        assert builder.authorization is None
        assert builder.options == {}


class TestSelectTemplateMethods:
    """Test select template related methods."""

    def test_add_select_template_success(self):
        """Test successful addition of select template."""
        builder = UnloadQueryBuilder()
        result = builder.add_select_template("SELECT * FROM table")
        assert builder.select_template == "SELECT * FROM table"
        assert result is builder  # Test method chaining

    def test_add_select_template_already_set(self):
        """Test that adding select template twice raises error."""
        builder = UnloadQueryBuilder()
        builder.add_select_template("SELECT * FROM table")
        with pytest.raises(ValueError, match="add_select_template is already called"):
            builder.add_select_template("SELECT * FROM another_table")

    def test_add_select_params_success(self):
        """Test successful addition of select params."""
        builder = UnloadQueryBuilder()
        params = {"date": "2024-01-01"}
        result = builder.add_select_params(params)
        assert builder.select_params == params
        assert result is builder  # Test method chaining

    def test_add_select_params_already_set(self):
        """Test that adding select params twice raises error."""
        builder = UnloadQueryBuilder()
        builder.add_select_params({"date": "2024-01-01"})
        with pytest.raises(ValueError, match="add_select_params is already called"):
            builder.add_select_params({"date": "2024-01-02"})


class TestPathMethods:
    """Test path related methods."""

    def test_add_to_path_success(self):
        """Test successful addition of to_path."""
        builder = UnloadQueryBuilder()
        result = builder.add_to_path("s3://bucket/path/")
        assert builder.to_path == "s3://bucket/path/"
        assert result is builder  # Test method chaining

    def test_add_to_path_already_set(self):
        """Test that adding to_path twice raises error."""
        builder = UnloadQueryBuilder()
        builder.add_to_path("s3://bucket/path/")
        with pytest.raises(ValueError, match="add_to_path is already called"):
            builder.add_to_path("s3://another-bucket/path/")


class TestAuthorizationMethods:
    """Test authorization related methods."""

    def test_add_iam_role_authorization_single_tuple(self):
        """Test IAM role authorization with single tuple."""
        builder = UnloadQueryBuilder()
        result = builder.add_iam_role_authorization(("123456789012", "MyRole"))
        expected = "IAM_ROLE arn:aws:iam::123456789012:role/MyRole"
        assert builder.authorization == expected
        assert result is builder  # Test method chaining

    def test_add_iam_role_authorization_list(self):
        """Test IAM role authorization with list of tuples."""
        builder = UnloadQueryBuilder()
        roles = [("123456789012", "MyRole"), ("987654321098", "AnotherRole")]
        result = builder.add_iam_role_authorization(roles)
        expected = "IAM_ROLE arn:aws:iam::123456789012:role/MyRole, arn:aws:iam::987654321098:role/AnotherRole"
        assert builder.authorization == expected
        assert result is builder  # Test method chaining

    def test_add_iam_role_authorization_already_set(self):
        """Test that adding IAM role authorization twice raises error."""
        builder = UnloadQueryBuilder()
        builder.add_iam_role_authorization(("123456789012", "MyRole"))
        with pytest.raises(ValueError, match="authorization is already set"):
            builder.add_iam_role_authorization(("987654321098", "AnotherRole"))

    def test_add_default_authorization_success(self):
        """Test successful addition of default authorization."""
        builder = UnloadQueryBuilder()
        result = builder.add_default_authorization()
        assert builder.authorization == "IAM_ROLE default"
        assert result is builder  # Test method chaining

    def test_add_default_authorization_already_set(self):
        """Test that adding default authorization twice raises error."""
        builder = UnloadQueryBuilder()
        builder.add_default_authorization()
        with pytest.raises(ValueError, match="authorization is already set"):
            builder.add_default_authorization()


class TestFormatMethods:
    """Test format related methods."""

    def test_set_format_csv(self):
        """Test setting CSV format."""
        builder = UnloadQueryBuilder()
        result = builder.set_format("CSV")
        assert builder.options["FORMAT"] == "CSV"
        assert result is builder  # Test method chaining

    def test_set_format_parquet(self):
        """Test setting PARQUET format."""
        builder = UnloadQueryBuilder()
        result = builder.set_format("PARQUET")
        assert builder.options["FORMAT"] == "PARQUET"
        assert result is builder  # Test method chaining

    def test_set_format_json(self):
        """Test setting JSON format."""
        builder = UnloadQueryBuilder()
        result = builder.set_format("JSON")
        assert builder.options["FORMAT"] == "JSON"
        assert result is builder  # Test method chaining

    def test_set_format_already_set(self):
        """Test that setting format twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_format("CSV")
        with pytest.raises(ValueError, match="set_format is already called"):
            builder.set_format("PARQUET")


class TestPartitionByMethods:
    """Test partition by related methods."""

    def test_set_partition_by_single_column(self):
        """Test setting partition by single column."""
        builder = UnloadQueryBuilder()
        result = builder.set_partition_by("year")
        expected = {"columns": ["year"], "include": False}
        assert builder.options["PARTITION_BY"] == expected
        assert result is builder  # Test method chaining

    def test_set_partition_by_multiple_columns(self):
        """Test setting partition by multiple columns."""
        builder = UnloadQueryBuilder()
        result = builder.set_partition_by(["year", "month"])
        expected = {"columns": ["year", "month"], "include": False}
        assert builder.options["PARTITION_BY"] == expected
        assert result is builder  # Test method chaining

    def test_set_partition_by_with_include(self):
        """Test setting partition by with include option."""
        builder = UnloadQueryBuilder()
        result = builder.set_partition_by(["year", "month"], include=True)
        expected = {"columns": ["year", "month"], "include": True}
        assert builder.options["PARTITION_BY"] == expected
        assert result is builder  # Test method chaining

    def test_set_partition_by_already_set(self):
        """Test that setting partition by twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_partition_by("year")
        with pytest.raises(ValueError, match="set_partition_by is already called"):
            builder.set_partition_by("month")


class TestManifestMethods:
    """Test manifest related methods."""

    def test_set_manifest_false(self):
        """Test setting manifest with verbose=False."""
        builder = UnloadQueryBuilder()
        result = builder.set_manifest(enable=True, verbose=False)
        assert builder.options["MANIFEST"]["enable"] is True
        assert builder.options["MANIFEST"]["verbose"] is False
        assert result is builder  # Test method chaining

    def test_set_manifest_true(self):
        """Test setting manifest with verbose=True."""
        builder = UnloadQueryBuilder()
        result = builder.set_manifest(enable=True, verbose=True)
        assert builder.options["MANIFEST"]["enable"] is True
        assert builder.options["MANIFEST"]["verbose"] is True
        assert result is builder  # Test method chaining

    def test_set_manifest_already_set(self):
        """Test that setting manifest twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_manifest()
        with pytest.raises(ValueError, match="set_manifest is already called"):
            builder.set_manifest()


class TestHeaderMethods:
    """Test header related methods."""

    def test_set_header_success(self):
        """Test setting header."""
        builder = UnloadQueryBuilder()
        result = builder.set_header()
        assert builder.options["HEADER"] is True
        assert result is builder  # Test method chaining

    def test_set_header_already_set(self):
        """Test that setting header twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_header()
        with pytest.raises(ValueError, match="set_header is already called"):
            builder.set_header()


class TestCompressionMethods:
    """Test compression related methods."""

    def test_set_compression_gzip(self):
        """Test setting GZIP compression."""
        builder = UnloadQueryBuilder()
        result = builder.set_compression("GZIP")
        assert builder.options["COMPRESSION"] == "GZIP"
        assert result is builder  # Test method chaining

    def test_set_compression_bzip2(self):
        """Test setting BZIP2 compression."""
        builder = UnloadQueryBuilder()
        result = builder.set_compression("BZIP2")
        assert builder.options["COMPRESSION"] == "BZIP2"
        assert result is builder  # Test method chaining

    def test_set_compression_zstd(self):
        """Test setting ZSTD compression."""
        builder = UnloadQueryBuilder()
        result = builder.set_compression("ZSTD")
        assert builder.options["COMPRESSION"] == "ZSTD"
        assert result is builder  # Test method chaining

    def test_set_compression_already_set(self):
        """Test that setting compression twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_compression("GZIP")
        with pytest.raises(ValueError, match="set_compression is already called"):
            builder.set_compression("BZIP2")

    def test_set_compression_invalid(self):
        """Test that setting invalid compression raises error."""
        builder = UnloadQueryBuilder()
        with pytest.raises(ValueError, match="Invalid compression: INVALID"):
            builder.set_compression("INVALID")


class TestDelimiterMethods:
    """Test delimiter related methods."""

    def test_set_delimiter_success(self):
        """Test setting delimiter."""
        builder = UnloadQueryBuilder()
        result = builder.set_delimiter(",")
        assert builder.options["DELIMITER"] == ","
        assert result is builder  # Test method chaining

    def test_set_delimiter_already_set(self):
        """Test that setting delimiter twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_delimiter(",")
        with pytest.raises(ValueError, match="set_delimiter is already called"):
            builder.set_delimiter("|")


class TestFixedwidthMethods:
    """Test fixedwidth related methods."""

    def test_set_fixedwidth_success(self):
        """Test setting fixedwidth."""
        builder = UnloadQueryBuilder()
        result = builder.set_fixedwidth("1:10,2:20")
        assert builder.options["FIXEDWIDTH"] == "1:10,2:20"
        assert result is builder  # Test method chaining

    def test_set_fixedwidth_already_set(self):
        """Test that setting fixedwidth twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_fixedwidth("1:10,2:20")
        with pytest.raises(ValueError, match="set_fixedwidth is already called"):
            builder.set_fixedwidth("3:15,4:25")


class TestEncryptedMethods:
    """Test encrypted related methods."""

    def test_set_encrypted_false(self):
        """Test setting encrypted with enable=False."""
        builder = UnloadQueryBuilder()
        result = builder.set_encrypted(enable=False)
        assert builder.options["ENCRYPTED"] is False
        assert result is builder  # Test method chaining

    def test_set_encrypted_true(self):
        """Test setting encrypted with enable=True."""
        builder = UnloadQueryBuilder()
        result = builder.set_encrypted(enable=True)
        assert builder.options["ENCRYPTED"] is True
        assert result is builder  # Test method chaining

    def test_set_encrypted_already_set(self):
        """Test that setting encrypted twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_encrypted()
        with pytest.raises(ValueError, match="set_encrypted is already called"):
            builder.set_encrypted()


class TestAddquotesMethods:
    """Test addquotes related methods."""

    def test_set_addquotes_success(self):
        """Test setting addquotes."""
        builder = UnloadQueryBuilder()
        result = builder.set_addquotes()
        assert builder.options["ADDQUOTES"] is True
        assert result is builder  # Test method chaining

    def test_set_addquotes_already_set(self):
        """Test that setting addquotes twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_addquotes()
        with pytest.raises(ValueError, match="set_addquotes is already called"):
            builder.set_addquotes()


class TestNullMethods:
    """Test null related methods."""

    def test_set_null_as_success(self):
        """Test setting null as."""
        builder = UnloadQueryBuilder()
        result = builder.set_null("NULL")
        assert builder.options["NULL"] == "NULL"
        assert result is builder  # Test method chaining

    def test_set_null_as_already_set(self):
        """Test that setting null as twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_null("NULL")
        with pytest.raises(ValueError, match="set_null is already called"):
            builder.set_null("EMPTY")


class TestEscapeMethods:
    """Test escape related methods."""

    def test_set_escape_success(self):
        """Test setting escape."""
        builder = UnloadQueryBuilder()
        result = builder.set_escape()
        assert builder.options["ESCAPE"] is True
        assert result is builder  # Test method chaining

    def test_set_escape_already_set(self):
        """Test that setting escape twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_escape()
        with pytest.raises(ValueError, match="set_escape is already called"):
            builder.set_escape()


class TestAllowoverwriteMethods:
    """Test allowoverwrite related methods."""

    def test_set_allowoverwrite_success(self):
        """Test setting allowoverwrite."""
        builder = UnloadQueryBuilder()
        result = builder.set_allowoverwrite()
        assert builder.options["ALLOWOVERWRITE"] is True
        assert result is builder  # Test method chaining

    def test_set_allowoverwrite_already_set(self):
        """Test that setting allowoverwrite twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_allowoverwrite()
        with pytest.raises(ValueError, match="set_allowoverwrite is already called"):
            builder.set_allowoverwrite()


class TestCleanpathMethods:
    """Test cleanpath related methods."""

    def test_set_cleanpath_success(self):
        """Test setting cleanpath."""
        builder = UnloadQueryBuilder()
        result = builder.set_cleanpath()
        assert builder.options["CLEANPATH"] is True
        assert result is builder  # Test method chaining

    def test_set_cleanpath_already_set(self):
        """Test that setting cleanpath twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_cleanpath()
        with pytest.raises(ValueError, match="set_cleanpath is already called"):
            builder.set_cleanpath()


class TestParallelMethods:
    """Test parallel related methods."""

    def test_set_parallel_boolean_true(self):
        """Test setting parallel with boolean True."""
        builder = UnloadQueryBuilder()
        result = builder.set_parallel(True)
        assert builder.options["PARALLEL"] == "ON"
        assert result is builder  # Test method chaining

    def test_set_parallel_boolean_false(self):
        """Test setting parallel with boolean False."""
        builder = UnloadQueryBuilder()
        result = builder.set_parallel(False)
        assert builder.options["PARALLEL"] == "OFF"
        assert result is builder  # Test method chaining

    def test_set_parallel_string_on(self):
        """Test setting parallel with string 'ON'."""
        builder = UnloadQueryBuilder()
        result = builder.set_parallel("ON")
        assert builder.options["PARALLEL"] == "ON"
        assert result is builder  # Test method chaining

    def test_set_parallel_string_off(self):
        """Test setting parallel with string 'OFF'."""
        builder = UnloadQueryBuilder()
        result = builder.set_parallel("OFF")
        assert builder.options["PARALLEL"] == "OFF"
        assert result is builder  # Test method chaining

    def test_set_parallel_string_true(self):
        """Test setting parallel with string 'TRUE'."""
        builder = UnloadQueryBuilder()
        result = builder.set_parallel("TRUE")
        assert builder.options["PARALLEL"] == "ON"
        assert result is builder  # Test method chaining

    def test_set_parallel_string_false(self):
        """Test setting parallel with string 'FALSE'."""
        builder = UnloadQueryBuilder()
        result = builder.set_parallel("FALSE")
        assert builder.options["PARALLEL"] == "OFF"
        assert result is builder  # Test method chaining

    def test_set_parallel_invalid(self):
        """Test that setting invalid parallel raises error."""
        builder = UnloadQueryBuilder()
        with pytest.raises(ValueError, match="Invalid parallel value: INVALID"):
            builder.set_parallel("INVALID")

    def test_set_parallel_already_set(self):
        """Test that setting parallel twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_parallel(True)
        with pytest.raises(ValueError, match="set_parallel is already called"):
            builder.set_parallel(False)


class TestMaxfilesizeMethods:
    """Test maxfilesize related methods."""

    def test_set_maxfilesize_success(self):
        """Test setting maxfilesize."""
        builder = UnloadQueryBuilder()
        result = builder.set_maxfilesize("100 MB")
        assert builder.options["MAXFILESIZE"] == "100 MB"
        assert result is builder  # Test method chaining

    def test_set_maxfilesize_already_set(self):
        """Test that setting maxfilesize twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_maxfilesize("100 MB")
        with pytest.raises(ValueError, match="set_maxfilesize is already called"):
            builder.set_maxfilesize("200 MB")


class TestRowgroupsizeMethods:
    """Test rowgroupsize related methods."""

    def test_set_rowgroupsize_success(self):
        """Test setting rowgroupsize."""
        builder = UnloadQueryBuilder()
        result = builder.set_rowgroupsize("64 MB")
        assert builder.options["ROWGROUPSIZE"] == "64 MB"
        assert result is builder  # Test method chaining

    def test_set_rowgroupsize_already_set(self):
        """Test that setting rowgroupsize twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_rowgroupsize("64 MB")
        with pytest.raises(ValueError, match="set_rowgroupsize is already called"):
            builder.set_rowgroupsize("128 MB")


class TestRegionMethods:
    """Test region related methods."""

    def test_set_region_success(self):
        """Test setting region."""
        builder = UnloadQueryBuilder()
        result = builder.set_region("us-east-1")
        assert builder.options["REGION"] == "us-east-1"
        assert result is builder  # Test method chaining

    def test_set_region_already_set(self):
        """Test that setting region twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_region("us-east-1")
        with pytest.raises(ValueError, match="set_region is already called"):
            builder.set_region("us-west-2")


class TestExtensionMethods:
    """Test extension related methods."""

    def test_set_extension_success(self):
        """Test setting extension."""
        builder = UnloadQueryBuilder()
        result = builder.set_extension("csv")
        assert builder.options["EXTENSION"] == "csv"
        assert result is builder  # Test method chaining

    def test_set_extension_already_set(self):
        """Test that setting extension twice raises error."""
        builder = UnloadQueryBuilder()
        builder.set_extension("csv")
        with pytest.raises(ValueError, match="set_extension is already called"):
            builder.set_extension("json")


class TestBuildMethod:
    """Test the build method."""

    def test_build_success(self):
        """Test successful build with all required fields."""
        builder = UnloadQueryBuilder()
        builder.add_select_template("SELECT * FROM table")
        builder.add_select_params({"date": "2024-01-01"})
        builder.add_to_path("s3://bucket/path/")
        builder.add_default_authorization()
        builder.set_format("CSV")

        result = builder.build()
        assert isinstance(result, UnloadQueryAndParams)
        assert result.select_template == "SELECT * FROM table"
        assert result.select_params == {"date": "2024-01-01"}
        assert result.to_path == "s3://bucket/path/"
        assert result.authorization == "IAM_ROLE default"
        assert result.options.FORMAT == "CSV"

    def test_build_missing_select_template(self):
        """Test that build fails when select_template is missing."""
        builder = UnloadQueryBuilder()
        builder.add_select_params({"date": "2024-01-01"})
        builder.add_to_path("s3://bucket/path/")
        builder.add_default_authorization()

        with pytest.raises(AssertionError):
            builder.build()

    def test_build_missing_to_path(self):
        """Test that build fails when to_path is missing."""
        builder = UnloadQueryBuilder()
        builder.add_select_template("SELECT * FROM table")
        builder.add_select_params({"date": "2024-01-01"})
        builder.add_default_authorization()

        with pytest.raises(AssertionError):
            builder.build()

    def test_build_missing_authorization(self):
        """Test that build fails when authorization is missing."""
        builder = UnloadQueryBuilder()
        builder.add_select_template("SELECT * FROM table")
        builder.add_select_params({"date": "2024-01-01"})
        builder.add_to_path("s3://bucket/path/")

        with pytest.raises(AssertionError):
            builder.build()


class TestUnloadQueryAndParams:
    """Test UnloadQueryAndParams class."""

    def test_query_property(self):
        """Test the query property."""
        from redshift_query_builder.core import UnloadQueryOption

        query_and_params = UnloadQueryAndParams(
            select_template="SELECT * FROM table",
            select_params={"date": "2024-01-01"},
            to_path="s3://bucket/path/",
            authorization="IAM_ROLE default",
            options=UnloadQueryOption(FORMAT="CSV"),
        )

        expected_query = "UNLOAD ('SELECT * FROM table') TO 's3://bucket/path/'\nIAM_ROLE default\nFORMAT AS CSV\nPARALLEL OFF"
        assert query_and_params.query == expected_query

    def test_params_property(self):
        """Test the params property."""
        from redshift_query_builder.core import UnloadQueryOption

        params = {"date": "2024-01-01"}
        query_and_params = UnloadQueryAndParams(
            select_template="SELECT * FROM table",
            select_params=params,
            to_path="s3://bucket/path/",
            authorization="IAM_ROLE default",
            options=UnloadQueryOption(),
        )

        assert query_and_params.params == params
