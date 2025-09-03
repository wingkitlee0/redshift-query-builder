import re
import weakref
from dataclasses import dataclass
from functools import wraps
from typing import Any, List, Literal, Optional, Self

from pydantic import BaseModel, Field, field_validator, model_validator

# Global weakref dictionary to track called functions per object instance
# Automatically cleans up when objects are garbage collected
CALLED_FUNCTIONS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()

# Validation rules for UNLOAD options
FORMAT_CONFLICTS = {
    "CSV": ["ESCAPE", "FIXEDWIDTH", "ADDQUOTES"],
    "PARQUET": [
        "DELIMITER",
        "FIXEDWIDTH",
        "ADDQUOTES",
        "ESCAPE",
        "NULL",
        "HEADER",
        "COMPRESSION",
    ],
    "JSON": ["DELIMITER", "FIXEDWIDTH", "ADDQUOTES", "ESCAPE", "NULL"],
}

# Conflict rules for UNLOAD options
CONFLICT_RULES = [
    {"CLEANPATH", "ALLOWOVERWRITE"},  # under CLEANPATH
    {"FIXEDWIDTH", "DELIMITER"},  # under FIXEDWIDTH
    {"FIXEDWIDTH", "HEADER"},  # under FIXEDWIDTH
]


class PartitionByOption(BaseModel):
    """Nested model for PARTITION_BY option."""

    columns: List[str] = Field(description="List of columns to partition by")
    include: bool = Field(
        default=False, description="Whether to include partition columns in output"
    )

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, v: List[str]) -> List[str]:
        """Validate that columns list is not empty."""
        if not v:
            raise ValueError("PARTITION_BY columns list cannot be empty")
        if not all(isinstance(col, str) and col.strip() for col in v):
            raise ValueError("All PARTITION_BY columns must be non-empty strings")
        return v


class ManifestOption(BaseModel):
    """Nested model for MANIFEST option."""

    enable: bool = Field(default=False, description="Whether to enable manifest")
    verbose: bool = Field(
        default=False, description="Whether to include verbose manifest"
    )


def call_once(func=None):
    """Decorator to ensure a function is only called once.

    Supports both forms:
    - @call_once
    - @call_once()
    """

    def decorator(f):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            if self not in CALLED_FUNCTIONS:
                CALLED_FUNCTIONS[self] = set()
            if f.__name__ in CALLED_FUNCTIONS[self]:
                raise ValueError(f"{f.__name__} is already called")
            CALLED_FUNCTIONS[self].add(f.__name__)
            return f(self, *args, **kwargs)

        return wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)


class UnloadQueryOption(BaseModel):
    """A Pydantic model that represents an unload query option."""

    model_config = {"extra": "forbid"}

    # String options (default to None)
    FORMAT: Optional[Literal["CSV", "PARQUET", "JSON"]] = Field(
        default=None, description="Output format: CSV, PARQUET, or JSON"
    )
    DELIMITER: Optional[str] = Field(
        default=None,
        description="Field delimiter character",
        min_length=1,
        max_length=1,
    )
    FIXEDWIDTH: Optional[str] = Field(
        default=None, description="Fixed width specification", min_length=3
    )
    COMPRESSION: Optional[Literal["GZIP", "BZIP2", "ZSTD"]] = Field(
        default=None, description="Compression method: GZIP, BZIP2, or ZSTD"
    )
    NULL: Optional[str] = Field(default=None, description="NULL value representation")
    MAXFILESIZE: Optional[str] = Field(default=None, description="Maximum file size")
    ROWGROUPSIZE: Optional[str] = Field(
        default=None, description="Row group size for Parquet"
    )
    REGION: Optional[str] = Field(default=None, description="AWS region", min_length=5)
    EXTENSION: Optional[str] = Field(default=None, description="File extension")

    # Boolean flags (default to False)
    HEADER: bool = Field(default=False, description="Whether to include header row")
    ADDQUOTES: bool = Field(
        default=False, description="Whether to add quotes around fields"
    )
    ESCAPE: bool = Field(
        default=False, description="Whether to escape special characters"
    )
    ALLOWOVERWRITE: bool = Field(
        default=False, description="Whether to allow overwriting existing files"
    )
    CLEANPATH: bool = Field(
        default=False, description="Whether to clean the output path"
    )
    PARALLEL: bool = Field(
        default=False, description="Whether to enable parallel processing"
    )
    ENCRYPTED: bool = Field(default=False, description="Whether to encrypt output")

    # Complex nested options
    PARTITION_BY: Optional[PartitionByOption] = Field(
        default=None, description="Partition configuration"
    )
    MANIFEST: Optional[ManifestOption] = Field(
        default=None, description="Manifest configuration"
    )

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    @field_validator("REGION")
    @classmethod
    def validate_region(cls, v: Optional[str]) -> Optional[str]:
        """Validate AWS region format."""
        if v is None:
            return v

        if not re.match(r"^[a-z]+-[a-z]+-\d+$", v):
            raise ValueError(
                "REGION must be a valid AWS region format (e.g., us-east-1)"
            )
        return v

    @field_validator("FIXEDWIDTH")
    @classmethod
    def validate_fixedwidth(cls, v: Optional[str]) -> Optional[str]:
        """Validate FIXEDWIDTH format: 'colID1:colWidth1,colID2:colWidth2, ...'."""
        if v is None:
            return v

        # Pattern: numeric_column_id:width,numeric_column_id:width,...
        pattern = r"^\d+:\d+(?:,\d+:\d+)*$"
        if not re.match(pattern, v):
            raise ValueError(
                "FIXEDWIDTH must be in format 'colID1:colWidth1,colID2:colWidth2, ...' (e.g., '0:3,1:100,2:30')"
            )
        return v

    @field_validator("EXTENSION")
    @classmethod
    def validate_extension(cls, v: Optional[str]) -> Optional[str]:
        """Validate file extension format."""
        if v is None:
            return v
        if v.startswith("."):
            raise ValueError("EXTENSION should not start with a dot")
        return v

    @model_validator(mode="after")
    def validate_format_conflicts(self) -> "UnloadQueryOption":
        """Validate format conflicts after all fields are set."""
        if self.FORMAT and self.FORMAT in FORMAT_CONFLICTS:
            conflicting_options = FORMAT_CONFLICTS[self.FORMAT]
            conflicts = []

            # Check for conflicts using the dictionary
            for option in conflicting_options:
                if self[option]:
                    conflicts.append(option)

            if conflicts:
                raise ValueError(
                    f"{self.FORMAT} cannot be used with {', '.join(conflicts)}"
                )

        return self

    @model_validator(mode="after")
    def validate_conflict_rules(self) -> "UnloadQueryOption":
        """Validate conflict rules between different UNLOAD options."""
        # Check each conflict rule
        for conflict_set in CONFLICT_RULES:
            enabled_options = []

            # An option is enabled if it is not None or False
            enabled_options = list(filter(lambda x: self[x], conflict_set))

            # If more than one option is enabled in a conflict set, raise error
            if len(enabled_options) > 1:
                raise ValueError(
                    f"Conflicting options cannot be used together: {', '.join(sorted(enabled_options))}"
                )

        return self

    @model_validator(mode="before")
    @classmethod
    def validate_data_before_creation(cls, data: Any) -> Any:
        """Validate and transform data before model creation."""
        if isinstance(data, dict):
            # Example: Normalize compression values
            if "COMPRESSION" in data and isinstance(data["COMPRESSION"], str):
                data["COMPRESSION"] = data["COMPRESSION"].upper()

            # Example: Normalize format values
            if "FORMAT" in data and isinstance(data["FORMAT"], str):
                data["FORMAT"] = data["FORMAT"].upper()

        return data

    def to_options_string(self) -> str:
        """Return a string of options in the correct order according to AWS Redshift UNLOAD documentation."""
        options_list = []

        # 1. PARTITION BY (comes first)
        if self.PARTITION_BY:
            partition_by_columns = ",".join(self.PARTITION_BY.columns)
            partition_by_include = "INCLUDE" if self.PARTITION_BY.include else ""
            options_list.append(
                f"PARTITION BY ({partition_by_columns}) {partition_by_include}".strip()
            )

        # 2. FORMAT AS
        if self.FORMAT:
            options_list.append(f"FORMAT AS {self.FORMAT}")

        # 3. File format options
        if self.DELIMITER:
            options_list.append(f"DELIMITER AS '{self.DELIMITER}'")
        if self.FIXEDWIDTH:
            options_list.append(f"FIXEDWIDTH '{self.FIXEDWIDTH}'")
        if self.HEADER:
            options_list.append("HEADER")
        if self.ADDQUOTES:
            options_list.append("ADDQUOTES")
        if self.NULL:
            options_list.append(f"NULL AS '{self.NULL}'")
        if self.ESCAPE:
            options_list.append("ESCAPE")

        # 4. Compression
        if self.COMPRESSION:
            options_list.append(self.COMPRESSION)

        # 5. Security and performance
        if self.ENCRYPTED:
            options_list.append("ENCRYPTED")
        if self.ALLOWOVERWRITE:
            options_list.append("ALLOWOVERWRITE")
        if self.CLEANPATH:
            options_list.append("CLEANPATH")
        if self.PARALLEL:
            options_list.append("PARALLEL ON")
        else:
            options_list.append("PARALLEL OFF")

        # 6. File management
        if self.MANIFEST and self.MANIFEST.enable:
            manifest_option = "MANIFEST"
            if self.MANIFEST.verbose:
                manifest_option += " VERBOSE"
            options_list.append(manifest_option)

        if self.MAXFILESIZE:
            options_list.append(f"MAXFILESIZE {self.MAXFILESIZE}")
        if self.ROWGROUPSIZE:
            options_list.append(f"ROWGROUPSIZE {self.ROWGROUPSIZE}")
        if self.REGION:
            options_list.append(f"REGION '{self.REGION}'")
        if self.EXTENSION:
            options_list.append(f"EXTENSION '{self.EXTENSION}'")

        return "\n".join(options_list)


@dataclass
class UnloadQueryAndParams:
    """A class that represents an unload query.

    Usage:
    >>> query_and_params = ...
    >>> cursor.execute(
    ...    query_and_params.query,
    ...    query_and_params.params,
    ... )
    """

    select_template: str
    select_params: dict[str, str]
    to_path: str
    authorization: str
    options: UnloadQueryOption

    @property
    def query(self) -> str:
        """Return a parameterized query string."""
        return f"UNLOAD ('{self.select_template}') TO '{self.to_path}'\n{self.authorization}\n{self.options.to_options_string()}"

    @property
    def params(self) -> dict[str, str]:
        """Return a dictionary of parameters."""
        return self.select_params


class UnloadQueryBuilder:
    """A builder for UNLOAD queries

    Validations are performed after all fields are set when build() is called.
    """

    def __init__(self):
        """Initialize the builder with all attributes to None."""
        self.select_template = None
        self.select_params = {}  # it's optional
        self.to_path = None
        self.authorization = None
        self.options: dict[str, Any] = {}  # it's optional

    @call_once
    def add_select_template(self, select_template: str) -> Self:
        self.select_template = select_template
        return self

    @call_once
    def add_select_params(self, select_params: dict[str, str]) -> Self:
        self.select_params = select_params
        return self

    @call_once
    def add_to_path(self, to_path: str) -> Self:
        """Set the target S3 path"""

        self.to_path = to_path
        return self

    def add_iam_role_authorization(
        self, account_id_and_roles: tuple[str, str] | list[tuple[str, str]]
    ) -> Self:
        if self.authorization:
            raise ValueError("authorization is already set")

        if isinstance(account_id_and_roles, tuple):
            account_id_and_roles = [account_id_and_roles]

        self.authorization = "IAM_ROLE " + ", ".join(
            f"arn:aws:iam::{account_id}:role/{role}"
            for account_id, role in account_id_and_roles
        )
        return self

    def add_default_authorization(self) -> Self:
        if self.authorization:
            raise ValueError("authorization is already set")

        self.authorization = "IAM_ROLE default"
        return self

    @call_once
    def set_format(self, format: str) -> Self:
        self.options["FORMAT"] = format
        return self

    @call_once
    def set_partition_by(self, columns: str | list[str], include: bool = False) -> Self:
        self.options["PARTITION_BY"] = {
            "columns": [columns] if isinstance(columns, str) else columns,
            "include": include,
        }
        return self

    @call_once
    def set_manifest(self, enable: bool = True, verbose: bool = False) -> Self:
        self.options["MANIFEST"] = {
            "enable": enable,
            "verbose": verbose,
        }
        return self

    @call_once
    def set_header(self, enable: bool = True) -> Self:
        self.options["HEADER"] = enable
        return self

    @call_once
    def set_compression(self, compression: str) -> Self:
        valid_methods = ["GZIP", "BZIP2", "ZSTD"]
        if compression not in valid_methods:
            raise ValueError(f"Invalid compression: {compression}")
        self.options["COMPRESSION"] = compression
        return self

    @call_once
    def set_delimiter(self, delimiter: str) -> Self:
        self.options["DELIMITER"] = delimiter
        return self

    @call_once
    def set_fixedwidth(self, fixedwidth_spec: str) -> Self:
        self.options["FIXEDWIDTH"] = fixedwidth_spec
        return self

    @call_once
    def set_encrypted(self, enable: bool = True) -> Self:
        self.options["ENCRYPTED"] = enable
        return self

    @call_once
    def set_addquotes(self, enable: bool = True) -> Self:
        self.options["ADDQUOTES"] = enable
        return self

    @call_once
    def set_null(self, null_string: str) -> Self:
        self.options["NULL"] = null_string
        return self

    @call_once
    def set_escape(self, enable: bool = True) -> Self:
        self.options["ESCAPE"] = enable
        return self

    @call_once
    def set_allowoverwrite(self, enable: bool = True) -> Self:
        self.options["ALLOWOVERWRITE"] = enable
        return self

    @call_once
    def set_cleanpath(self, enable: bool = True) -> Self:
        self.options["CLEANPATH"] = enable
        return self

    @call_once
    def set_parallel(self, parallel: bool | str) -> Self:
        if isinstance(parallel, str):
            if parallel.upper() in ["ON", "TRUE"]:
                self.options["PARALLEL"] = "ON"
            elif parallel.upper() in ["OFF", "FALSE"]:
                self.options["PARALLEL"] = "OFF"
            else:
                raise ValueError(f"Invalid parallel value: {parallel}")
        else:
            self.options["PARALLEL"] = "ON" if parallel else "OFF"

        return self

    @call_once
    def set_maxfilesize(self, maxfilesize: str) -> Self:
        self.options["MAXFILESIZE"] = maxfilesize
        return self

    @call_once
    def set_rowgroupsize(self, rowgroupsize: str) -> Self:
        self.options["ROWGROUPSIZE"] = rowgroupsize
        return self

    @call_once
    def set_region(self, region: str) -> Self:
        self.options["REGION"] = region
        return self

    @call_once
    def set_extension(self, extension: str) -> Self:
        self.options["EXTENSION"] = extension
        return self

    def build(self) -> UnloadQueryAndParams:
        """Build an unload query and params."""

        assert self.select_template is not None, "add_select_template is not called"
        assert self.to_path is not None, "add_to_path is not called"
        assert self.authorization is not None, "add_default_authorization is not called"

        return UnloadQueryAndParams(
            select_template=self.select_template,
            select_params=self.select_params,
            to_path=self.to_path,
            authorization=self.authorization,
            options=UnloadQueryOption.model_validate(self.options),
        )
