"""SQL client handling.

This includes mssqlStream and mssqlConnector.
"""
from __future__ import annotations

import gzip
import json
import datetime
import subprocess
import os
import time

from decimal import Decimal
from uuid import uuid4
from typing import Any, Iterable, Iterator
import re

import pendulum
import pyodbc
import sqlalchemy
import pathlib

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.batch import BaseBatcher, lazy_chunked_generator
import logging

# Get output directory from environment variables
job_root = os.environ.get("JOB_ROOT")
job_id = os.environ.get("JOB_ID", "")
LOCAL_OUTPUT_DIR = f"/home/hotglue/{job_id}/sync-output" if job_root else f"../.secrets"

# Get BCP path from environment
BCP_PATH = "/opt/mssql-tools/bin/bcp" if job_root else "bcp"

# BCP character delimiter (must match _build_bcp_command -t)
BCP_DELIMITER = "\x1F"


def read_last_line(path: str) -> str | None:
    with open(path, "rb") as f:
        try:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b"\n":
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        return f.readline().decode("utf-8", errors="replace")


class mssqlConnector(SQLConnector):
    """Connects to the mssql SQL source."""

    def __init__(
            self,
            config: dict | None = None,
            sqlalchemy_url: str | None = None
         ) -> None:
        """Class Default Init"""
        # If pyodbc given set pyodbc.pooling to False
        # This allows SQLA to manage to connection pool
        if config.get('driver_type') == 'pyodbc':
            pyodbc.pooling = False

        super().__init__(config, sqlalchemy_url)

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Return the SQLAlchemy URL string.

        Args:
            config: A dictionary of settings from the tap or target config.

        Returns:
            The URL as a string.
        """
        url_drivername = f"{config.get('dialect')}+{config.get('driver_type')}"
        
        config_url = URL.create(
            url_drivername,
            username=config.get('user'),
            password=config.get('password'),
            host=config.get('host'),
            database=config.get('database'),
        )

        if 'port' in config:
            config_url = config_url.set(port=config.get('port'))

        config_url = config_url.update_query_dict(
            config.get('sqlalchemy_url_query')
        )

        return config_url

    def create_engine(self) -> Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {
            f"{eng_prefix}url": self.sqlalchemy_url,
            f"{eng_prefix}echo": "False"
        }

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return sqlalchemy.engine_from_config(eng_config, prefix=eng_prefix)

    def to_jsonschema_type(
            self,
            from_type: str
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine],) -> None:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        if self.config.get('hd_jsonschema_types', False):
            return self.hd_to_jsonschema_type(from_type)
        else:
            return self.org_to_jsonschema_type(from_type)

    @staticmethod
    def org_to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """

        """
            Checks for the MSSQL type of NUMERIC
                if scale = 0 it is typed as a INTEGER
                if scale != 0 it is typed as NUMBER
        """
        if str(from_type).startswith("NUMERIC"):
            if str(from_type).endswith(", 0)"):
                from_type = "int"
            else:
                from_type = "number"

        if str(from_type) in ["MONEY", "SMALLMONEY"]:
            from_type = "number"

        # This is a MSSQL only DataType
        # SQLA does the converion from 0,1
        # to Python True, False
        if str(from_type) in ['BIT']:
            from_type = "bool"
        
        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def hd_to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine.
                If a TypeEngine is provided, it may be provided as a class or
                a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.
        """
        # This is taken from to_jsonschema_type() in typing.py
        if isinstance(from_type, str):
            sql_type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            sql_type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
            from_type, sqlalchemy.types.TypeEngine
        ):
            sql_type_name = from_type.__name__
        else:
            raise ValueError(
                "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
             )

        # Add in the length of the
        if sql_type_name in ['CHAR', 'NCHAR', 'VARCHAR', 'NVARCHAR']:
            maxLength: int = getattr(from_type, 'length')

            if getattr(from_type, 'length'):
                return {
                    "type": ["string"],
                    "maxLength": maxLength
                }

        if sql_type_name == 'TIME':
            return {
                "type": ["string"],
                "format": "time"
            }

        if sql_type_name == 'UNIQUEIDENTIFIER':
            return {
                "type": ["string"],
                "format": "uuid"
            }

        if sql_type_name == 'XML':
            return {
                "type": ["string"],
                "contentMediaType": "application/xml",
            }

        if sql_type_name in ['BINARY', 'IMAGE', 'VARBINARY']:
            maxLength: int = getattr(from_type, 'length')
            if getattr(from_type, 'length'):
                return {
                    "type": ["string"],
                    "contentEncoding": "base64",
                    "maxLength": maxLength
                }
            else:
                return {
                    "type": ["string"],
                    "contentEncoding": "base64",
                }

        # This is a MSSQL only DataType
        # SQLA does the converion from 0,1
        # to Python True, False
        if sql_type_name == 'BIT':
            return {"type": ["boolean"]}

        # This is a MSSQL only DataType
        if sql_type_name == 'TINYINT':
            return {
                "type": ["integer"],
                "minimum": 0,
                "maximum": 255
            }

        if sql_type_name == 'SMALLINT':
            return {
                "type": ["integer"],
                "minimum": -32768,
                "maximum": 32767
            }

        if sql_type_name == 'INTEGER':
            return {
                "type": ["integer"],
                "minimum": -2147483648,
                "maximum": 2147483647
            }

        if sql_type_name == 'BIGINT':
            return {
                "type": ["integer"],
                "minimum": -9223372036854775808,
                "maximum": 9223372036854775807
            }

        # Checks for the MSSQL type of NUMERIC and DECIMAL
        #     if scale = 0 it is typed as a INTEGER
        #     if scale != 0 it is typed as NUMBER
        if sql_type_name in ("NUMERIC", "DECIMAL"):
            precision: int = getattr(from_type, 'precision')
            scale: int = getattr(from_type, 'scale')
            if scale == 0:
                return {
                    "type": ["integer"],
                    "minimum": (-pow(10, precision))+1,
                    "maximum": (pow(10, precision))-1
                }
            else:
                maximum_as_number = str()
                minimum_as_number: str = '-'
                for i in range(precision):
                    if i == (precision-scale):
                        maximum_as_number += '.'
                    maximum_as_number += '9'
                minimum_as_number += maximum_as_number

                maximum_scientific_format: str = '9.'
                minimum_scientific_format: str = '-'
                for i in range(scale):
                    maximum_scientific_format += '9'
                maximum_scientific_format += f"e+{precision}"
                minimum_scientific_format += maximum_scientific_format

                if "e+" not in str(float(maximum_as_number)):
                    return {
                        "type": ["number"],
                        "minimum": float(minimum_as_number),
                        "maximum": float(maximum_as_number)
                    }
                else:
                    return {
                        "type": ["number"],
                        "minimum": float(minimum_scientific_format),
                        "maximum": float(maximum_scientific_format)
                    }

        # This is a MSSQL only DataType
        if sql_type_name == "SMALLMONEY":
            return {
                "type": ["number"],
                "minimum": -214748.3648,
                "maximum": 214748.3647
            }

        # This is a MSSQL only DataType
        # The min and max are getting truncated catalog
        if sql_type_name == "MONEY":
            return {
                "type": ["number"],
                "minimum": -922337203685477.5808,
                "maximum": 922337203685477.5807
            }

        if sql_type_name == "FLOAT":
            return {
                "type": ["number"],
                "minimum": -1.79e308,
                "maximum": 1.79e308
            }

        if sql_type_name == "REAL":
            return {
                "type": ["number"],
                "minimum": -3.40e38,
                "maximum": 3.40e38
            }

        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """

        return SQLConnector.to_sql_type(jsonschema_type)


class CustomJSONEncoder(json.JSONEncoder):
    """Custom class extends json.JSONEncoder"""

    # Override default() method
    def default(self, obj):

        # Datetime in ISO format
        if isinstance(obj, datetime.datetime):
            return pendulum.instance(obj).isoformat()

        # Date in ISO format
        if isinstance(obj, datetime.date):
            return obj.isoformat()

        # Time in ISO format truncated to the second to pass
        # json-schema validation
        if isinstance(obj, datetime.time):
            return obj.isoformat(timespec='seconds')

        # JSON Encoder doesn't know Decimals but it
        # does know float so we convert Decimal to float
        if isinstance(obj, Decimal):
            return float(obj)
        
        # Default behavior for all other types
        return super().default(obj)

class JSONLinesBatcher(BaseBatcher):
    """JSON Lines Record Batcher."""

    encoder_class = CustomJSONEncoder

    def get_batches(
        self,
        records: Iterator[dict],
    ) -> Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: The records to batch.

        Yields:
            A list of file paths (called a manifest).
        """
        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""

        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with self.batch_config.storage.fs(create=True) as fs:
                # TODO: Determine compression from config.
                with fs.open(filename, "wb") as f, gzip.GzipFile(
                    fileobj=f,
                    mode="wb",
                ) as gz:
                    gz.writelines(
                        (json.dumps(record, cls=self.encoder_class, default=str) + "\n").encode()
                        for record in chunk
                    )
                file_url = fs.geturl(filename)
            yield [file_url]


class mssqlStream(SQLStream):
    """Stream class for mssql streams."""

    connector_class = mssqlConnector

    def _build_sql_query_string(
        self,
        selected_columns: list[str],
        context: dict | None = None,
    ) -> str:
        """Build SQL query string for BCP.

        Args:
            selected_columns: List of column names to select.
            context: Stream partition or context dictionary.

        Returns:
            SQL query string.
        """
        # Get table metadata to extract schema and table name
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_columns,
        )

        # Extract database, schema, and table name
        database = self.config.get('database')
        
        # Get schema - try multiple ways to access it
        schema = 'dbo'  # Default schema
        if hasattr(table, 'schema') and table.schema:
            schema = table.schema
        elif hasattr(table, 'key') and '.' in table.key:
            # Try to extract from table key if it's in format schema.table
            parts = table.key.split('.')
            if len(parts) > 1:
                schema = parts[0]
        
        table_name = table.name

        # Build column list with proper escaping
        column_list = ', '.join([f'[{col}]' for col in selected_columns])

        # Build FROM clause (bracket-escape all identifiers for spaces/special chars/reserved words)
        # Handle schema - use 'dbo' as default if schema is None or empty
        if schema and schema != '':
            from_clause = f'[{database}].[{schema}].[{table_name}]'
        else:
            from_clause = f'[{database}].[dbo].[{table_name}]'

        # Build SELECT clause (with TOP if needed)
        # Check for max_records config first, then ABORT_AT_RECORD_COUNT
        max_records = self.config.get('max_records')
        if max_records is not None:
            select_clause = f'SELECT TOP {max_records} {column_list}'
        elif self.ABORT_AT_RECORD_COUNT is not None:
            limit_val = self.ABORT_AT_RECORD_COUNT + 1
            select_clause = f'SELECT TOP {limit_val} {column_list}'
        else:
            select_clause = f'SELECT {column_list}'

        # Start building query
        query_parts = [select_clause, f'FROM {from_clause}']

        # Add WHERE clause for replication key if applicable
        where_clause = None
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            
            # Get the starting value
            if replication_key_col.type.python_type in (
                datetime.datetime,
                datetime.date
            ):
                start_val = self.get_starting_timestamp(context)
                # Apply redundancy to pull some days before the start_val
                if start_val:
                    lookback_window_days = self.config.get("lookback_window_days", 0)
                    self.logger.info(
                        f"Debug - lookback_window_days: {lookback_window_days} days."
                    )
                    if isinstance(lookback_window_days, int) and lookback_window_days > 0:
                        self.logger.info(
                            f"Applying replication key redundancy of {lookback_window_days} days to the start_val {start_val}."
                        )
                        start_val -= datetime.timedelta(days=lookback_window_days)
                        self.logger.info(
                            f"Debug - FINAL start_val: {start_val}"
                        )
                    else:
                        self.logger.info(
                            "No redundancy was applied"
                        )
            else:
                start_val = self.get_starting_replication_key_value(context)

            if start_val:
                # Format the value appropriately for SQL
                if isinstance(start_val, (datetime.datetime, datetime.date)):
                    # Format datetime/date values for SQL Server
                    if isinstance(start_val, datetime.datetime):
                        # ISO format with timezone for datetimeoffset comparison
                        iso_str = start_val.isoformat()
                        # Only append UTC offset when there is no offset (Z or ±HH:MM)
                        has_offset = "Z" in iso_str or bool(re.search(r"[+-]\d{2}:", iso_str))
                        if not has_offset:
                            iso_str += "+00:00"
                        escaped = iso_str.replace("'", "''")
                        val_str = f"CAST('{escaped}' AS datetimeoffset(6))"
                    else:
                        val_str = start_val.strftime("'%Y-%m-%d'")
                elif isinstance(start_val, str):
                    # Escape single quotes in SQL by doubling them
                    escaped_val = start_val.replace("'", "''")
                    val_str = f"'{escaped_val}'"
                else:
                    val_str = str(start_val)
                
                where_clause = f'WHERE [{self.replication_key}] > {val_str}'
                query_parts.append(where_clause)

            # Add ORDER BY clause
            query_parts.append(f'ORDER BY [{self.replication_key}]')

        query = ' '.join(query_parts)

        return query

    def _build_bcp_command(
        self,
        sql_query: str,
        output_file: str,
    ) -> list[str]:
        """Build BCP command arguments.

        Args:
            sql_query: SQL query string to execute.

        Returns:
            List of command arguments for subprocess.
        """
        config = self.config
        host = config.get('host')
        port = config.get('port', '1433')
        database = config.get('database')
        user = config.get('user')
        password = config.get('password')

        # Build server string
        server = f'tcp:{host},{port}'

        # Build BCP command
        # Note: The delimiter \x1F needs to be passed properly
        # In Python, we'll use the actual character
        delimiter = '\x1F'

        # BCP expects the query as a quoted string argument
        # The query should be wrapped in quotes for the command
        cmd = [
            BCP_PATH,
            sql_query,  # The query string will be passed as-is, subprocess handles quoting
            'queryout',
            output_file,  # Direct path to CSV file
            '-S', server,
            '-d', database,
            '-U', user,
            '-P', password,
            '-c',
            '-t', delimiter,
            '-b', '1000000000',  # Large batch size to avoid summary messages
        ]
        # ODBC Driver 18 requires -u to trust server cert when TrustServerCertificate=yes in config
        url_query = config.get('sqlalchemy_url_query') or {}
        if (url_query.get('TrustServerCertificate') or '').lower() == 'yes':
            cmd.append('-u')

        return cmd

    def _get_last_replication_key_value_from_csv(
        self,
        csv_file: str,
        selected_column_names: list[str],
    ) -> Any | None:
        """Read the last row from the BCP CSV and return the replication_key value.

        BCP output is ordered by replication_key, so the last row has the max value.
        No header; columns are in selected_column_names order with BCP_DELIMITER.
        Returns a JSON-compatible value for state bookmarking.

        Uses tail-only reads (seek from end + read backwards in blocks until a newline
        is found) so it is O(1) in file size and safe for arbitrarily large CSVs.
        """
        if not self.replication_key or self.replication_key not in selected_column_names:
            return None
        try:
            last_line = read_last_line(csv_file)
            if not last_line:
                return None
            parts = last_line.split(BCP_DELIMITER)
            rk_index = selected_column_names.index(self.replication_key)
            if rk_index >= len(parts):
                return None
            raw = parts[rk_index].strip()
            if raw == "":
                return None
            prop = (self.schema.get("properties") or {}).get(self.replication_key) or {}
            fmt = prop.get("format", "")
            if fmt == "date-time":
                return pendulum.parse(raw).isoformat()
            if fmt == "date":
                return pendulum.parse(raw).date().isoformat()
            return raw
        except Exception as e:
            self.logger.warning(
                f"Could not read last replication key value from CSV: {e}",
                exc_info=True,
            )
            return None

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be
        sorted by the incremental key. If the stream also has an available
        starting bookmark, the records will be filtered for values greater
        than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically
                from this data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the
                stream does not support partitioning.
        """
        if context:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning.",
            )

        # Get selected column names
        selected_column_names = list(self.get_selected_schema()["properties"].keys())
        
        # Build SQL query string
        sql_query = self._build_sql_query_string(selected_column_names, context)

        # Create output directory if it doesn't exist
        os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
        
        # Create output file name: {stream}.csv
        stream_name = self.name
        csv_file = os.path.join(LOCAL_OUTPUT_DIR, f"{stream_name}.csv")

        # Initialize variables for BCP output parsing
        bcp_stdout = ""
        bcp_stderr = ""
        bcp_returncode = 0
        
        try:
            # Build BCP command with CSV file path
            bcp_cmd = self._build_bcp_command(sql_query, csv_file)
            
            # Log BCP command (hide password for security)
            bcp_cmd_safe = bcp_cmd.copy()
            if '-P' in bcp_cmd_safe:
                pwd_idx = bcp_cmd_safe.index('-P')
                if pwd_idx + 1 < len(bcp_cmd_safe):
                    bcp_cmd_safe[pwd_idx + 1] = '***'
            self.logger.info(f'Executing BCP: {" ".join(bcp_cmd_safe)}')
            self.logger.info(f'BCP SQL query: {sql_query}')
            
            # Time BCP export
            bcp_start_time = time.time()
            self.logger.info(f'Starting BCP export to {csv_file}')
            
            # BCP writes directly to CSV file via queryout parameter
            bcp_process = subprocess.Popen(
                bcp_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
            )
            self.logger.info(f'BCP process started (PID: {bcp_process.pid})')
            
            # Wait for BCP and read stdout/stderr (communicate() avoids deadlock if pipes fill)
            self.logger.info(f'Waiting for BCP to complete...')
            bcp_stdout_bytes, bcp_stderr_bytes = bcp_process.communicate()
            bcp_returncode = bcp_process.returncode
            self.logger.info(f'BCP process completed with return code: {bcp_returncode}')
            
            # Decode BCP output (summary "X rows copied" may be in either stream)
            bcp_stdout = bcp_stdout_bytes.decode('utf-8', errors='replace')
            bcp_stderr = bcp_stderr_bytes.decode('utf-8', errors='replace')
            self.logger.info(f'BCP stdout ({len(bcp_stdout)} chars), stderr ({len(bcp_stderr)} chars)')
            
            # Parse record count from "X rows copied." (in stdout per BCP; fallback to stderr)
            record_count = 0

            def parse_rows_copied(text: str):
                """Return last 'N rows copied' value found in text, or 0."""
                count = 0
                for line in text.strip().split('\n'):
                    line = line.strip()
                    if line:
                        match = re.search(r'(\d+)\s+rows?\s+copied', line, re.IGNORECASE)
                        if match:
                            count = int(match.group(1))
                return count

            if bcp_stdout:
                self.logger.info(f'BCP stdout content ({len(bcp_stdout)} bytes):\n{bcp_stdout[:500]}...')
            record_count = parse_rows_copied(bcp_stdout)
            if record_count == 0 and bcp_stderr:
                record_count = parse_rows_copied(bcp_stderr)
                if record_count:
                    self.logger.info(f'Parsed record count from BCP stderr: {record_count:,} rows')
            if record_count:
                self.logger.info(f'Parsed record count from BCP output: {record_count:,} rows')
            if record_count == 0:
                self.logger.warning(
                    'Could not parse "X rows copied" from BCP output. '
                    f'stdout last 500 chars: {bcp_stdout[-500:]!r}; stderr: {bcp_stderr[-500:]!r}'
                )
            
            # Check for errors
            if bcp_returncode != 0:
                error_msg = f'BCP command failed with return code {bcp_returncode}'
                if bcp_stderr:
                    error_msg += f': {bcp_stderr.strip()[-500:]}'
                if bcp_stdout and not bcp_stderr:
                    error_msg += f': {bcp_stdout.strip()[-500:]}'
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)
            
            bcp_end_time = time.time()
            bcp_duration = bcp_end_time - bcp_start_time
            
            # Get CSV file size
            csv_size = os.path.getsize(csv_file)
            csv_size_mb = csv_size / (1024 * 1024)
            
            self.logger.info(
                f'BCP export completed in {bcp_duration:.2f} seconds. '
                f'CSV file: {csv_file} ({csv_size_mb:.2f} MB)'
            )
            
            self.update_job_metrics(self.name, record_count, LOCAL_OUTPUT_DIR)
                        
            self.logger.info(f'Emitted metric for {record_count:,} records')

            # Use last record from CSV (ordered by replication_key) to advance bookmark
            if record_count > 0 and self.replication_key:
                last_rk_value = self._get_last_replication_key_value_from_csv(
                    csv_file, selected_column_names
                )
                if last_rk_value is not None:
                    dummy_record = {self.replication_key: last_rk_value}
                    self.logger.info(
                        f"Yielding dummy record from last CSV row to advance bookmark "
                        f"{self.replication_key!r} = {last_rk_value!r}"
                    )
                    yield dummy_record
            
        finally:
            # Keep the CSV file - don't delete it
            # The file is at csv_file location and contains all the data
            pass
        
    def update_job_metrics(self, stream_name: str, record_count: int, output_dir: str):
        """
        Update metrics for a running job by tracking record counts per stream.

        This function maintains a JSON file that keeps track of the number of records
        processed for each stream during a job execution. The metrics are stored in
        a 'job_metrics.json' file in the specified folder path.

        Args:
            stream_name (str): The name of the stream being processed
            record_count (int): Number of records processed in the current batch
            output_dir (str): Folder path to store the job metrics

        Examples:
            >>> update_job_metrics("customers", 1000, "job_123")
            # Updates job_metrics.json with:
            # {
            #   "recordCount": {
            #     "customers": 1000
            #   }
            # }
        """
        job_metrics_path = os.path.expanduser(os.path.join(output_dir, "job_metrics.json"))

        if not os.path.isfile(job_metrics_path):
            pathlib.Path(job_metrics_path).touch()

        with open(job_metrics_path, "r+") as f:
            content = dict()

            try:
                content = json.loads(f.read())
            except:
                pass

            if not content.get("recordCount"):
                content["recordCount"] = dict()

            content["recordCount"][stream_name] = (
                content["recordCount"].get(stream_name, 0) + record_count
            )

            f.seek(0)
            self.logger.info(f"Updating job metrics for {stream_name} with {record_count} records")
            f.write(json.dumps(content))
            f.truncate()
