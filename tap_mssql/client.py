"""SQL client handling.

This includes mssqlStream and mssqlConnector.
"""
from __future__ import annotations

import gzip
import json
import datetime
import subprocess
import tempfile
import os
import csv
import time

from base64 import b64encode
from decimal import Decimal
from uuid import uuid4
from typing import Any, Iterable, Iterator
import re

import pendulum
import pyodbc
import sqlalchemy

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.batch import BaseBatcher, lazy_chunked_generator
from singer_sdk.metrics import Counter

# Get output directory from environment variables
job_root = os.environ.get("JOB_ROOT")
job_id = os.environ.get("JOB_ID", "")
LOCAL_OUTPUT_DIR = f"/home/hotglue/{job_id}/sync-output" if job_root else f"../.secrets"

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
        url_drivername = f"{config.get('dialect', 'mssql')}+{config.get('driver_type', 'pyodbc')}"
        
        config_url = URL.create(
            url_drivername,
            username=config.get('user'),
            password=config.get('password'),
            host=config.get('host'),
            database=config.get('database'),
        )

        if 'port' in config:
            config_url = config_url.set(port=config.get('port'))

        # Add the driver specification and SSL settings to the URL query parameters
        driver_query = {
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",  # Add this to trust the server certificate
            "Encrypt": "yes"                  # Ensure encryption is enabled
        }
        
        if 'sqlalchemy_url_query' in config:
            driver_query.update(config.get('sqlalchemy_url_query'))
        
        config_url = config_url.update_query_dict(driver_query)

        return str(config_url)

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
    
    @staticmethod
    def get_fully_qualified_name(
        table_name: str | None = None,
        schema_name: str | None = None,
        db_name: str | None = None,
        delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            db_name: The name of the database. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Raises:
            ValueError: If all 3 name parts not supplied.

        Returns:
            The fully qualified name as a string.
        """
        parts = []

        if table_name:
            parts.append(table_name)

        if not parts:
            raise ValueError(
                "Could not generate fully qualified name: "
                + ":".join(
                    [
                        table_name or "(unknown-table-name)",
                    ],
                ),
            )

        return table_name



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

    def _emit_record_count_metric(self, record_count: int) -> None:
        """Utility function to create and emit a record count metric.
        
        Args:
            record_count: The number of records to report in the metric.
        """
        metric = Counter(
            metric="record_count",
            value=record_count,
            tags={"stream": self.name},
        )
        # Write the metric message directly using the tap's write method
        try:
            if hasattr(self, 'tap') and self.tap and hasattr(self.tap, '_write_message'):
                self.tap._write_message(metric)
            elif hasattr(self, '_write_message'):
                self._write_message(metric)
            else:
                # Fallback: construct and print the metric message manually
                metric_dict = {
                    "type": "METRIC",
                    "metric": "record_count",
                    "value": record_count,
                    "tags": {"stream": self.name}
                }
                print(json.dumps(metric_dict), flush=True)
        except Exception as e:
            # If metric emission fails, log it
            self.logger.warning(f'Failed to emit metric: {e}')

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

        # Build FROM clause
        # Handle schema - use 'dbo' as default if schema is None or empty
        if schema and schema != '':
            from_clause = f'[{database}].{schema}.[{table_name}]'
        else:
            from_clause = f'[{database}].dbo.[{table_name}]'

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
            else:
                start_val = self.get_starting_replication_key_value(context)

            if start_val:
                # Format the value appropriately for SQL
                if isinstance(start_val, (datetime.datetime, datetime.date)):
                    # Format datetime/date values for SQL Server
                    if isinstance(start_val, datetime.datetime):
                        val_str = start_val.strftime("'%Y-%m-%d %H:%M:%S'")
                    else:
                        val_str = start_val.strftime("'%Y-%m-%d'")
                elif isinstance(start_val, str):
                    # Escape single quotes in SQL by doubling them
                    escaped_val = start_val.replace("'", "''")
                    val_str = f"'{escaped_val}'"
                else:
                    val_str = str(start_val)
                
                where_clause = f'WHERE [{self.replication_key}] >= {val_str}'
                query_parts.append(where_clause)

            # Add ORDER BY clause
            query_parts.append(f'ORDER BY [{self.replication_key}]')

        query = ' '.join(query_parts)

        return query

    def _build_bcp_command(
        self,
        sql_query: str,
        output_file: str | None = None,
    ) -> list[str]:
        """Build BCP command arguments.

        Args:
            sql_query: SQL query string to execute.
            output_file: Path to output CSV file. If None, outputs to stdout.

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
            'bcp',
            sql_query,  # The query string will be passed as-is, subprocess handles quoting
            'queryout',
            output_file if output_file else '/dev/stdout',
            '-S', server,
            '-d', database,
            '-U', user,
            '-P', password,
            '-c',
            '-t', delimiter,
        ]

        return cmd

    def _parse_bcp_csv(
        self,
        csv_file: str,
        column_names: list[str],
    ) -> Iterator[dict[str, Any]]:
        """Parse BCP CSV output file.

        Args:
            csv_file: Path to CSV file.
            column_names: List of column names in order.

        Yields:
            Dictionary records.
        """
        delimiter = '\x1F'

        try:
            # Check if file exists and has content
            if not os.path.exists(csv_file):
                self.logger.warning(f'BCP output file does not exist: {csv_file}')
                return
            
            if os.path.getsize(csv_file) == 0:
                self.logger.info(f'BCP output file is empty: {csv_file}')
                return

            with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
                # Use csv.reader with custom delimiter
                reader = csv.reader(f, delimiter=delimiter)
                
                for row_num, row in enumerate(reader, start=1):
                    # Skip empty rows
                    if not row or all(not cell.strip() for cell in row):
                        continue
                    
                    # Create dict from row values and column names
                    if len(row) != len(column_names):
                        # Skip rows that don't match expected column count
                        self.logger.warning(
                            f'Row {row_num} has {len(row)} columns, expected {len(column_names)}. Skipping.'
                        )
                        continue
                    
                    # Convert empty strings to None for consistency with SQLAlchemy behavior
                    record = {
                        col: (val if val != '' else None)
                        for col, val in zip(column_names, row)
                    }
                    yield record
        except Exception as e:
            self.logger.error(f'Error parsing BCP CSV file: {e}')
            raise

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        # We change the name to record so when the change breaking
        # change from row to record is done in SDK 1.0 the edits
        # to accomidate the swithc will be two
        record: dict = row

        # Get the Stream Properties Dictornary from the Schema
        properties: dict = self.schema.get('properties')

        for key, value in record.items():
            if value is not None:
                # Get the Item/Column property
                property_schema: dict = properties.get(key)
                # Date in ISO format
                if isinstance(value, datetime.date):
                    record.update({key: value.isoformat()})
                # Encode base64 binary fields in the record
                if property_schema.get('contentEncoding') == 'base64':
                    record.update({key: b64encode(value).decode()})

        return record

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
        
        # Create output file name: {stream}.csv.gz
        stream_name = self.name
        compressed_file = os.path.join(LOCAL_OUTPUT_DIR, f"{stream_name}.csv.gz")

        try:
            # Build BCP command to output to stdout
            bcp_cmd = self._build_bcp_command(sql_query, output_file=None)
            
            # Log BCP command (hide password for security)
            bcp_cmd_safe = bcp_cmd.copy()
            if '-P' in bcp_cmd_safe:
                pwd_idx = bcp_cmd_safe.index('-P')
                if pwd_idx + 1 < len(bcp_cmd_safe):
                    bcp_cmd_safe[pwd_idx + 1] = '***'
            self.logger.info(f'Executing BCP piped to gzip: {" ".join(bcp_cmd_safe)} | gzip > {compressed_file}')
            self.logger.info(f'BCP SQL query: {sql_query}')
            
            # Time BCP and compression
            bcp_start_time = time.time()
            
            # Open gzip file for writing
            with open(compressed_file, 'wb') as gz_file:
                # Start BCP process with stdout piped and stderr piped
                bcp_process = subprocess.Popen(
                    bcp_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=False,  # Keep binary mode - stdout goes to gzip
                )
                
                # Start gzip process to compress BCP output
                gzip_process = subprocess.Popen(
                    ['gzip'],
                    stdin=bcp_process.stdout,
                    stdout=gz_file,
                    stderr=subprocess.PIPE,
                )
                
                # Close BCP's stdout to allow it to receive SIGPIPE if gzip exits
                bcp_process.stdout.close()
                
                # Read BCP stderr (this will block until stderr is closed)
                bcp_stderr = bcp_process.stderr.read().decode('utf-8', errors='replace')
                
                # Wait for both processes to complete
                bcp_returncode = bcp_process.wait()
                gzip_returncode = gzip_process.wait()
            
            # Parse BCP stderr to extract record count
            record_count = 0  # Initialize record count
            bcp_stderr_lines = []
            if bcp_stderr:
                for line in bcp_stderr.strip().split('\n'):
                    line = line.strip()
                    if line:
                        bcp_stderr_lines.append(line)
                        self.logger.debug(f'BCP: {line}')
                
                # Parse final record count from stderr output
                # Look for the final "X rows copied." message
                if bcp_stderr_lines:
                    # Log last few stderr lines for debugging
                    self.logger.debug(f'BCP stderr last 5 lines: {bcp_stderr_lines[-5:]}')
                    
                    # Search for the final "X rows copied." message (usually at the end)
                    # The message format is: "                                                          100000 rows copied."
                    # with leading whitespace and optional period
                    for line in reversed(bcp_stderr_lines):
                        # Match pattern like "100000 rows copied." (with optional leading/trailing whitespace and period)
                        # More flexible pattern to handle various formats
                        match = re.search(r'(\d+)\s+rows?\s+copied', line, re.IGNORECASE)
                        if match:
                            record_count = int(match.group(1))
                            self.logger.info(f'Parsed final record count from BCP output: {record_count:,} rows')
                            break
                    
                    # If not found, try alternative pattern and log for debugging
                    if record_count == 0:
                        self.logger.warning(f'Could not parse record count from BCP stderr. Last 10 lines: {bcp_stderr_lines[-10:]}')
                        # Try alternative pattern - look for any line with "rows copied"
                        for line in reversed(bcp_stderr_lines):
                            # Try matching just numbers followed by "rows" and "copied" anywhere in line
                            match = re.search(r'(\d+)\s+rows?', line, re.IGNORECASE)
                            if match and 'copied' in line.lower():
                                record_count = int(match.group(1))
                                self.logger.info(f'Parsed record count using alternative pattern: {record_count:,} rows')
                                break
                    
                    # If not found, log all stderr lines for debugging
                    if record_count == 0:
                        self.logger.warning(f'Could not parse record count from BCP stderr. Last 10 lines: {bcp_stderr_lines[-10:]}')
                
                # Read and log gzip stderr if any
                gzip_stderr = gzip_process.stderr.read().decode('utf-8', errors='replace')
                if gzip_stderr:
                    self.logger.debug(f'gzip: {gzip_stderr}')
                
                # Check for errors
                if bcp_returncode != 0:
                    error_msg = f'BCP command failed with return code {bcp_returncode}'
                    if bcp_stderr_lines:
                        error_msg += f': {" ".join(bcp_stderr_lines[-5:])}'  # Last 5 lines
                    self.logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                if gzip_returncode != 0:
                    error_msg = f'gzip command failed with return code {gzip_returncode}'
                    if gzip_stderr:
                        error_msg += f': {gzip_stderr}'
                    self.logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
            
            bcp_end_time = time.time()
            bcp_duration = bcp_end_time - bcp_start_time
            
            # Get compressed file size
            compressed_size = os.path.getsize(compressed_file)
            compressed_size_mb = compressed_size / (1024 * 1024)
            
            self.logger.info(
                f'BCP export and compression completed in {bcp_duration:.2f} seconds. '
                f'Compressed file: {compressed_file} ({compressed_size_mb:.2f} MB)'
            )
            
            # Emit final record count metric
            if record_count > 0:
                self._emit_record_count_metric(record_count)
                self.logger.info(f'Emitted metric for {record_count:,} records')
            
            # Don't yield any records - all data is in the CSV.gz file
            # Return empty generator (function must be a generator, even if it yields nothing)
            if False:
                yield  # This makes the function a generator
            
        finally:
            # Keep the CSV.gz file - don't delete it
            # The file is at compressed_file location and contains all the data
            pass