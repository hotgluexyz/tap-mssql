"""BCP (Bulk Copy Program) helper for MSSQL tap.

Used when config use_bcp_for_sync is True to export stream data via BCP
instead of SQLAlchemy. Data is written to CSV on disk; get_records_via_bcp
advances the bookmark by writing a Singer STATE message to stdout (no
singer-python dependency). No dummy records are yielded.
"""
from __future__ import annotations

import copy
import datetime
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable

import pendulum

# Get output directory from environment variables
job_root = os.environ.get("JOB_ROOT")
job_id = os.environ.get("JOB_ID", "")
LOCAL_OUTPUT_DIR = f"/home/hotglue/{job_id}/sync-output" if job_root else "../.secrets"

# Get BCP path from environment
BCP_PATH = "/opt/mssql-tools/bin/bcp" if job_root else "bcp"

# BCP character delimiter (must match _build_bcp_command -t)
BCP_DELIMITER = "\x1F"


def read_last_line(path: str) -> str | None:
    """Read the last line of a file (efficient for large files)."""
    with open(path, "rb") as f:
        try:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b"\n":
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        return f.readline().decode("utf-8", errors="replace")


def _build_sql_query_string(
    stream: Any,
    selected_columns: list[str],
    context: dict | None = None,
) -> str:
    """Build SQL query string for BCP.

    Args:
        stream: mssqlStream instance (self from get_records).
        selected_columns: List of column names to select.
        context: Stream partition or context dictionary.

    Returns:
        SQL query string.
    """
    table = stream.connector.get_table(
        full_table_name=stream.fully_qualified_name,
        column_names=selected_columns,
    )

    database = stream.config.get("database")
    schema = "dbo"
    if hasattr(table, "schema") and table.schema:
        schema = table.schema
    elif hasattr(table, "key") and "." in table.key:
        parts = table.key.split(".")
        if len(parts) > 1:
            schema = parts[0]

    table_name = table.name
    column_list = ", ".join([f"[{col}]" for col in selected_columns])

    if schema and schema != "":
        from_clause = f"[{database}].[{schema}].[{table_name}]"
    else:
        from_clause = f"[{database}].[dbo].[{table_name}]"

    max_records = stream.config.get("max_records")
    if max_records is not None:
        select_clause = f"SELECT TOP {max_records} {column_list}"
    elif getattr(stream, "ABORT_AT_RECORD_COUNT", None) is not None:
        limit_val = stream.ABORT_AT_RECORD_COUNT + 1
        select_clause = f"SELECT TOP {limit_val} {column_list}"
    else:
        select_clause = f"SELECT {column_list}"

    query_parts = [select_clause, f"FROM {from_clause}"]

    if stream.replication_key:
        replication_key_col = table.columns[stream.replication_key]

        if replication_key_col.type.python_type in (datetime.datetime, datetime.date):
            start_val = stream.get_starting_timestamp(context)
            if start_val:
                lookback_window_days = stream.config.get("lookback_window_days", 0)
                stream.logger.info(
                    f"Debug - lookback_window_days: {lookback_window_days} days."
                )
                if isinstance(lookback_window_days, int) and lookback_window_days > 0:
                    stream.logger.info(
                        f"Applying replication key redundancy of {lookback_window_days} days to the start_val {start_val}."
                    )
                    start_val -= datetime.timedelta(days=lookback_window_days)
                    stream.logger.info(f"Debug - FINAL start_val: {start_val}")
                else:
                    stream.logger.info("No redundancy was applied")
        else:
            start_val = stream.get_starting_replication_key_value(context)

        if start_val:
            if isinstance(start_val, (datetime.datetime, datetime.date)):
                if isinstance(start_val, datetime.datetime):
                    start_val = start_val + datetime.timedelta(milliseconds=1)
                    iso_str = start_val.isoformat()
                    has_offset = "Z" in iso_str or bool(
                        re.search(r"[+-]\d{2}:", iso_str)
                    )
                    if not has_offset:
                        iso_str += "+00:00"
                    escaped = iso_str.replace("'", "''")
                    val_str = f"CAST('{escaped}' AS datetimeoffset(6))"
                else:
                    val_str = start_val.strftime("'%Y-%m-%d'")
            elif isinstance(start_val, str):
                escaped_val = start_val.replace("'", "''")
                val_str = f"'{escaped_val}'"
            else:
                val_str = str(start_val)

            query_parts.append(f"WHERE [{stream.replication_key}] > {val_str}")

        query_parts.append(f"ORDER BY [{stream.replication_key}]")

    return " ".join(query_parts)


def _build_bcp_command(
    stream: Any,
    sql_query: str,
    output_file: str,
) -> list[str]:
    """Build BCP command arguments."""
    config = stream.config
    host = config.get("host")
    port = config.get("port", "1433")
    database = config.get("database")
    user = config.get("user")
    password = config.get("password")

    server = f"tcp:{host},{port}"
    cmd = [
        BCP_PATH,
        sql_query,
        "queryout",
        output_file,
        "-S",
        server,
        "-d",
        database,
        "-U",
        user,
        "-P",
        password,
        "-c",
        "-t",
        BCP_DELIMITER,
        "-b",
        "1000000000",
    ]
    url_query = config.get("sqlalchemy_url_query") or {}
    if (url_query.get("TrustServerCertificate") or "").lower() == "yes":
        cmd.append("-u")

    return cmd


def _get_last_replication_key_value_from_csv(
    stream: Any,
    csv_file: str,
    selected_column_names: list[str],
) -> Any | None:
    """Read the last row from the BCP CSV and return the replication_key value."""
    if not stream.replication_key or stream.replication_key not in selected_column_names:
        return None
    try:
        last_line = read_last_line(csv_file)
        if not last_line:
            return None
        parts = last_line.split(BCP_DELIMITER)
        rk_index = selected_column_names.index(stream.replication_key)
        if rk_index >= len(parts):
            return None
        raw = parts[rk_index].strip()
        if raw == "":
            return None
        prop = (stream.schema.get("properties") or {}).get(stream.replication_key) or {}
        fmt = prop.get("format", "")
        if fmt == "date-time":
            return pendulum.parse(raw).isoformat()
        if fmt == "date":
            return pendulum.parse(raw).date().isoformat()
        return raw
    except Exception as e:
        stream.logger.warning(
            f"Could not read last replication key value from CSV: {e}",
            exc_info=True,
        )
        return None


def _update_job_metrics(stream: Any, stream_name: str, record_count: int, output_dir: str) -> None:
    """Update job_metrics.json with record count for the stream."""
    job_metrics_path = os.path.expanduser(
        os.path.join(output_dir, "job_metrics.json")
    )

    if not os.path.isfile(job_metrics_path):
        Path(job_metrics_path).touch()

    with open(job_metrics_path, "r+") as f:
        content = {}
        try:
            content = json.loads(f.read())
        except (json.JSONDecodeError, ValueError):
            pass

        if not content.get("recordCount"):
            content["recordCount"] = {}

        content["recordCount"][stream_name] = (
            content["recordCount"].get(stream_name, 0) + record_count
        )

        f.seek(0)
        stream.logger.info(
            f"Updating job metrics for {stream_name} with {record_count} records"
        )
        f.write(json.dumps(content))
        f.truncate()


def _parse_rows_copied(text: str) -> int:
    """Return last 'N rows copied' value found in text, or 0."""
    count = 0
    for line in text.strip().split("\n"):
        line = line.strip()
        if line:
            match = re.search(r"(\d+)\s+rows?\s+copied", line, re.IGNORECASE)
            if match:
                count = int(match.group(1))
    return count


def _write_state_message(state: dict[str, Any]) -> None:
    """Emit a Singer STATE message to stdout (NDJSON). No singer-python dependency."""
    msg = {"type": "STATE", "value": state}
    sys.stdout.write(json.dumps(msg, default=str) + "\n")
    sys.stdout.flush()


def _advance_bookmark_for_bcp(stream: Any, last_rk_value: Any) -> None:
    """Advance bookmark by writing a Singer STATE message to stdout.

    Uses the tap's state when available (e.g. stream._tap.state). No dummy
    record is yielded. Implemented without singer-python to avoid jsonschema
    conflict with singer-sdk.
    """
    tap = getattr(stream, "_tap", None)
    state = None
    if tap is not None:
        state = getattr(tap, "state", None) or getattr(tap, "_state", None)
    tap_stream_id = getattr(stream, "tap_stream_id", stream.name)

    # Normalize for JSON state (e.g. datetime/date -> ISO string)
    rep_key_value: Any = last_rk_value
    if isinstance(rep_key_value, (datetime.datetime, datetime.date)):
        rep_key_value = pendulum.instance(rep_key_value).isoformat()

    if state is not None:
        state = copy.deepcopy(state)
        bookmarks = state.setdefault("bookmarks", {})
        stream_bookmark = bookmarks.setdefault(tap_stream_id, {})
        stream_bookmark["replication_key_value"] = rep_key_value
        _write_state_message(state)
        stream.logger.info(
            f"Wrote bookmark (STATE message) for "
            f"{stream.replication_key!r} = {rep_key_value!r}"
        )
    else:
        stream.logger.warning(
            "Could not advance bookmark: tap state not available on stream. "
            "Bookmark for %r will not be updated.",
            stream.replication_key,
        )


def get_records_via_bcp(
    stream: Any,
    context: dict | None,
) -> Iterable[dict[str, Any]]:
    """Run BCP export and advance bookmark via singer.write_bookmark + StateMessage.

    Runs BCP to export stream data to CSV. When replication_key is set and
    record_count > 0, advances the bookmark by writing state directly (no
    dummy record). Actual data remains in the CSV on disk.

    Args:
        stream: mssqlStream instance (self from get_records).
        context: Stream partition or context dictionary.

    Yields:
        Nothing (records are in the CSV on disk; bookmark is written to state).
    """
    selected_column_names = list(stream.get_selected_schema()["properties"].keys())
    sql_query = _build_sql_query_string(stream, selected_column_names, context)

    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
    stream_name = stream.name
    csv_file = os.path.join(LOCAL_OUTPUT_DIR, f"{stream_name}.csv")

    bcp_cmd = _build_bcp_command(stream, sql_query, csv_file)

    bcp_cmd_safe = bcp_cmd.copy()
    if "-P" in bcp_cmd_safe:
        pwd_idx = bcp_cmd_safe.index("-P")
        if pwd_idx + 1 < len(bcp_cmd_safe):
            bcp_cmd_safe[pwd_idx + 1] = "***"
    stream.logger.info(f"Executing BCP: {' '.join(bcp_cmd_safe)}")
    stream.logger.info(f"BCP SQL query: {sql_query}")

    bcp_start_time = time.time()
    stream.logger.info(f"Starting BCP export to {csv_file}")

    bcp_process = subprocess.Popen(
        bcp_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    stream.logger.info(f"BCP process started (PID: {bcp_process.pid})")

    stream.logger.info("Waiting for BCP to complete...")
    bcp_stdout_bytes, bcp_stderr_bytes = bcp_process.communicate()
    bcp_returncode = bcp_process.returncode
    stream.logger.info(f"BCP process completed with return code: {bcp_returncode}")

    bcp_stdout = bcp_stdout_bytes.decode("utf-8", errors="replace")
    bcp_stderr = bcp_stderr_bytes.decode("utf-8", errors="replace")
    stream.logger.info(
        f"BCP stdout ({len(bcp_stdout)} chars), stderr ({len(bcp_stderr)} chars)"
    )

    record_count = _parse_rows_copied(bcp_stdout)
    if record_count == 0 and bcp_stderr:
        record_count = _parse_rows_copied(bcp_stderr)
        if record_count:
            stream.logger.info(
                f"Parsed record count from BCP stderr: {record_count:,} rows"
            )
    if record_count:
        stream.logger.info(
            f"Parsed record count from BCP output: {record_count:,} rows"
        )
    if record_count == 0:
        stream.logger.warning(
            'Could not parse "X rows copied" from BCP output. '
            f"stdout last 500 chars: {bcp_stdout[-500:]!r}; stderr: {bcp_stderr[-500:]!r}"
        )

    if bcp_returncode != 0:
        error_msg = f"BCP command failed with return code {bcp_returncode}"
        if bcp_stderr:
            error_msg += f": {bcp_stderr.strip()[-500:]}"
        if bcp_stdout and not bcp_stderr:
            error_msg += f": {bcp_stdout.strip()[-500:]}"
        stream.logger.error(error_msg)
        raise RuntimeError(error_msg)

    bcp_end_time = time.time()
    bcp_duration = bcp_end_time - bcp_start_time
    csv_size = os.path.getsize(csv_file)
    csv_size_mb = csv_size / (1024 * 1024)
    stream.logger.info(
        f"BCP export completed in {bcp_duration:.2f} seconds. "
        f"CSV file: {csv_file} ({csv_size_mb:.2f} MB)"
    )

    _update_job_metrics(stream, stream_name, record_count, LOCAL_OUTPUT_DIR)
    stream.logger.info(f"Emitted metric for {record_count:,} records")

    if record_count > 0 and stream.replication_key:
        last_rk_value = _get_last_replication_key_value_from_csv(
            stream, csv_file, selected_column_names
        )
        if last_rk_value is not None:
            _advance_bookmark_for_bcp(stream, last_rk_value)

    # No records yielded (data is in CSV); this makes the function a generator
    # so "yield from get_records_via_bcp(...)" in client.get_records is valid.
    return
    yield  # unreachable; ensures this is a generator