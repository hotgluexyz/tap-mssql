"""mssql tap class."""

from __future__ import annotations

from singer_sdk import SQLTap, SQLStream, SQLConnector
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_mssql.client import mssqlStream, mssqlConnector


class Tapmssql(SQLTap):
    """mssql tap class."""

    name = "tap-mssql"
    default_stream_class = mssqlStream
    default_connector_class = mssqlConnector
    _tap_connector: SQLConnector = None

    @property
    def tap_connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        if self._tap_connector is None:
            self._tap_connector = self.default_connector_class(dict(self.config))
        return self._tap_connector
    
    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.tap_connector

        result: dict[str, list[dict]] = {"streams": []}
        catalog_entries = connector.discover_catalog_entries()

        config_replication_keys = self.config.get("replication_keys")
        if config_replication_keys:
            for catalog_entry in catalog_entries:
                for replication_key in config_replication_keys:
                    if catalog_entry.get("tap_stream_id") == replication_key.get("table"):
                        catalog_entry["replication_key"] = replication_key.get("replication_key")
                        break

        result["streams"].extend(catalog_entries)

        self._catalog_dict = result
        return self._catalog_dict

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchamey",
            required=True,
            allowed_values=["mssql"],
            default="mssql"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver you will be using to connect to the SQL server",
            required=True,
            allowed_values=["pyodbc", "pymssql"],
            default="pymssql"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving out the SQL Instance",
            required=True
        ),
        th.Property(
            "port",
            th.StringType,
            description="The port on which SQL awaiting connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account who has been granted access to the SQL Server",
            required=True
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account",
            required=True,
            secret=True
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection",
            required=True
        ),
        th.Property(
            "sqlalchemy_eng_params",
            th.ObjectType(
                th.Property(
                    "fast_executemany",
                    th.StringType,
                    description="Fast Executemany Mode: True, False"
                ),
                th.Property(
                    "future",
                    th.StringType,
                    description="Run the engine in 2.0 mode: True, False"
                )
            ),
            description="SQLAlchemy Engine Paramaters: fast_executemany, future"
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.ObjectType(
                th.Property(
                    "driver",
                    th.StringType,
                    description="The Driver to use when connection should match the Driver Type"
                ),
                th.Property(
                    "TrustServerCertificate",
                    th.StringType,
                    description="This is a Yes No option"
                )
            ),
            description="SQLAlchemy URL Query options: driver, TrustServerCertificate"
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property(
                            "format",
                            th.StringType,
                            description="Currently the only format is jsonl",
                        ),
                        th.Property(
                            "compression",
                            th.StringType,
                            description="Currently the only compression options is gzip",
                        )
                    )
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description="the directory you want batch messages to be placed in\n"\
                                        "example: file://test/batches",
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description="What prefix you want your messages to have\n"\
                                        "example: test-batch-",
                        )
                    )
                )
            ),
            description="Optional Batch Message configuration",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "hd_jsonschema_types",
            th.BooleanType,
            default=False,
            description="Turn on Higher Defined(HD) JSON Schema types to assist Targets"
        ),
    ).to_dict()

    def discover_streams(self) -> list[SQLStream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            result.append(
                self.default_stream_class(
                    tap=self,
                    catalog_entry=catalog_entry,
                    connector=self.tap_connector
                )
            )

        return result

if __name__ == "__main__":
    Tapmssql.cli()
