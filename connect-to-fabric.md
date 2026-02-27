# Connecting to a Microsoft Fabric Warehouse

Use an Entra service principal to connect to the warehouse without a user/password.

## Getting service principal credentials

1. **Register an app in Entra ID**
   - Go to [Azure Portal](https://portal.azure.com) → **Microsoft Entra ID** → **Manage** → **App registrations** → **New registration**.
   - Name the app (e.g. `tap-mssql-fabric`), choose **Single tenant only**, then **Register**.

2. **Get Client ID**
   - On the app’s **Overview** page, copy **Application (client) ID**. You will set this as your user when connecting.

3. **Create a client secret**
   - Go to **Manage** → **Certificates & secrets** → **New client secret** → add description, choose expiry → **Add**.
   - Copy the **Value** immediately (it’s only shown once). This is the client secret that will be used as `password`.

4. **Grant the app access to Fabric**
   - In **Microsoft Fabric** (portal or admin), open your workspace and add the app (by name or application ID) as a member with at least **Viewer** (or the role that allows read on the warehouse).
   - Ensure the app has access to the **warehouse** (and database) you specify as `database` in your config.

5. **Get the warehouse host**
   - In **Microsoft Fabric**, open your workspace and select the **Warehouse** you want to connect to.
   - Open the warehouse, then go to **Settings** (gear icon) → **SQL Endpoint** → **SQL Endpoint**.
   - Copy the **SQL connection string** value. It has the form `<warehouse-id>.datawarehouse.fabric.microsoft.com` — use this as `host` in your config.

## Config for Fabric with Entra service principal

Set up your `config.json` with the following required values:
- **dialect**: "mssql"
- **driver_type**: "pyodbc"
- **host**: the **SQL connection string** value from the warehouse
- **database**: the warehouse name
- **user**: the **Application (client) ID** from the app registration
- **password**: the **client secret** from the app registration
- **sqlalchemy_url_query.driver**: "ODBC Driver 18 for SQL Server"
- **sqlalchemy_url_query.Authentication**: "ActiveDirectoryServicePrincipal"

```json
{
  "dialect": "mssql",
  "driver_type": "pyodbc",
  "host": "your-warehouse.datawarehouse.fabric.microsoft.com",
  "database": "your_warehouse",
  "user": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "password": "your-client-secret-value",
  "sqlalchemy_url_query": {
    "driver": "ODBC Driver 18 for SQL Server",
    "Authentication": "ActiveDirectoryServicePrincipal"
  }
}
```
