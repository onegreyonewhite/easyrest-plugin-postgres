# EasyREST PostgreSQL Plugin

The **EasyREST PostgreSQL Plugin** is an external plugin for [EasyREST](https://github.com/onegreyonewhite/easyrest) that enables EasyREST to connect to and perform CRUD operations on PostgreSQL databases. This plugin implements the `easyrest.DBPlugin` interface using a PostgreSQL connection pool, session variable injection for context propagation, and transactional function execution.

**Key Features:**

- **CRUD Operations:** Supports SELECT, INSERT, UPDATE, and DELETE queries.
- **Context Injection:** If query fields or conditions reference context variables (using the `erctx.` prefix), the plugin injects these values into session variables using `set_config`.
- **Function Calls:** Executes functions within a transaction, rolling back on error.
- **Connection Pooling:** Uses a PostgreSQL connection pool for optimal performance, with special handling for session variables to avoid race conditions.
- **Deterministic SQL Generation:** Ensures predictable SQL statements by sorting map keys where necessary.
- **COPY Support:** Uses PostgreSQL's COPY command for efficient bulk inserts.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Performance Optimizations](#performance-optimizations)
- [PostgreSQL Setup using Docker](#postgresql-setup-using-docker)
- [SQL Schema and Functions](#sql-schema-and-functions)
- [Environment Variables for EasyREST](#environment-variables-for-easyrest)
- [Building the Plugin](#building-the-plugin)
- [Running EasyREST Server with the Plugin](#running-easyrest-server-with-the-plugin)
- [Testing API Endpoints](#testing-api-endpoints)
- [Working with Tables, Views, and Context Variables](#working-with-tables-views-and-context-variables)
- [License](#license)

---

## Prerequisites

- [Docker](https://www.docker.com) installed on your machine.
- [Go 1.23.6](https://golang.org/dl/) or later.
- Basic knowledge of PostgreSQL and Docker.

## Performance Optimizations

The plugin includes several performance optimizations:

1. **Connection Pool Management:**
   - Configurable maximum open connections (default: 100)
   - Configurable idle connections (default: 25)
   - Connection lifetime management
   - Idle connection timeout

2. **Bulk Operations:**
   - Automatic COPY for large inserts (>100 rows)
   - Memory-efficient result handling
   - Pre-allocated memory for large result sets
   - Batch processing for large operations

3. **Transaction Optimizations:**
   - Read-only transactions for SELECT queries
   - Configurable transaction timeouts
   - Proper connection release

4. **Query Parameters:**
   - `maxOpenConns` - Maximum number of open connections (default: 100)
   - `maxIdleConns` - Maximum number of idle connections (default: 25)
   - `connMaxLifetime` - Connection reuse time in minutes (default: 5)
   - `connMaxIdleTime` - Connection idle time in minutes (default: 10)
   - `timeout` - Query timeout in seconds (default: 30)
   - `bulkThreshold` - Number of rows threshold for using COPY command (default: 100)

Example URI with all optimization parameters:

- **Via Environment Variable:**
  ```bash
  export ER_DB_PG="postgres://postgres:root@localhost:5433/easyrestdb?maxOpenConns=100&maxIdleConns=25&connMaxLifetime=5&connMaxIdleTime=10&timeout=30&bulkThreshold=100&sslmode=disable&search_path=public"
  ```
- **Via Configuration File:**
  ```yaml
  plugins:
    postgres: # Or any name you choose for the plugin instance
      uri: postgres://postgres:root@localhost:5433/easyrestdb?maxOpenConns=100&maxIdleConns=25&connMaxLifetime=5&connMaxIdleTime=10&timeout=30&bulkThreshold=100&sslmode=disable&search_path=public
      path: ./easyrest-plugin-postgres # Path to the plugin binary
  ```

---

## PostgreSQL Setup using Docker

Run PostgreSQL in a Docker container on a non-standard port (e.g., **5433**) with a pre-created database. Open your terminal and execute:

```bash
docker run --name pg-easyrest -p 5433:5432 \
  -e POSTGRES_PASSWORD=root \
  -e POSTGRES_DB=easyrestdb \
  -d postgres:15
```

This command starts a PostgreSQL container:

- **Container Name:** `pg-easyrest`
- **Host Port:** `5433` (mapped to PostgreSQL's default port 5432 in the container)
- **Password:** `root`
- **Database Created:** `easyrestdb`

---

## SQL Schema and Functions

Create a SQL script (e.g., `schema.sql`) with tables, triggers, and functions. The script includes:

1. A basic `users` table with auto-increment ID and timestamp
2. A more complex `products` table with various field types and triggers
3. Functions for business logic and data access
4. Triggers for automatic timestamp updates and validation

See the complete `schema.sql` in the repository for the full implementation.

To execute this script on the running PostgreSQL server, run:

```bash
docker exec -i pg-easyrest psql -U postgres -d easyrestdb < schema.sql
```

---

## Environment Variables for EasyREST

Configure EasyREST to use this PostgreSQL database via the PostgreSQL plugin. Set the following environment variables before starting the EasyREST server:

```bash
# --- Database Connection ---
# The URI for the PostgreSQL database. The plugin will be selected when the URI scheme is 'postgres://'.
export ER_DB_PG="postgres://postgres:root@localhost:5433/easyrestdb?sslmode=disable"

# --- JWT Authentication ---
# Secret for HS* algorithms OR path to public key file for RS*/ES*/PS* algorithms
export ER_TOKEN_SECRET="your-secret-key"
# export ER_TOKEN_PUBLIC_KEY="/path/to/public.key"

# Claim in the JWT token to use as the user identifier
export ER_TOKEN_USER_SEARCH="sub"

# --- General Settings ---
# Default timezone for context propagation.
export ER_DEFAULT_TIMEZONE="GMT"

# Optional: Enable/disable scope checking (default: true if token secret/key is set)
# export ER_CHECK_SCOPE="true"
```

**Note:** If both a configuration file and environment variables are present, the configuration file settings take precedence for overlapping parameters. The `path` for the plugin can only be set via the configuration file.

---

## Building the Plugin

Clone the repository for the EasyREST PostgreSQL Plugin and build the plugin binary. In the repository root, run:

```bash
go build -o easyrest-plugin-postgres postgres_plugin.go
```

This produces the binary `easyrest-plugin-postgres` which must be in your PATH or referenced by the EasyREST server.

---

## Running EasyREST Server with the Plugin

Download and install the pre-built binary for the EasyREST Server from the [EasyREST Releases](https://github.com/onegreyonewhite/easyrest/releases) page. Once installed, ensure the configuration is set up (either via `config.yaml` or environment variables) and run the server binary.

**Using a Configuration File (Recommended):**

1. **Create the `config.yaml` file:** Save the configuration example above (or your customized version) as `config.yaml` (or any name you prefer).
2. **Place the plugin binary:** Ensure the compiled `easyrest-plugin-postgres` binary exists at the location specified in the `path` field within `config.yaml` (e.g., in the same directory as the `easyrest-server` binary if `path: ./easyrest-plugin-postgres` is used).
3. **Run the server:** Execute the EasyREST server binary, pointing it to your configuration file using the `--config` flag:

    ```bash
    ./easyrest-server --config config.yaml
    ```

    *   The server reads `config.yaml`.
    *   It finds the `plugins.postgres` section.
    *   It sees the `postgres://` scheme in the `uri` and knows to use a PostgreSQL plugin.
    *   It uses the `path` field (`./easyrest-plugin-postgres`) to locate and execute the plugin binary.
    *   The server then communicates with the plugin via RPC to handle requests to `/api/postgres/...`.

**Using Environment Variables:**

1. **Set Environment Variables:** Define the necessary `ER_` variables as shown previously.
2. **Place the plugin binary:** The `easyrest-plugin-postgres` binary *must* be located either in the same directory as the `easyrest-server` binary or in a directory included in your system's `PATH` environment variable.
3. **Run the server:**
    ```bash
    ./easyrest-server
    ```
    *   The server detects `ER_DB_PG` starting with `postgres://`.
    *   It searches for an executable named `easyrest-plugin-postgres` in the current directory and in the system `PATH`.
    *   If found, it executes the plugin binary and communicates via RPC.

    **Limitation:** You cannot specify a custom path or name for the plugin binary when using only environment variables.

---

## Testing API Endpoints

1. **Creating a JWT Token:**

   EasyREST uses JWT for authentication. Use your preferred method to generate a token with the appropriate claims (including the subject claim specified by `ER_TOKEN_USER_SEARCH`). For example, you might generate a token using a script or an [online tool](https://jwt.io/) and then export it:

   ```bash
   export TOKEN="your_generated_jwt_token"
   ```

2. **Calling the API:**

   With the server running, you can test the endpoints. The API path depends on the name you gave the plugin instance in your `config.yaml` (e.g., `postgres`) or defaults to `postgres` if using `ER_DB_PG`. Assuming the name is `postgres`:

   ```bash
   # Fetch users
   curl -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/postgres/users/?select=id,name,created_at"

   # Create users (setup name from JWT-token 'sub' claim)
   curl -X POST "http://localhost:8080/api/postgres/users/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '[{"name": "erctx.claims_sub"},{"name": "request.claims.sub"}]'

   # Call a function
   curl -X POST "http://localhost:8080/api/postgres/rpc/doSomething/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"jsonParam": "test"}'
   ```

---

## Working with Tables, Views, and Context Variables

This plugin allows you to interact with your PostgreSQL database tables and views directly through the EasyREST API. It also provides a powerful mechanism for injecting context from the incoming request (like user information from a JWT) directly into your SQL session using PostgreSQL's `set_config` function.

### Interacting with Tables and Views

Once the plugin is configured and connected to your database, EasyREST exposes API endpoints for your tables and views under the path `/api/{plugin_name}/{table_or_view_name}/` (e.g., `/api/postgres/users/`).

-   **Read (SELECT):** Use `GET` requests. You can specify fields (`?select=col1,col2`), filters (`?where=...`), ordering (`?orderBy=...`), grouping (`?groupBy=...`), limit (`?limit=...`), and offset (`?offset=...`). Views are accessible via `GET` just like tables.
-   **Create (INSERT):** Use `POST` requests with a JSON array of objects in the request body.
-   **Update (UPDATE):** Use `PATCH` requests. Provide the data to update in the request body and specify the rows to update using `?where=...` query parameters.
-   **Delete (DELETE):** Use `DELETE` requests, specifying rows to delete using `?where=...` query parameters.

### Schema Introspection and Type Mapping

The plugin automatically introspects your database schema (tables and views) and makes it available via the `/api/{plugin_name}/schema` endpoint. This schema reflects the columns and their basic types.

When the plugin reads the schema, it maps PostgreSQL data types to simpler, JSON-friendly types:

| PostgreSQL Data Type Category | Example Types                               | Schema Type | Notes                                    |
| :--------------------------- | :------------------------------------------ | :---------- | :--------------------------------------- |
| Integer Types               | `INTEGER`, `SMALLINT`, `BIGINT`             | `integer`   |                                          |
| Numeric Types              | `NUMERIC`, `DECIMAL`, `REAL`, `DOUBLE`      | `number`    |                                          |
| Character Types            | `VARCHAR`, `CHAR`, `TEXT`                   | `string`    |                                          |
| Binary Types               | `BYTEA`                                     | `string`    | Returned as base64 string                |
| Date/Time Types            | `DATE`, `TIMESTAMP`, `TIME`                 | `string`    | Formatted as string (e.g., `YYYY-MM-DD`) |
| Boolean                    | `BOOLEAN`                                   | `boolean`   |                                          |
| JSON Types                 | `JSON`, `JSONB`                            | `string`    | Serialized as JSON string                |

### Using Context Variables (`erctx.` / `request.`)

The plugin uses PostgreSQL's `set_config` function to set session variables for each request. These variables are available using `current_setting()` in your SQL code.

**Example: Using Context in Functions**

```sql
-- Function that uses context variables
CREATE OR REPLACE FUNCTION get_my_products()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(row_to_json(p))
        FROM (
            SELECT id, sku, name, price
            FROM products
            WHERE created_by = current_setting('request.claims_sub', TRUE)
        ) p
    );
END;
$$ LANGUAGE plpgsql;
```

Call this function via the API:
```bash
curl -X POST "http://localhost:8080/api/postgres/rpc/getMyProducts/" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}'
```

---

## License

EasyREST PostgreSQL Plugin is licensed under the Apache License 2.0.  
See the file "LICENSE" for more information.

© 2025 Sergei Kliuikov
