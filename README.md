```markdown
# EasyREST PostgreSQL Plugin

The **EasyREST PostgreSQL Plugin** is an external plugin for [EasyREST](https://github.com/onegreyonewhite/easyrest) that enables EasyREST to connect to and perform CRUD operations on PostgreSQL databases. This plugin implements the `easyrest.DBPlugin` interface using a PostgreSQL connection pool, context-based value substitution, and transactional function execution.

**Key Features:**

- **CRUD Operations:** Supports SELECT, INSERT, UPDATE, and DELETE queries.
- **Context Substitution:** If query fields or conditions reference context variables (using the `erctx.` prefix), the plugin substitutes these values from the provided context.
- **Function Calls:** Executes functions (stored procedures) using SELECT syntax within a transaction, ensuring rollback on error.
- **Connection Pooling:** Uses a PostgreSQL connection pool for optimal performance.
- **Deterministic SQL Generation:** Ensures predictable SQL statements by sorting map keys where necessary.
- **Placeholder Conversion:** Automatically converts standard SQL placeholders ("?") to PostgreSQL-style placeholders (e.g., `$1`, `$2`, ...).

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [PostgreSQL Setup using Docker](#postgresql-setup-using-docker)
- [SQL Schema and Function](#sql-schema-and-function)
- [Environment Variables for EasyREST](#environment-variables-for-easyrest)
- [Building the Plugin](#building-the-plugin)
- [Running EasyREST Server with the Plugin](#running-easyrest-server-with-the-plugin)
- [Testing API Endpoints](#testing-api-endpoints)
- [License](#license)

---

## Prerequisites

- [Docker](https://www.docker.com) installed on your machine.
- [Go 1.23.6](https://golang.org/dl/) or later.
- Basic knowledge of PostgreSQL and Docker.

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
- **Host Port:** `5433` (mapped to PostgreSQL’s default port 5432 in the container)
- **Password:** `root`
- **Database Created:** `easyrestdb`

---

## SQL Schema and Function

Create a SQL script (e.g., `schema.sql`) with the following content. This script creates a `users` table with a primary key, a `name` column, and a `created_at` timestamp that defaults to the current time. It also defines a simple function named `doSomething` which returns a processed message.

```sql
-- schema.sql

-- Create the 'users' table
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a function 'doSomething'
CREATE OR REPLACE FUNCTION doSomething(jsonParam TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN 'Processed: ' || jsonParam;
END;
$$ LANGUAGE plpgsql;
```

To execute this script on the running PostgreSQL server, run:

```bash
docker exec -i pg-easyrest psql -U postgres -d easyrestdb < schema.sql
```

---

## Environment Variables for EasyREST

Configure EasyREST to use this PostgreSQL database via the PostgreSQL plugin. Set the following environment variables before starting the EasyREST server:

```bash
export ER_DB_PG="postgres://postgres:root@localhost:5433/easyrestdb?sslmode=disable"
export ER_TOKEN_SECRET="your-secret-key"
export ER_TOKEN_USER_SEARCH="sub"
export ER_DEFAULT_TIMEZONE="GMT"
```

- **ER_DB_PG:** The URI for the PostgreSQL database. The plugin is selected when the URI scheme is `pg://`.
- **ER_TOKEN_SECRET & ER_TOKEN_USER_SEARCH:** Used by EasyREST for JWT authentication.
- **ER_DEFAULT_TIMEZONE:** Default timezone for context propagation.

---

## Building the Plugin

Clone the repository for the EasyREST PostgreSQL Plugin and build the plugin binary. In the repository root, run:

```bash
go build -o easyrest-plugin-postgres postgres_plugin.go
```

This produces the binary `easyrest-plugin-postgres` which must be in your PATH or referenced by the EasyREST server.

---

## Running EasyREST Server with the Plugin

Download and install the pre-built binary for the EasyREST Server from the [EasyREST Releases](https://github.com/onegreyonewhite/easyrest/releases) page. Once installed, set the environment variables (as above) and run the server binary:

```bash
./easyrest-server
```

The server will detect the `ER_DB_PG` environment variable and launch the PostgreSQL plugin via RPC.

---

## Testing API Endpoints

1. **Creating a JWT Token:**

   EasyREST uses JWT for authentication. Generate a token with the appropriate claims (including the subject claim specified by `ER_TOKEN_USER_SEARCH`). For example, use your preferred method or an [online tool](https://jwt.io/) to generate a token, then export it:

   ```bash
   export TOKEN="your_generated_jwt_token"
   ```

2. **Calling the API:**

   With the server running, you can test the endpoints. For example, to fetch users:

   ```bash
   curl -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/pg/users/?select=id,name,created_at"
   ```

   To create a user (using a context value from the JWT-token's subject):

   ```bash
   curl -X POST "http://localhost:8080/api/pg/users/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '[{"name": "erctx.claims_sub"},{"name": "request.claims.sub"}]'
   ```

   To call the function:

   ```bash
   curl -X POST "http://localhost:8080/api/pg/rpc/doSomething/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"jsonParam": "test"}'
   ```

---

## License

EasyREST PostgreSQL Plugin is licensed under the Apache License 2.0.  
See the file "LICENSE" for more information.

© 2025 Sergei Kliuikov
