
# pgrollback

PostgreSQL proxy for **integration and functional tests**: your app talks to a real database, but writes are not permanently committed. One long-lived backend transaction per test session; client `BEGIN` / `COMMIT` / `ROLLBACK` are mapped to savepoints.

---

## Contents

- [Why use it](#why-use-it)
- [Features](#features)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Run the proxy](#run-the-proxy)
- [Connect from tests](#connect-from-tests)
- [Special commands](#special-commands)
- [Build](#build)
- [Running tests](#running-tests)
- [Configuration](#configuration)
- [Transaction mapping](#transaction-mapping)
- [Language examples](#language-examples)
- [SQL log GUI](#sql-log-gui)
- [CI sketch](#ci-sketch)
- [Safety](#safety)
- [License](#license)

---

## Why use it

Integration tests change the database. Common pain points:

- Resetting or cloning the DB between tests is slow or heavy.
- A SQL transaction is **one connection**; apps use **pools and many connections**.

pgrollback keeps **one real PostgreSQL transaction** on the server side and exposes normal `BEGIN` / `COMMIT` / `ROLLBACK` to clients via savepoints, so multiple connections can share the same logical test session.

---

## Features

- Any PostgreSQL client (wire protocol).
- Connection pools and multiple connections per test ID.
- Automatic rollback when the session ends; no extra cleanup scripts.
- Real PostgreSQL execution (not mocked SQL).
- Optional web GUI for queries on the same listen port.

---

## How it works

```
Your app  →  pgrollback (proxy)  →  PostgreSQL
```

The proxy holds **one** long-lived transaction to Postgres. Application `COMMIT` does **not** commit that base transaction—it becomes `RELEASE SAVEPOINT` (and similar for `ROLLBACK`).

Example:

| Your app | On PostgreSQL (conceptually) |
|----------|--------------------------------|
| `BEGIN; INSERT …; COMMIT;` | `SAVEPOINT pgrollback_v_1; INSERT …; RELEASE SAVEPOINT pgrollback_v_1;` |
| `BEGIN; INSERT …; ROLLBACK;` | `SAVEPOINT …; INSERT …; ROLLBACK TO SAVEPOINT …; RELEASE …;` |

When the test finishes (or you issue a full rollback command), the sandbox is discarded—**no permanent writes**.

---

## Quick start

1. **Install Go** (1.23+) and, on Windows for this repo, a **64-bit MinGW** toolchain for CGO (see [Build](#build)).
2. **Config** — Copy `config/pgrollback.yaml` and set `postgres.*` and `proxy.listen_*` for your machine.
3. **Run the proxy** — `make run` or `./bin/pgrollback --config config/pgrollback.yaml` (Windows: `build.bat` / `run.bat` after `setEnvironments.bat`).
4. **Point tests at the proxy** — Use the proxy host/port as the DB endpoint and set `application_name` to `pgrollback_<testID>` so sessions are isolated (see below).

---

## Run the proxy

```bash
./bin/pgrollback --config config/pgrollback.yaml
```

**Windows (tray build):** `build.bat` produces a GUI-subsystem binary (tray icon, no console). For console logs, use `go run ./cmd/pgrollback` or build without `-H windowsgui`. Right‑click the tray icon → **Open GUI** / **Quit**.

---

## Connect from tests

Use the **proxy** port (not the raw Postgres port) and a stable **test id** via `application_name`:

```text
host = <proxy listen_host>
port = <proxy listen_port>
application_name = pgrollback_<your_test_id>
```

Same `test_id` across connections = same sandbox (shared logical transaction). Different `test_id` = isolated sandboxes.

To reset a sandbox without reconnecting, execute the SQL string `pgrollback rollback` (see [Special commands](#special-commands)).

---

## Special commands

Send as a **single statement** through the connection to the proxy (not forwarded as normal DDL/DML). The test id comes from `application_name`.

| Command | Purpose |
|--------|---------|
| `pgrollback rollback` | Roll back the **entire** base transaction for this test id and start a new one (reset sandbox). |
| `pgrollback status` | Result columns include `test_id`, `active`, `level`, `created_at`. |
| `pgrollback list` | One row per session (`test_id`, `active`, `level`, `created_at`). |
| `pgrollback cleanup` | Remove expired sessions; returns how many were cleaned. |
| `pgrollback disconnect` | (Used by tests/tools) disconnect flow for a session. |

Example: `db.Exec("pgrollback rollback")` in Go, or the equivalent in your stack.

---

## Build

### Requirements

- **Go 1.23+**
- **CGO** — This project uses `github.com/pganalyze/pg_query_go/v5` (C extension).

**Windows:** install a **64-bit MinGW** GCC (e.g. MSYS2: `pacman -S mingw-w64-x86_64-gcc`), then run **`setEnvironments.bat`** so `PATH`, `CC`, `CXX`, and `CGO_ENABLED=1` are set. See `.vscode/settings.json` for terminal integration.

```bash
go build -o bin/pgrollback ./cmd/pgrollback
# or
make build
```

Without a proper 64-bit `gcc`, you may see errors like `sorry, unimplemented: 64-bit mode not compiled in`.

---

## Running tests

### What runs where

| Suite | Packages | Needs PostgreSQL? | Build tag |
|-------|----------|-------------------|-----------|
| **Unit** | `./pkg/...`, `./internal/...`, `./tests/unit/...` | No (except some `tests/unit/proxy` tests that skip if DB missing) | *(none)* |
| **Integration** | `./test/integration/...` | **Yes** — real server + config | `-tags=integration` |

Integration tests start their own proxy via `TestMain`; they use **`PGTEST_CONFIG`** (default `config/pgtest-sandbox.yaml` when using `test.bat`) and **`PGROLLBACK_LISTEN_HOST` / `PGROLLBACK_LISTEN_PORT`** for the proxy listen address.

### Windows (`test.bat`)

`test.bat` calls `setEnvironments.bat` first. Arguments can appear in any order.

| You type | What happens |
|----------|----------------|
| `test.bat` | Unit tests → integration tests; output in **`test_results_unit.log`** and **`test_results_integration.log`**. |
| `test.bat` `detailed` | Same suites, output on the **console** (no log files). |
| `test.bat` `unit` | Only unit tests. |
| `test.bat` `integration` | Only integration tests. |
| `test.bat` `integration` `TestProtectionAgainstAccidentalCommit` | One integration test (substring match for `-run`). |
| `test.bat` `integration` `detailed` `TestIsolationBetweenTestIDs` | One test, console output. |
| `test.bat` `all` | Same as default: unit + integration. |

If **`PGROLLBACK_LISTEN_PORT`** is already in use on `PGROLLBACK_LISTEN_HOST`, the script sets port to **`1` + original port** (e.g. `5433` → `15433`) and prints a message.

**Examples (integration test names from this repo):**

```bat
test.bat unit detailed
test.bat integration detailed TestProtectionAgainstAccidentalCommit
test.bat integration detailed TestTransactionSharing
test.bat integration TestMultiStatementMultiConnectionWorkflow
```

### Linux / macOS

```bash
go test -count=1 ./pkg/... ./internal/... ./tests/unit/...
go test -count=1 ./test/integration/... -tags=integration
# or
make test-unit      # pkg + internal only — add ./tests/unit/... for parity with test.bat
make test-integration
```

More detail (debugging, Cursor launch configs): **[TESTING.md](TESTING.md)**.

---

## Configuration

Default config path: **`config/pgrollback.yaml`** (or first CLI argument, or `pgrollback_CONFIG` env).

Main blocks:

- **`postgres`** — Real server: `host`, `port`, `database`, `user`, `password`, `session_timeout`, …
- **`proxy`** — Listen address: `listen_host`, `listen_port`, timeouts, keepalive.
- **`logging`** — `level`, optional `file`.
- **`test`** — Defaults used by tests/tools: `schema`, timeouts, etc.

Clients connect to **`proxy.listen_*`**; the proxy connects upstream using **`postgres.*`**.

---

## Transaction mapping

- **`BEGIN`** — Ensures a base transaction, then **`SAVEPOINT pgrollback_v_N`** (N increases for nested logical transactions).
- **`COMMIT`** — **`RELEASE SAVEPOINT pgrollback_v_N`**; the base transaction is **never** committed by the app.
- **`ROLLBACK`** (plain, not `ROLLBACK TO SAVEPOINT`) — **`ROLLBACK TO SAVEPOINT`** + **`RELEASE SAVEPOINT`** for the current user savepoint, or no-op at level 0.

User-defined **`SAVEPOINT` / `RELEASE` / `ROLLBACK TO SAVEPOINT`** are passed through with guarding so failures do not abort the whole session transaction.

```mermaid
flowchart LR
  subgraph app [Application]
    A[BEGIN]
    B[COMMIT]
    C[ROLLBACK]
  end
  subgraph proxy [pgrollback]
    A2[SAVEPOINT pgrollback_v_N]
    B2[RELEASE SAVEPOINT]
    C2[ROLLBACK TO + RELEASE]
  end
  subgraph db [PostgreSQL]
    TX[Long-lived transaction]
  end
  A --> A2 --> TX
  B --> B2 --> TX
  C --> C2 --> TX
```

---

## Language examples

**Python**

```python
import psycopg
conn = psycopg.connect(
    host="localhost", port=6432, dbname="mydb",
    application_name="pgrollback_test1",
)
conn.execute("INSERT INTO users VALUES (1)")
```

**Go**

```go
connStr := "host=localhost port=6432 user=postgres dbname=mydb application_name=pgrollback_test1"
db, _ := sql.Open("postgres", connStr)
db.Exec("INSERT INTO users VALUES (1)")
```

**Node.js**

```js
const client = new Client({
  host: 'localhost', port: 6432, application_name: 'pgrollback_test1',
})
```

**PHP**

```php
$pdo = new PDO("pgsql:host=localhost;port=6432;dbname=mydb");
$pdo->exec("SET application_name='pgrollback_test1'");
```

---

## SQL log GUI

The proxy can serve a small web UI (same listen port) to inspect queries:

![GUI for pgrollback logs](doc/log_sql_commands.png)

---

## CI sketch

```bash
pgrollback --config config.yaml &
run migrations
run tests
pgrollback rollback   # optional explicit reset
kill pgrollback
```

---

## Safety

Use **only on test databases**. Do not point production traffic here—the proxy keeps long transactions open.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
