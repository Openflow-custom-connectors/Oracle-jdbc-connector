# Oracle JDBC Connector – Multi-Table (Openflow Custom Flow)

A custom Openflow (Apache NiFi) flow that loads **one or more Oracle tables into Snowflake**, including tables with **no primary key**. Each table is configured as one entry in a JSON parameter (`Table Config`), and can run either:

- **Incremental** – pulls only rows changed since the last successful load, using a watermark column, or
- **Full load** – pulls the whole table on every run (when no watermark column is configured).

Rows are upserted into the target with a dynamic `MERGE` keyed on a generated composite key (`CKEY`), and every target row carries `_INSERTED_AT` / `_UPDATED_AT` audit timestamps.

Flow definition: `Oracle JDBC Connector - MultiTable.json`

---

## 1. How it works

The flow has two independent loops, each on its own schedule. They communicate only through a Snowflake internal stage and a staging table per source table.

```
Oracle JDBC Connector - MultiTable
├── Extract Loop                      (cron: Flow Run Schedule)
│   ├── Trigger → Split Table Config → Parse → Apply Defaults → Validate
│   └── Extract Data and Load to Stage   (runs once per table)
│        Read Last Watermark → Store Start Time → Purge Table Stage
│        → Truncate Staging Table → Clone N Chunks → Query Oracle (chunked)
│        → Put to Internal Stage
└── Merge Loop                        (cron: Merge Schedule)
    ├── Trigger → Split Table Config → Parse → Apply Defaults → Validate
    └── Stage to Final Table             (runs once per table)
         Create Staging Table IF NOT EXISTS → COPY INTO Staging (PURGE)
         → Generate Dynamic Merge → MERGE INTO Target → Log Watermark
         (+ truncate staging)
```

**Per-table settings are FlowFile attributes, not parameters.** At the start of each loop, `Table Config` is split into one FlowFile per table. Each table's settings become attributes (`${source_table}`, `${watermark_column}`, …) that the downstream SQL uses. Settings shared by all tables (database, schema, stage, schedules, credentials) stay as `#{...}` parameters.

### Extract Loop (per table)
1. **Read Last Watermark** – Reads the last `SUCCESS` watermark for the table from `INCREMENTAL_WATERMARK`, defaulting to `1900-01-01 00:00:00`. On first run it also creates the `INCREMENTAL_WATERMARK` table.
2. **Store Start Time** – Builds `watermark_filter`:
   - Incremental tables: `b.<watermark_column> >= TO_TIMESTAMP('<last_watermark>', ...)`.
   - Full-load tables: `1=1`.
3. **Purge Table Stage / Truncate Staging Table** – Clears this table's leftover stage files and staging rows. This stops files piling up when merges were failing. Nothing is lost, because the watermark only advances after a successful merge, so the next pull re-reads the same rows. The stage and JSON file format are also created here if they don't exist.
4. **Clone N Chunks → Query Oracle** – Splits the table into `chunk_count` parallel slices using `MOD(ORA_HASH(ROWID), chunk_count)`. Each slice selects `b.*` plus `<ckey_expression> AS CKEY`. Failed chunks are retried (failure relationship set to retry, 10 attempts with backoff) before being routed to `Log Failure`.
5. **Put to Internal Stage** – Writes compressed JSON to `@<stage>/<source_table>/`.

### Merge Loop (per table)
1. **Create Staging Table IF NOT EXISTS** – Infers the staging table schema from the staged files. `NUMBER*` columns become `NUMBER(38,0)`.
2. **COPY INTO Staging** – Loads the files and purges them from the stage.
3. **Generate Dynamic Merge** – Builds the `MERGE` from the staging table's columns:
   - Deduplicates with `QUALIFY ROW_NUMBER() OVER (PARTITION BY CKEY ORDER BY <merge_order_column> DESC) = 1`.
   - Updates a matched row **only if its data changed** (`HASH(T.cols) <> HASH(S.cols)`), and then sets `_UPDATED_AT`.
   - Inserts new rows with `_INSERTED_AT` and `_UPDATED_AT`.
4. **MERGE INTO Target** – Creates the target table if missing (`LIKE` staging), adds the audit columns if missing, then runs the MERGE.
5. **Log Watermark** – Inserts a `SUCCESS` row into `INCREMENTAL_WATERMARK`, then truncates the staging table. The value logged is:
   - Incremental tables: `MAX(<watermark_column>)` from the target.
   - Full-load tables: the load time.

### Objects created automatically (in `Destination Database.Destination Schema`)
| Object | Name |
|---|---|
| Internal stage | `#{Snowflake Internal Stage}` (files under `/<source_table>/`) |
| File format | `OPENFLOW_JSON_FORMAT` |
| Watermark / run-log table | `INCREMENTAL_WATERMARK` |
| Staging table (per source table) | `staging_table` (default `<target_table>_STG`) |
| Target table (per source table) | `target_table` (default `<source_table>`), plus `CKEY`, `_INSERTED_AT`, `_UPDATED_AT` |

The **database and schema are not created by the flow.** See [Prerequisites](#2-prerequisites).

---

## 2. Prerequisites

### 2.1 Openflow runtime
- An Openflow runtime where the flow is imported.
- **Snowflake deployment (SPCS):** an External Access Integration that allows the Oracle host and port, attached to the runtime.
- The Oracle JDBC driver JARs available to the runtime (see `Oracle JDBC Driver` parameter).

### 2.2 Snowflake database, schema and permissions
The flow runs as the role in `Snowflake Role`, referred to below as `<RUNTIME_ROLE>`. That role must be able to use the warehouse and create objects in the destination schema.

**Recommended:** create the schema **as `<RUNTIME_ROLE>`** so it owns the schema and everything the flow creates in it.

```sql
-- 1. One-time, as an admin: database + grants to the runtime role
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS <DEST_DB> COMMENT = 'Oracle JDBC multi-table landing. [openflow]';
GRANT USAGE, CREATE SCHEMA ON DATABASE <DEST_DB> TO ROLE <RUNTIME_ROLE>;
GRANT USAGE ON WAREHOUSE <WAREHOUSE> TO ROLE <RUNTIME_ROLE>;

-- 2. As the runtime role: schema (owned by the runtime role)
USE ROLE <RUNTIME_ROLE>;
CREATE SCHEMA IF NOT EXISTS <DEST_DB>.<DEST_SCHEMA> COMMENT = 'Oracle JDBC multi-table landing schema. [openflow]';
```

If the schema already exists and is owned by another role (e.g. `ACCOUNTADMIN`), use **one** of:

```sql
-- Option A: transfer ownership to the runtime role (simplest)
GRANT OWNERSHIP ON SCHEMA <DEST_DB>.<DEST_SCHEMA> TO ROLE <RUNTIME_ROLE> COPY CURRENT GRANTS;

-- Option B: keep ownership, grant what the flow needs
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
  ON SCHEMA <DEST_DB>.<DEST_SCHEMA> TO ROLE <RUNTIME_ROLE>;
-- With Option B, pre-existing target/staging tables also need:
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA <DEST_DB>.<DEST_SCHEMA> TO ROLE <RUNTIME_ROLE>;
```

> A schema that exists but that the role can't access fails with the same error as a missing one:
> `Schema '<DB>.<SCHEMA>' does not exist or not authorized`. Check ownership with `SHOW SCHEMAS IN DATABASE <DEST_DB>`.

### 2.3 Oracle
- An Oracle user (`Oracle Username`) with `SELECT` on every table listed in `Table Config`.
- Network reachability from the runtime to the Oracle listener.
- For incremental tables, a column that reliably increases when a row is inserted or updated (e.g. `LAST_UPDATE_DATE`), ideally indexed for large tables.

---

## 3. Parameters

Parameters are organised in three contexts. The flow's process group is bound to the table-level context, which **inherits** the other two.

### 3.1 Table-level context (bound to the process group, e.g. `ORACLE JDBC Parameters`)

| Parameter | Description | Example / possible values |
|---|---|---|
| `Table Config` | JSON array, one object per table. See [Section 4](#4-table-config). | `[{"source_schema":"CO","source_table":"ORDERS","watermark_column":"ORDER_TMS"}]` |
| `Destination Database` | Snowflake database for stage, staging, target and watermark tables. Must already exist. Overrides the inherited destination value. | `ORACLE_JDBC_CONNECTOR` |
| `Destination Schema` | Snowflake schema for all objects. Must already exist and be usable by the runtime role. | `ORACLE_CO` |
| `Snowflake Internal Stage` | Name of the internal stage used for landing files. Created automatically. | `OPENFLOW_LANDING_STAGE` |
| `Chunk Count` | Default number of parallel Oracle slices for tables that don't set `chunk_count`. | `50` (initial load), `5`–`10` (incremental) |
| `Flow Run Schedule` | Quartz cron for the **Extract Loop**. | `0 0 2 * * ?` (daily 02:00) |
| `Merge Schedule` | Quartz cron for the **Merge Loop**. | `0 */5 * * * ?` (every 5 min) |

> Cron fields are Quartz format: `second minute hour day-of-month month day-of-week`. The scheduler uses the runtime's time zone, typically UTC.

### 3.2 Oracle Source Parameters (inherited)

| Parameter | Description | Example |
|---|---|---|
| `Oracle Connection URL` | JDBC thin URL to the database / PDB. | `jdbc:oracle:thin:@//host:1521/SERVICE` |
| `Oracle Username` | Oracle user with `SELECT` on the source tables. | `c##connectuser` |
| `Oracle Password` | Password for that user (sensitive). | — |
| `Oracle JDBC Driver` | Comma-separated driver JAR paths on the runtime. | `/nifi/extensions/native/ojdbc11.jar, ...` |
| `Oracle SSL Mode` / `Oracle Wallet Filename` | TLS settings for the thin driver, if used. | `DISABLED` |

`XStream Out Server Name` / `XStream Out Server URL` exist in the shared context for the Oracle CDC connector. **This flow does not use them.**

### 3.3 Oracle Destination Parameters (inherited)

| Parameter | Description | Example |
|---|---|---|
| `Snowflake Authentication Strategy` | `SNOWFLAKE_MANAGED` (runtime token) or `KEY_PAIR`. | `SNOWFLAKE_MANAGED` |
| `Snowflake Role` | Role used for all Snowflake SQL. Needs the grants in [Section 2.2](#22-snowflake-database-schema-and-permissions). | `OPENFLOW_ADMIN` |
| `Snowflake Warehouse` | Warehouse for COPY / MERGE. | `OPENFLOW_INGEST_WAREHOUSE` |
| `Snowflake Connection Strategy` | `STANDARD` or `PRIVATE_CONNECTIVITY`. | `STANDARD` |
| `Snowflake Account Identifier`, `Snowflake Username`, `Snowflake Private Key*` | Only needed for `KEY_PAIR` auth. | — |

The shared context also contains `Destination Schema Pattern`, `Table Storage Format`, `Iceberg Version`, `Error Handling Strategy` and `Oversized Value Strategy`, which are used by the Oracle CDC connector. **This flow does not use them.** Its `Destination Database` is overridden by the table-level context.

---

## 4. Table Config

`Table Config` is a JSON **array**. Each object describes one source table. Tables are processed in array order (FIFO), each in its own chunks.

### 4.1 Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `source_schema` | **Yes** | — | Oracle schema (owner) of the table. |
| `source_table` | **Yes** | — | Oracle table name. Also used as the stage sub-folder and as the key in `INCREMENTAL_WATERMARK`. |
| `watermark_column` | No | *(none, so full load)* | Timestamp/date column used for incremental pulls. **If omitted, the table is fully reloaded on every run.** |
| `ckey_expression` | No | `ROWID` | Oracle SQL expression that uniquely identifies a row. Becomes the `CKEY` merge key. See [4.3](#43-choosing-ckey_expression). |
| `target_table` | No | `source_table` | Snowflake target table name. |
| `staging_table` | No | `<target_table>_STG` | Snowflake staging table name. |
| `chunk_count` | No | `#{Chunk Count}` | Number of parallel Oracle slices for this table. |
| `merge_order_column` | No | `LAST_UPDATE_DATE` if a watermark is set, otherwise `CKEY` | Column used to pick the latest version when the same `CKEY` appears more than once in staging. |

### 4.2 Validation
- An entry missing `source_schema` or `source_table` is routed to **Log Failure** and skipped. Other tables still run.
- Invalid JSON in `Table Config` fails the split step, and nothing runs for that cycle.
- Field names are case-sensitive and lowercase.
- Use uppercase table and column names, matching Oracle's data dictionary.

### 4.3 Choosing `ckey_expression`
| Situation | Recommended |
|---|---|
| Table has a primary / unique key | The key column, e.g. `"ORDER_ID"`, or concatenate several: `LEDGER_ID \|\| CHR(167) \|\| CODE_COMBINATION_ID` |
| Composite key with nullable columns | Use a NULL-safe sentinel: `NVL(TO_CHAR(COL_A), CHR(167)) \|\| CHR(167) \|\| NVL(TO_CHAR(COL_B), CHR(167))` |
| No key at all | `ROWID` (default) |

**Caution with `ROWID`:** an Oracle `ROWID` changes when the table is moved, shrunk or rebuilt, or exported and imported, or when a row moves between partitions. The next pull then inserts those rows again under new CKEYs, which creates duplicates in Snowflake. It's acceptable for append-mostly tables. Avoid it for long-lived tables that are updated in place, especially full-load tables.

**Don't use `$` or `#{` inside `ckey_expression`.** The flow would try to evaluate them as NiFi expressions or parameters.

### 4.4 Load modes
| Mode | When | Behaviour |
|---|---|---|
| **Incremental** | `watermark_column` set | `WHERE <watermark_column> >= <last successful watermark>`. The watermark is `MAX(<watermark_column>)` from the target after each successful merge. |
| **Full load** | `watermark_column` omitted | `WHERE 1=1`: all rows every run, upserted via MERGE. Unchanged rows aren't updated, because of the hash comparison. |

**Deletes are not propagated in either mode.** Rows deleted in Oracle stay in Snowflake.

### 4.5 Examples
```json
[
  {
    "source_schema": "CO",
    "source_table": "ORDERS",
    "watermark_column": "ORDER_TMS",
    "ckey_expression": "ORDER_ID",
    "chunk_count": 10,
    "merge_order_column": "ORDER_TMS"
  },
  {
    "source_schema": "CO",
    "source_table": "CUSTOMERS",
    "ckey_expression": "CUSTOMER_ID",
    "chunk_count": 2
  },
  {
    "source_schema": "ONT",
    "source_table": "OE_ORDER_LINES_HISTORY",
    "watermark_column": "HIST_CREATION_DATE",
    "target_table": "OE_ORDER_LINES_HISTORY",
    "staging_table": "OE_ORDER_LINES_HISTORY_STG",
    "chunk_count": 50
  }
]
```
- `ORDERS`: incremental on `ORDER_TMS`, keyed on `ORDER_ID`.
- `CUSTOMERS`: full load, keyed on `CUSTOMER_ID`.
- `OE_ORDER_LINES_HISTORY`: incremental, keyed on `ROWID` (default), with explicit target and staging names.

---

## 5. Scheduling

### 5.1 Recommended schedule
| Loop | Parameter | Recommended | Why |
|---|---|---|---|
| Extract | `Flow Run Schedule` | Once per day, off-peak for Oracle, e.g. `0 0 2 * * ?` | Puts the read load on the source at a quiet time. |
| Merge | `Merge Schedule` | Every 5–30 min, **skipping the extract hour**, e.g. `0 */5 0-1,3-23 * * ?` | Files merge shortly after they land, and the merge doesn't overlap the extract's cleanup. |

**Why skip the extract hour?** At the start of each table, the extract purges that table's stage files and truncates its staging table. If a merge runs at that moment it may merge an empty or partial staging table. **This does not lose data:** the watermark isn't advanced past unmerged rows, and the extract re-pulls them. It does waste a cycle. Excluding the extract hour from `Merge Schedule` avoids this. If the extract takes longer than an hour, exclude more hours.

### 5.2 Sizing guidance
| Scenario | `chunk_count` | `Query Oracle…` concurrent tasks | Warehouse |
|---|---|---|---|
| Initial / historical load of a large table (100M+ rows) | 50 | 5–8 | Large |
| Daily incremental | 5–10 | 3–5 | Small / X-Small |
| Small full-load tables (< 1M rows) | 1–2 | — | Small |

- Total Oracle sessions are capped by the `Oracle Connection Pool` controller service (`Max Total Connections`, currently 5). Raise it together with the `Query Oracle with CKEY (Chunked)` concurrent tasks setting.
- For the first load of a very large table, run it alone, either by keeping only that table in `Table Config` or by triggering it with **Run Once**, before adding it to the daily schedule.

### 5.3 Behaviour with several tables
- Every cycle of each loop processes **all** tables in `Table Config`.
- Tables are queued first-in-first-out. A table's chunks are queued ahead of the next table's, but when threads are free the next table can start before the previous one's last chunks finish.
- One table failing doesn't stop the others.

### 5.4 Ad-hoc runs
Right-click a loop's **Trigger (Scheduled)** processor and choose **Run Once** to run that loop immediately, for all tables, without changing the schedule.

---

## 6. Monitoring

### Run history
```sql
SELECT TABLE_NAME, WATERMARK_VALUE, STATUS, ROWS_LOADED, RUN_TS
FROM <DEST_DB>.<DEST_SCHEMA>.INCREMENTAL_WATERMARK
ORDER BY RUN_TS DESC
LIMIT 50;
```
Every merge cycle writes one `SUCCESS` row per table, including cycles with no new data, so this table grows steadily. Clean it up periodically if needed, keeping the most recent `SUCCESS` row per table:
```sql
DELETE FROM <DEST_DB>.<DEST_SCHEMA>.INCREMENTAL_WATERMARK
WHERE RUN_TS < DATEADD(day, -30, SYSDATE())
  AND ID NOT IN (SELECT MAX(ID) FROM <DEST_DB>.<DEST_SCHEMA>.INCREMENTAL_WATERMARK
                 WHERE STATUS = 'SUCCESS' GROUP BY TABLE_NAME);
```

### Audit columns on target tables
| Column | Meaning |
|---|---|
| `CKEY` | Generated merge key (`ckey_expression`). |
| `_INSERTED_AT` | UTC time the row was first inserted by the flow. |
| `_UPDATED_AT` | UTC time the row's data last changed. Not updated when a re-pulled row is unchanged. |

Rows loaded before the audit columns were added have `NULL` in both columns.

### Failures
All failure paths go to **Log Failure** (`LogMessage` at `ERROR`): `Oracle Incremental Pull FAILED for <source_table>. Error: <message>`. These appear as bulletins on the canvas and in the runtime's event table.

**Expected noise:** on a table's very first merge cycle, before its extract has staged any files, **Create Staging Table IF NOT EXISTS** fails because there's nothing to infer a schema from. This stops once the first extract has run.

---

## 7. Operations

### Add a table
1. Grant the Oracle user `SELECT` on the table.
2. Add an object to `Table Config`.
3. Run the Extract Loop with **Run Once**, or wait for the schedule. Target and staging tables are created automatically.

### Remove a table
Remove its entry from `Table Config`. Nothing is dropped. The target table, staging table and `INCREMENTAL_WATERMARK` history remain until you drop them manually.

### Reload a table from scratch
1. Delete its watermark rows: `DELETE FROM <DEST_DB>.<DEST_SCHEMA>.INCREMENTAL_WATERMARK WHERE TABLE_NAME = '<source_table>';`
2. Optionally, truncate or drop the target table.
3. Run the Extract Loop. The pull restarts from `1900-01-01`.

### Change `ckey_expression` for an existing table
Existing target rows keep their old `CKEY`, so new rows wouldn't match them. Drop or truncate the target table and reload the table from scratch (see above).

---

## 8. Running multiple copies of the flow

Within one copy, add tables to `Table Config` rather than copying the flow. Use separate copies only when tables need different schedules, destination schemas or source databases.

Canvas copy/paste keeps the **same parameter context** as the original. To give a copy its own table-level context:
- **File import:** rename the table-level context in the JSON before importing. Change it in `parameterContexts` (both the key and `name`) and in every `parameterContextName`. Leave `Oracle Source Parameters` and `Oracle Destination Parameters` unchanged so credentials stay shared.
- **Registry (Git) import:** untick **Keep Existing Parameter Contexts**. NiFi then creates new contexts, including copies of the shared ones, so you have to re-enter sensitive values or point the new table context back at the shared contexts.

Two copies must not load the **same `source_table` into the same destination schema**. They would share the stage folder, the staging table and the watermark rows.

---

## 9. Known limitations
- **No delete propagation.** Rows deleted in Oracle remain in Snowflake.
- **A failed chunk can be skipped.** If one Oracle chunk still fails after retries while the others succeed, the watermark can advance past that chunk's rows, so the next incremental run won't re-read them. Watch for `Log Failure` errors. To recover, reload the table (Section 7), or add an overlap window to **Read Last Watermark** (e.g. `DATEADD(hour, -24, …)`).
- **Watermark resolution.** The pull uses `>=` on the last watermark, so rows sharing the boundary timestamp are re-read and deduplicated by the MERGE.
- **Schema changes.** New Oracle columns are added to the staging table only when it's recreated. The target table is not altered automatically.
- **Column types.** Staging types are inferred from JSON. All `NUMBER*` columns become `NUMBER(38,0)`, so decimal values may need an explicit target table definition.
