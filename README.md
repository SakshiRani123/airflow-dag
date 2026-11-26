# DHAP-34: Local CSV → PostgreSQL via Dockerized Airflow

## Overview
This project provides an automated, idempotent Data Engineering ETL pipeline that ingests a 3,725-row conversational dataset from a local CSV file, validates its schema and integrity, cleans the text records, and loads them into a local PostgreSQL database orchestrated by Apache Airflow running in Docker Compose.

```
Local CSV (conversation.csv)
       ↓
Apache Airflow DAG (load_3k_conversations_to_postgres)
       ↓
Task 1: read_csv (read dataset & extract metadata)
       ↓
Task 2: validate_schema (strict type, column & primary key checks)
       ↓
Task 3: transform (clean whitespace, cast IDs, write staging parquet)
       ↓
Task 4: load_to_postgres (idempotent truncate-and-load via PostgresHook)
       ↓
PostgreSQL Target Table (conversations)
```

---

## Dataset

- **Dataset Name:** 3K Conversations Dataset for ChatBot
- **Source File:** `conversation.csv`
- **Total Rows:** `3,725`
- **Source Columns:**
  - `Unnamed: 0` (Integer, 0 to 3724, 100% unique & sequential candidate primary key)
  - `question` (Text, prompt/inquiry with occasional leading/trailing spaces)
  - `answer` (Text, conversational response with occasional leading/trailing spaces)
- **Transformations:**
  - Column rename: `Unnamed: 0` → `conversation_id`
  - Dialogue cleaning: Trimming leading/trailing whitespace on both `question` and `answer`
  - Target table: `conversations` with `conversation_id INTEGER PRIMARY KEY`

---

## Project Structure

```
airflow-dag-configs/
├── .gitignore
└── intern-project/
    └── Sakshi/
        └── project-DHAP-34/
            └── 3k-conversations/
                ├── manifest.yaml
                ├── schema.yaml
                ├── ddl.sql
                ├── docker-compose.yaml
                ├── .env.example
                ├── README.md
                ├── data/
                │   └── conversation.csv
                └── dags/
                    └── load_3k_conversations_to_postgres.py
```

---

## Prerequisites

Before running the project, ensure you have:
1. **Docker Desktop** installed and running (with Linux container support enabled).
2. **Docker Compose v2** (included with modern Docker Desktop).
3. **Git** and **VS Code** (or terminal of choice).
4. Sufficient system memory allocated to Docker (recommended: at least 4 GB RAM).

---

## Configuration

The project uses environment variables to isolate credentials and ensure no passwords or secrets are hardcoded in the codebase.

1. Navigate to the project directory:
   ```bash
   cd intern-project/Sakshi/project-DHAP-34/3k-conversations
   ```

2. Copy `.env.example` to create your local `.env`:
   ```bash
   cp .env.example .env
   ```
   *(On Windows PowerShell: `Copy-Item .env.example .env`)*

3. Verify or edit `.env` parameters if needed:
   ```ini
   AIRFLOW_UID=50000
   POSTGRES_DB=conversations_db
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=change_me
   POSTGRES_PORT=5432
   _AIRFLOW_WWW_USER_USERNAME=airflow
   _AIRFLOW_WWW_USER_PASSWORD=airflow
   ```

> **Note:** `.env` is ignored by Git in `.gitignore` and must never be committed.

---

## How Airflow Connects to PostgreSQL

Inside the Docker network:
- Airflow connects to PostgreSQL using the Docker Compose service name `postgres` (not `localhost`).
- The connection is automatically pre-configured for Airflow using the environment variable:
  ```ini
  AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
  ```
- In the DAG, `PostgresHook(postgres_conn_id='postgres_default')` retrieves these credentials securely without any hardcoded passwords.

---

## Start the Project

1. Start Docker Desktop on your machine.
2. From the project directory, run:
   ```bash
   docker compose up -d
   ```
3. Check container status:
   ```bash
   docker compose ps
   ```
   You should see:
   - `dhap34_postgres` (healthy)
   - `dhap34_airflow_init` (completed successfully)
   - `dhap34_airflow_webserver` (running/healthy on port 8080)
   - `dhap34_airflow_scheduler` (running/healthy)

---

## Airflow UI

1. Open your web browser and go to:
   [http://localhost:8080](http://localhost:8080)
2. Log in with the default credentials:
   - **Username:** `airflow`
   - **Password:** `airflow`

---

## Run the DAG

1. In the Airflow DAGs list, find **`load_3k_conversations_to_postgres`**.
2. Unpause the DAG toggle (if paused).
3. Click the **Trigger DAG** button (Play icon ▶) on the top right.
4. Watch the pipeline execute sequentially:
   `read_csv` → `validate_schema` → `transform` → `load_to_postgres`
5. All tasks will turn dark green (`success`).

---

## PostgreSQL Verification

You can verify the loaded records directly inside the PostgreSQL container using `docker exec`:

1. Open a terminal and run `psql`:
   ```bash
   docker exec -it dhap34_postgres psql -U postgres -d conversations_db
   ```

2. Check row count:
   ```sql
   SELECT COUNT(*) FROM conversations;
   ```
   *Expected Output: `3725`*

3. Inspect sample records:
   ```sql
   SELECT conversation_id, question, answer FROM conversations LIMIT 5;
   ```

4. Exit psql:
   ```sql
   \q
   ```

---

## Idempotency Test

To prove that the pipeline is safe to run multiple times:
1. In the Airflow UI, trigger the DAG a second time.
2. Once the DAG completes, run:
   ```bash
   docker exec -it dhap34_postgres psql -U postgres -d conversations_db -c "SELECT COUNT(*) FROM conversations;"
   ```
3. **Result:** The row count remains exactly **`3725`** and does NOT double to `7450`. The controlled truncate-and-reload mechanism guarantees idempotency.

---

## Validation Test (Failure Case)

To verify that the schema validation task actively protects downstream systems:
1. Temporarily modify a column name or header in `data/conversation.csv` (e.g., rename `question` to `query`).
2. Trigger the DAG.
3. Observe that `validate_schema` raises an `AirflowFailException` and immediately terminates the DAG run.
4. Downstream tasks `transform` and `load_to_postgres` are skipped/prevented from running.
5. Revert the change back to restore successful runs.

---

## Troubleshooting

- **Docker not running:** Ensure Docker Desktop is started and the whale icon is visible in the taskbar.
- **Port 8080 already in use:** Change the webserver host port in `docker-compose.yaml` (e.g., `"8081:8080"`).
- **PostgreSQL connection failure:** Ensure the target service name `postgres` is used inside Docker, not `localhost`.
- **DAG not appearing:** Wait ~30 seconds for the scheduler file sync, or check scheduler logs:
  ```bash
  docker compose logs -f airflow-scheduler
  ```
- **Permission issues:** Ensure `AIRFLOW_UID=50000` is defined in `.env`.

---

## Tear Down

To stop and remove containers when finished:
```bash
docker compose down
```
To also remove the database volume:
```bash
docker compose down -v
```
