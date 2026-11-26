"""
DAG: load_3k_conversations_to_postgres
Project: DHAP-34 (3K Conversations Dataset for ChatBot)
Author: Sakshi
Description:
  Automated ETL pipeline to load and validate 3K Conversations dataset
  from local CSV into PostgreSQL running in Docker.

Workflow:
  read_csv -> validate_schema -> transform -> load_to_postgres
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Paths inside Docker container (configured via docker-compose volume mounts)
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "/opt/airflow/data/conversation.csv")
STAGING_PARQUET_PATH = "/tmp/transformed_conversations.parquet"
DDL_FILE_PATH = "/opt/airflow/ddl.sql"
POSTGRES_CONN_ID = "postgres_default"
TARGET_TABLE = "conversations"

# Expected schema constraints verified during inspection
EXPECTED_COLUMNS = ["Unnamed: 0", "question", "answer"]
EXPECTED_ROW_COUNT = 3725

DEFAULT_ARGS = {
    "owner": "Sakshi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="load_3k_conversations_to_postgres",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline for DHAP-34: Read CSV, validate schema, clean, and load into PostgreSQL",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dhap-34", "intern-project", "etl", "postgres"],
)
def load_3k_conversations_pipeline():
    """
    Pipeline DAG implementing:
    1. read_csv
    2. validate_schema
    3. transform
    4. load_to_postgres
    """

    @task(task_id="read_csv")
    def read_csv() -> dict:
        """
        Task 1: Read the CSV file into memory, verify existence,
        and return basic dataset metadata for validation.
        """
        logging.info("Reading CSV dataset from path: %s", CSV_FILE_PATH)
        if not os.path.exists(CSV_FILE_PATH):
            raise AirflowFailException(f"CSV file not found at expected container path: {CSV_FILE_PATH}")

        # Read CSV with pandas
        df = pd.read_csv(CSV_FILE_PATH)
        row_count, col_count = df.shape
        logging.info("Successfully read CSV. Rows: %d, Columns: %d", row_count, col_count)
        logging.info("Detected columns: %s", df.columns.tolist())

        return {
            "file_path": CSV_FILE_PATH,
            "row_count": row_count,
            "column_count": col_count,
            "columns": df.columns.tolist(),
        }

    @task(task_id="validate_schema")
    def validate_schema(csv_meta: dict) -> dict:
        """
        Task 2: Validate dataset integrity and schema compliance.
        Checks:
        - Expected columns are present
        - No unexpected or missing columns
        - Non-empty row count
        - No null/missing values in required columns
        - Monotonic uniqueness of primary key candidate ('Unnamed: 0')
        - Fail pipeline immediately if any check fails.
        """
        logging.info("Starting schema and integrity validation for dataset...")
        file_path = csv_meta.get("file_path", CSV_FILE_PATH)
        df = pd.read_csv(file_path)

        # 1. Validate Columns
        actual_cols = df.columns.tolist()
        missing_cols = [col for col in EXPECTED_COLUMNS if col not in actual_cols]
        if missing_cols:
            raise AirflowFailException(
                f"Schema Validation Failed: Missing required column(s): {missing_cols}. Actual columns: {actual_cols}"
            )

        unexpected_cols = [col for col in actual_cols if col not in EXPECTED_COLUMNS]
        if unexpected_cols:
            raise AirflowFailException(
                f"Schema Validation Failed: Unexpected column(s) found: {unexpected_cols}"
            )

        # 2. Validate Row Count
        if len(df) == 0:
            raise AirflowFailException("Schema Validation Failed: CSV file is empty.")

        if len(df) != EXPECTED_ROW_COUNT:
            logging.warning(
                "Row count differs from expected benchmark (%d vs %d). Proceeding with current rows: %d",
                len(df),
                EXPECTED_ROW_COUNT,
                len(df),
            )

        # 3. Validate Null Values
        null_counts = df.isnull().sum()
        for col in EXPECTED_COLUMNS:
            col_nulls = int(null_counts[col])
            if col_nulls > 0:
                raise AirflowFailException(
                    f"Schema Validation Failed: Column '{col}' contains {col_nulls} null values. Nulls are not allowed."
                )

        # 4. Validate Primary Key Candidate ('Unnamed: 0')
        pk_col = "Unnamed: 0"
        if not pd.api.types.is_integer_dtype(df[pk_col]):
            raise AirflowFailException(
                f"Schema Validation Failed: Primary key '{pk_col}' must be integer type. Found: {df[pk_col].dtype}"
            )

        if df[pk_col].duplicated().any():
            duplicate_count = df[pk_col].duplicated().sum()
            raise AirflowFailException(
                f"Schema Validation Failed: Primary key '{pk_col}' has {duplicate_count} duplicate values."
            )

        # 5. Validate Non-empty String content in dialogue columns
        for text_col in ["question", "answer"]:
            empty_count = (df[text_col].astype(str).str.strip() == "").sum()
            if empty_count > 0:
                raise AirflowFailException(
                    f"Schema Validation Failed: Column '{text_col}' contains {empty_count} blank/whitespace-only records."
                )

        logging.info("Schema validation passed successfully with 0 errors!")
        return {"status": "validated", "row_count": len(df)}

    @task(task_id="transform")
    def transform(validation_result: dict) -> str:
        """
        Task 3: Clean and transform the dataset.
        Transformations:
        - Rename 'Unnamed: 0' -> 'conversation_id'
        - Enforce integer type on 'conversation_id'
        - Strip leading and trailing whitespace from 'question' and 'answer'
        - Ensure string types on dialogue text
        - Save cleaned data to intermediate staging Parquet file for efficient atomic loading
        """
        logging.info("Starting data transformation...")
        df = pd.read_csv(CSV_FILE_PATH)

        # 1. Rename column
        df.rename(columns={"Unnamed: 0": "conversation_id"}, inplace=True)
        logging.info("Renamed 'Unnamed: 0' -> 'conversation_id'")

        # 2. Enforce primary key type
        df["conversation_id"] = df["conversation_id"].astype(int)

        # 3. Clean string columns
        df["question"] = df["question"].astype(str).str.strip()
        df["answer"] = df["answer"].astype(str).str.strip()
        logging.info("Trimmed leading/trailing whitespace from 'question' and 'answer'")

        # 4. Save to intermediate staging path
        os.makedirs(os.path.dirname(STAGING_PARQUET_PATH), exist_ok=True)
        df.to_parquet(STAGING_PARQUET_PATH, index=False)
        logging.info(
            "Transformed dataset written to staging path: %s (Rows: %d)",
            STAGING_PARQUET_PATH,
            len(df),
        )

        return STAGING_PARQUET_PATH

    @task(task_id="load_to_postgres")
    def load_to_postgres(staging_path: str) -> dict:
        """
        Task 4: Load transformed records into PostgreSQL table.
        Steps:
        - Connect using PostgresHook(postgres_conn_id='postgres_default')
        - Execute DDL to ensure table exists
        - Execute safe TRUNCATE (idempotent reload)
        - Bulk insert transformed rows
        - Verify row count matches staging data
        """
        logging.info("Connecting to PostgreSQL via PostgresHook ('%s')...", POSTGRES_CONN_ID)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # 1. Ensure table exists using DDL
            if os.path.exists(DDL_FILE_PATH):
                logging.info("Executing DDL from %s...", DDL_FILE_PATH)
                with open(DDL_FILE_PATH, "r", encoding="utf-8") as f:
                    ddl_sql = f.read()
                cursor.execute(ddl_sql)
            else:
                logging.info("DDL file not found at %s. Creating table directly...", DDL_FILE_PATH)
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                        conversation_id INTEGER PRIMARY KEY,
                        question TEXT NOT NULL,
                        answer TEXT NOT NULL
                    );
                    """
                )
            conn.commit()

            # 2. Read transformed data from staging
            df = pd.read_parquet(staging_path)
            record_count = len(df)
            logging.info("Loaded %d records from staging parquet.", record_count)

            # 3. Safe Truncate for idempotency
            logging.info("Truncating target table '%s' for idempotent reload...", TARGET_TABLE)
            cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE};")
            conn.commit()

            # 4. Bulk Insert
            logging.info("Inserting %d records into '%s'...", record_count, TARGET_TABLE)
            records = [
                (int(row["conversation_id"]), str(row["question"]), str(row["answer"]))
                for _, row in df.iterrows()
            ]

            insert_sql = f"""
                INSERT INTO {TARGET_TABLE} (conversation_id, question, answer)
                VALUES (%s, %s, %s);
            """
            cursor.executemany(insert_sql, records)
            conn.commit()

            # 5. Verify row count
            cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE};")
            loaded_count = cursor.fetchone()[0]
            logging.info("PostgreSQL table '%s' verified row count: %d", TARGET_TABLE, loaded_count)

            if loaded_count != record_count:
                raise AirflowFailException(
                    f"Row count mismatch after load! Staging: {record_count}, Table: {loaded_count}"
                )

            return {
                "status": "success",
                "table": TARGET_TABLE,
                "loaded_rows": loaded_count,
            }

        except Exception as exc:
            conn.rollback()
            logging.error("Failed to load records into PostgreSQL: %s", exc)
            raise
        finally:
            cursor.close()
            conn.close()

    # Define task dependencies
    csv_metadata = read_csv()
    validation_status = validate_schema(csv_metadata)
    staging_file = transform(validation_status)
    load_result = load_to_postgres(staging_file)


# Instantiate the DAG
conversations_etl_dag = load_3k_conversations_pipeline()
