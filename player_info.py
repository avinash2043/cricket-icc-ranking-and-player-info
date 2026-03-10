from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging
import requests
import pandas as pd


# -----------------------------
# CONFIG
# -----------------------------
PROJECT_ID = "project-2b297edf-113a-4397-829"
DATASET_ID = "players_data"
TABLE_ID = "players_info"

api_key = '483158d05amshaa335f152d9b44dp182c45jsnf95bb3f15a15'#'2bf346da4emsh2cb38cccb65d058p1fb351jsn7f0d973e1c64'#

headers = {
    "x-rapidapi-key": api_key,
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}


# -----------------------------
# TASK 1: Find Missing Player IDs
# -----------------------------
def find_missing_player_ids(ti, **context):

    player_ids = ti.xcom_pull(
        dag_id="player_stats",
        task_ids="get_player_ids",
        key="player_ids",
        include_prior_dates=True,
    )

    if not player_ids:
        logging.info("❗ No player_ids found in XCom")
        return []

    logging.info(f"📥 Pulled player_ids: {player_ids}")

    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT player_id
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE player_id IN UNNEST(@player_ids)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("player_ids", "INT64", player_ids)
        ]
    )

    rows = client.query(query, job_config=job_config).result()
    existing_ids = {row.player_id for row in rows}
    missing_ids = list(set(player_ids) - existing_ids)

    logging.info(f"✔ Existing IDs: {existing_ids}")
    logging.info(f"❓ Missing IDs: {missing_ids}")

    return missing_ids


# -----------------------------
# TASK 2: Fetch Player Info from API
# -----------------------------
def fetch_player_info(ti, **context):

    missing_ids = ti.xcom_pull(
        task_ids="find_missing_player_ids"
    )

    if not missing_ids:
        logging.info("✅ No new players to fetch")
        return []

    all_player_info = []

    for player_id in missing_ids:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}"

        try:
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logging.error(f"❌ API failed for {player_id}: {response.text}")
                break

            data = response.json()
            dob = data.get("DoB")

            player_record = {
                "player_id": data.get("id"),
                "name": data.get("name"),
                "role": data.get("role"),
                "country": data.get("intlTeam"),
                "batting_style": data.get("bat"),
                "bowling_style": data.get("bowl"),
                "DoB": dob.split("(")[0].strip() if dob else None,
                "birthPlace": data.get("birthPlace"),
                "insert_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            all_player_info.append(player_record)

        except Exception as e:
            logging.error(f"❌ Exception while fetching {player_id}: {e}")
            continue

    logging.info(f"📦 Total records fetched: {len(all_player_info)}")
    return all_player_info


# -----------------------------
# TASK 3: Load to BigQuery
# -----------------------------
def load_to_bigquery(ti, **context):

    records = ti.xcom_pull(
        task_ids="fetch_player_info"
    )

    if not records:
        logging.info("⚠ No records to load into BigQuery")
        return

    df = pd.DataFrame(records)
    if "player_id" in df.columns:
        df["player_id"] = (
            pd.to_numeric(df["player_id"], errors="coerce")
            .astype("Int64")
        )
    if "insert_timestamp" in df.columns:
        df["insert_timestamp"] = pd.to_datetime(df["insert_timestamp"])

    if df.empty:
        logging.info("⚠ DataFrame is empty")
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        client = bigquery.Client(project=PROJECT_ID)

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )

        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )

        job.result()

        logging.info(f"✅ Loaded {job.output_rows} rows into {TABLE_ID}")

    except Exception as e:
        logging.error(f"❌ Failed to load data into BigQuery: {e}")
        raise


# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "email": ["avinashkeerthi20@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="player_info",
    default_args=default_args,
    description="Fetch player info from Cricbuzz API and load to BigQuery",
    schedule_interval=None,
    catchup=False,
) as dag:

    find_missing_player_ids_task = PythonOperator(
        task_id="find_missing_player_ids",
        python_callable=find_missing_player_ids,
    )

    fetch_player_info_task = PythonOperator(
        task_id="fetch_player_info",
        python_callable=fetch_player_info,
    )

    load_to_bigquery_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    # Task Dependencies
    find_missing_player_ids_task >> fetch_player_info_task >> load_to_bigquery_task