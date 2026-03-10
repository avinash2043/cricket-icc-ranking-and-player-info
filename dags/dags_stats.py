from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
import logging
import json

# -----------------------------
# Configuration
# -----------------------------
api_key = '2bf346da4emsh2cb38cccb65d058p1fb351jsn7f0d973e1c64'#'483158d05amshaa335f152d9b44dp182c45jsnf95bb3f15a15'
PROJECT_ID = "project-2b297edf-113a-4397-829"
SOURCE_DATASET = "icc_ranking"
SOURCE_TABLE = "rankings"
TARGET_DATASET = "players_data"


headers = {
    "x-rapidapi-key": api_key,
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

bq_client = bigquery.Client(project=PROJECT_ID)

# -----------------------------
# Functions
# -----------------------------

'''
SELECT *
FROM `project-2b297edf-113a-4397-829.icc_ranking.rankings`
WHERE DATE(update_timestamp) = CURRENT_DATE();
'''
def fetch_player_ids(ti):
    """Fetch distinct player IDs from BigQuery."""
    try:
        query = f"""
            SELECT DISTINCT player_id
            FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
            WHERE lastUpdatedOn = (
                SELECT MAX(lastUpdatedOn)
                FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
            )
        """
        df = bq_client.query(query).to_dataframe()
        player_ids = df["player_id"].tolist()
        logging.info(f"Found {len(player_ids)} player_ids")
        ti.xcom_push(key="player_ids", value=player_ids)
    except Exception as e:
        logging.error(f"❌ Failed to fetch player IDs: {e}")
        raise

def fetch_stats(ti, **kwargs):
    """Fetch stats for all players and load to BigQuery."""
    stat_type = kwargs['stat_type']
    TARGET_TABLE = kwargs['TARGET_TABLE']
    now = datetime.now()
    try:
        player_ids = ti.xcom_pull(task_ids="get_player_ids", key="player_ids")
        if not player_ids:
            raise ValueError("No player IDs received from XCom")

        all_data = []
        for player_id in player_ids:
            try:
                url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/{stat_type}"
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()  # HTTP errors

                # JSON parsing
                try:
                    data = response.json()
                except (json.JSONDecodeError, ValueError) as je:
                    logging.error(f"❌ JSON decode error for player_id {player_id}: {je}")
                    continue

                if "values" not in data or "headers" not in data:
                    logging.warning(f"No valid data for player {player_id}, stat_type {stat_type}")
                    continue

                df = pd.DataFrame(
                    [row["values"] for row in data["values"]],
                    columns=data["headers"]
                )

                dft = (
                    df.set_index("ROWHEADER")
                      .T
                      .reset_index()
                      .rename(columns={"index": "Format"})
                )
                dft.insert(0, "player_id", player_id)
                dft.insert(1, "stat_type", stat_type)
                dft["insert_timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
                all_data.append(dft)

            except requests.exceptions.RequestException as re:
                logging.error(f"❌ HTTP error for player_id {player_id}: {re}")
            except Exception as e:
                logging.error(f"❌ Error processing player_id {player_id}: {e}")

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            load_to_bigquery(final_df, TARGET_TABLE)
        else:
            logging.warning(f"No data fetched for stat_type: {stat_type}")

    except Exception as e:
        logging.error(f"❌ Failed to fetch stats for stat_type {stat_type}: {e}")
        raise

def load_to_bigquery(final_df, TARGET_TABLE):
    """Load dataframe into BigQuery."""
    table_id = f"{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}"
    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = bq_client.load_table_from_dataframe(final_df, table_id, job_config=job_config)
        job.result()
        logging.info(f"✅ Loaded {job.output_rows} rows into {TARGET_TABLE}")
    except Exception as e:
        logging.error(f"❌ Failed to load data into BigQuery table {TARGET_TABLE}: {e}")
        raise

# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'depends_on_past': False,
    'email': ['avinashkeerthi20@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='player_stats',
    default_args=default_args,
    description='Fetching player stats from Cricbuzz and loading to BigQuery',
    schedule_interval=None,
    catchup=False
) as dag:

    get_player_ids = PythonOperator(
        task_id='get_player_ids',
        python_callable=fetch_player_ids
    )

    get_batting_stats = PythonOperator(
        task_id='get_batting_stats',
        python_callable=fetch_stats,
        op_kwargs={
            'stat_type': 'batting',
            'TARGET_TABLE': 'batting_stats_stg'
        }
    )

    get_bowling_stats = PythonOperator(
        task_id='get_bowling_stats',
        python_callable=fetch_stats,
        op_kwargs={
            'stat_type': 'bowling',
            'TARGET_TABLE': 'bowling_stats'
        }
    )

    get_player_ids >> [get_batting_stats, get_bowling_stats]