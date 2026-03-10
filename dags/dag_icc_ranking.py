from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from datetime import datetime,timedelta
import requests
import csv
import logging


api_key = '483158d05amshaa335f152d9b44dp182c45jsnf95bb3f15a15'
bucket_name = "us-central1-project-cricket-130a1246-bucket"
project_id = 'project-2b297edf-113a-4397-829'
dataset_id = 'icc_ranking'
csv_folder = 'icc_ranking_files'

def upload_csv_to_gcs(local_path,gcp_path):
    try:
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name = bucket_name,
            object_name = gcp_path, 
            filename = local_path 
        )
        logging.info(f"✅ Uploaded {local_path} to gs://{bucket_name}/{gcp_path}")
    except Exception as e:
        logging.error(f"❌ Failed to upload CSV to GCS: {e}")
    return None


def load_gcs_to_bigquery(gcp_path, table_name):
    """Load CSV from GCS to BigQuery"""
    gcs_uri = f"gs://{bucket_name}/{gcp_path}"
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.{table_name}_ranking"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True
        )

        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        load_job.result()
        logging.info(f"✅ Loaded rows into {table_id}")
    except Exception as e:
        logging.error(f"❌ Error loading data into BigQuery: {e}")
    return None

def run_bq_stored_procedure(**kwargs):
    sp_name = kwargs["procedure"]
    client = bigquery.Client(project=project_id)
    sp_query = f"CALL `project-2b297edf-113a-4397-829.icc_ranking.{sp_name}`();"
    
    try:
        query_job = client.query(sp_query)
        query_job.result()  # Wait for the job to complete
        affected_rows = query_job.num_dml_affected_rows
        logging.info(f"✅{sp_name} Stored procedure executed successfully and affected {affected_rows} rows.")
    except Exception as e:
        logging.error(f"❌{sp_name} Failed to execute stored procedure: {e}")
    return None


def call_icc_ranking_api(**kwargs):
    now = datetime.now()
    category = kwargs["category"]

    format_types = ["test", "odi", "t20"]

    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/{category}"
    
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    csv_filename = f"{category}_rankings_{now.strftime('%Y%m%d_%H-%M-%S')}.csv"
    local_path = f"/tmp/{csv_filename}"
    gcp_path = f"{csv_folder}/{csv_filename}"

    field_names = ["match_format","ranking","name","player_id","country","rating","points","lastUpdatedOn","insert_timestamp"]

    all_rows = []

    for match_format in format_types:
        params = {"formatType": match_format}
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            logging.warning(f"❌ Failed for {match_format}")
            break
        logging.info(f"Successful for {match_format}")

        json_data = response.json()
        data = json_data.get("rank", [])

        for entry in data:
            all_rows.append({
                "match_format": match_format,
                "ranking": entry.get("rank"),
                "name": entry.get("name"),
                "player_id": entry.get("id"),
                "country": entry.get("country"),
                "rating": entry.get("rating"),
                "points": entry.get("points"),
                "lastUpdatedOn": entry.get("lastUpdatedOn"),
                "insert_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            })

    # Write once
    if all_rows:
        with open(local_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(all_rows)
        upload_csv_to_gcs(local_path,gcp_path) # Upload to GCS
        logging.info(f"✅ Data written to {csv_filename}")
        if upload_csv_to_gcs(local_path, gcp_path):
                load_gcs_to_bigquery(gcp_path, category) # Load to BigQuery
    else:
        logging.warning("⚠️ No data received")
    return None

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

# -----------------------------
# DAG Definition
# -----------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'email': ['avinashkeerthi20@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG (
    dag_id = 'fetch_icc_ranking',
    default_args=default_args,
    description = 'fetching icc ranking on daily base',
    schedule_interval = '@daily',
    catchup=False
) as dag :
    
    icc_ranking_batsmen = PythonOperator(
        task_id="icc_ranking_batsmen",
        python_callable = call_icc_ranking_api,
        op_kwargs={"category": "batsmen"}
    )

    icc_ranking_bowlers = PythonOperator(
        task_id="icc_ranking_bowlers",
        python_callable = call_icc_ranking_api,
        op_kwargs={"category": "bowlers"},
    )

    icc_ranking_allrounders = PythonOperator(
        task_id="icc_ranking_allrounders",
        python_callable = call_icc_ranking_api,
        op_kwargs={"category": "allrounders"},
    )

    sp_update_ranking = PythonOperator(
        task_id="sp_update_ranking",
        python_callable = run_bq_stored_procedure,
        op_kwargs={"procedure": "sp_update_rankings"},
    )

    [icc_ranking_batsmen, icc_ranking_bowlers, icc_ranking_allrounders] >> sp_update_ranking