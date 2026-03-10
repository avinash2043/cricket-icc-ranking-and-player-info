from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
import requests
import pandas as pd
import json
import csv
import logging

bucket_name = Variable.get("gcs_bucket_name",'')  # Get bucket name from Airflow Variable
project_id = Variable.get("gcp_project_id",'')  # Get project ID from Airflow Variable
icc_ranking_dataset_id = Variable.get("icc_ranking_dataset_id",'')  # Get dataset ID from Airflow Variable
players_data_dataset_id = Variable.get("players_data_dataset_id",'')  # Get dataset ID from Airflow Variable

headers = {
    "x-rapidapi-key": Variable.get("rapidapi_key",'') ,  # Get API key from Airflow Variable
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}


now = datetime.now(tz=ZoneInfo('Asia/Kolkata'))
print("Current date and time in Asia/Kolkata:", now)


def upload_csv_to_gcs(local_path,tg_gcp_path,bucket_name):
    try:
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name = bucket_name,
            object_name = tg_gcp_path, 
            filename = local_path 
        )
        logging.info(f"✅ Uploaded {local_path} to gs://{bucket_name}/{tg_gcp_path}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to upload CSV to GCS: {e}")
        return False


def load_gcs_to_bigquery(gcp_path,project_id, dataset_id, table_name):
    """Load CSV from GCS to BigQuery"""
    gcs_uri = f"gs://{bucket_name}/{gcp_path}"
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.{table_name}"

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


def run_bq_query(query=None):
    bq_client = bigquery.Client(project=project_id)
    
    try:
        query_job = bq_client.query(query)
        results = query_job.result()  # Wait for the job to complete
        return results
    except Exception as e:
        logging.error(f"❌ Error running BigQuery query: {e}")
        return None



def icc_ranking(**kwargs):
    now = datetime.now(tz=ZoneInfo('Asia/Kolkata'))
    category = kwargs["category"]
    csv_folder = kwargs.get("csv_folder", "icc_ranking_files")  # Use provided csv_folder or default
    format_types = ["test", "odi", "t20"]

    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/{category}"

    csv_filename = f"{category}_rankings_{now.strftime('%Y%m%d_%H-%M-%S')}.csv"
    local_path = f"/tmp/{csv_filename}"
    gcp_path = f"{csv_folder}/{csv_filename}"

    field_names = ["match_format","ranking","name","player_id","country","rating","points","lastUpdatedOn","insert_timestamp"]

    all_rows = []

    for match_format in format_types:
        params = {"formatType": match_format}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code != 200:
                logging.error(f"❌ Failed for {match_format} with status code {response.status_code}. Stopping downstream tasks.")
                return None  # Stop downstream processing
            logging.info(f"Successful for {match_format}")
            json_data = response.json()
            data = json_data.get("rank", [])
            for entry in data:
                all_rows.append({
                    "match_format": match_format,
                    "ranking": entry.get("rank"),
                    "player_name": entry.get("name"),
                    "player_id": entry.get("id"),
                    "country": entry.get("country"),
                    "rating": entry.get("rating"),
                    "points": entry.get("points"),
                    "lastUpdatedOn": entry.get("lastUpdatedOn"),
                    "insert_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                })
        except Exception as e:
            logging.error(f"❌ Exception occurred for {match_format}: {e}. Stopping downstream tasks.")
            return None  # Stop downstream processing

    # Write once
    if all_rows:
        with open(local_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(all_rows)
        status_upload_csv_to_gcs = upload_csv_to_gcs(local_path, gcp_path, bucket_name)  # Upload to GCS
        logging.info(f"✅ Data written to {csv_filename}")
        if status_upload_csv_to_gcs:
                load_gcs_to_bigquery(gcp_path,project_id, icc_ranking_dataset_id, f"{category}_ranking_stg") # Load to BigQuery
    else:
        logging.warning("⚠️ No data received")
    return None


def push_to_xcom(ti, key, value):
    """Push a value to XCom."""
    try:
        ti.xcom_push(key=key, value=value)
        logging.info(f"✅ Pushed {key} to XCom")
    except Exception as e:
        logging.error(f"❌ Failed to push {key} to XCom: {e}")
        raise

def fetch_stats(ti, **kwargs):
    """Fetch stats for all players and load to BigQuery."""
    stat_type = kwargs['stat_type']
    try:
        player_ids = ti.xcom_pull(key="player_ids")
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
                dft["insert_timestamp"] = datetime.now(tz=ZoneInfo('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
                all_data.append(dft)

            except requests.exceptions.RequestException as re:
                logging.error(f"❌ HTTP error for player_id {player_id}: {re}")
            except Exception as e:
                logging.error(f"❌ Error processing player_id {player_id}: {e}")

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            load_df_to_bigquery(final_df, kwargs.get("TARGET_TABLE"))
        else:
            logging.warning(f"No data fetched for stat_type: {stat_type}")

    except Exception as e:
        logging.error(f"❌ Failed to fetch stats for stat_type {stat_type}: {e}")
        raise

def load_df_to_bigquery(df, target_table):
    """Load a DataFrame to BigQuery."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{players_data_dataset_id}.{target_table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )

        load_job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        load_job.result()  # Wait for the job to complete
        logging.info(f"✅ Loaded {len(df)} rows into {table_id}")
    except Exception as e:
        logging.error(f"❌ Error loading DataFrame into BigQuery: {e}")
        raise

def fetch_player_info(player_ids, **context):
    if not player_ids:
        logging.info("✅ No new players to fetch")
        return None
    all_player_info = []
    for player_id in player_ids:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}"
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                logging.error(f"❌ API failed for {player_id}: {response.text}")
                break
            data = response.json()
            data['DoB'] = data.get('DoB', '').split('(')[0].strip() if data.get('DoB') else None
            player_record = {
                "player_id": int(data.get("id")),
                "name": data.get("name"),
                "role": data.get("role"),
                "country": data.get("intlTeam"),
                "batting_style": data.get("bat"),
                "bowling_style": data.get("bowl"),
                "DoB": data.get("DoB"),
                "birthPlace": data.get("birthPlace"),
                "insert_timestamp": datetime.now(tz=ZoneInfo('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
            }
            all_player_info.append(player_record)
        except Exception as e:
            logging.error(f"❌ Exception while fetching {player_id}: {e}")
            continue
    logging.info(f"📦 Total records fetched: {len(all_player_info)}")

    if all_player_info:
        df = pd.DataFrame(all_player_info)
        load_df_to_bigquery(df, context.get('TARGET_TABLE', 'player_info'))