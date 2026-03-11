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


ist = datetime.now(tz=ZoneInfo('Asia/Kolkata'))
print("Current date and time in Asia/Kolkata:", ist)


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
        raise Exception(f'⚠️Exception:{e}')


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
        raise Exception(f'⚠️Exception:{e}')


def run_bq_query(query=None, params=None):
    bq_client = bigquery.Client(project=project_id)
    try:
        if params:
            query_job = bq_client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        else:
            query_job = bq_client.query(query)
        results = query_job.result()  # Wait for the job to complete
        return results
    except Exception as e:
        logging.error(f"❌ Error running BigQuery query: {e}")
        raise Exception(f'⚠️Exception:{e}')
    
def run_bq_sp(dataset_id, sp_name):
    bq_client = bigquery.Client(project=project_id)
    query = f"CALL `{project_id}.{dataset_id}.{sp_name}`()"
    try:
        query_job = bq_client.query(query)
        results = query_job.result()  # Wait for the job to complete
        affected_rows = query_job.num_dml_affected_rows
        logging.info(f"✅ Stored procedure {project_id}.{dataset_id}.{sp_name} executed, affected rows: {affected_rows}")
    except Exception as e:
        logging.error(f"❌ Error running BigQuery stored procedure: {e}")
        raise Exception(f'⚠️Exception:{e}')



def icc_ranking(**kwargs):
    now = datetime.now()
    category = kwargs["category"]
    csv_folder = kwargs.get("csv_folder", "icc_ranking_files")  # Use provided csv_folder or default
    format_types = ["test", "odi", "t20"]

    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/{category}"

    csv_filename = f"{category}_rankings_{now.strftime('%Y%m%d_%H-%M-%S')}.csv"
    local_path = f"/tmp/{csv_filename}"
    gcp_path = f"{csv_folder}/{csv_filename}"

    field_names = ["match_format","ranking","player_name","player_id","country","rating","points","lastUpdatedOn","insert_timestamp"]

    all_rows = []

    for match_format in format_types:
        params = {"formatType": match_format}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code != 200:
                logging.error(f"❌ API call failed for match_format: {match_format} with status code {response.status_code}.")
                raise Exception(f'⚠️Exception:{e}')
            logging.info(f"✅ API call successful for match_format: {match_format}")
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
            logging.error(f"❌ Exception occurred for {match_format}: {e}.")
            raise Exception(f'⚠️Exception:{e}')

    # Write once
    try:
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
            logging.error("No data received.")
            raise Exception("❌ No data received.")
    except Exception as e:
        logging.error(f"❌ Exception in writing/loading rankings data: {e}")
        raise Exception(f'⚠️Exception:{e}')


def push_to_xcom(ti, key, value):
    """Push a value to XCom."""
    try:
        ti.xcom_push(key=key, value=value)
        logging.info(f"✅ Pushed {key} to XCom")
    except Exception as e:
        logging.error(f"❌ Failed to push {key} to XCom: {e}")
        raise Exception(f'⚠️Exception:{e}')

def fetch_stats(ti, **kwargs):
    """Fetch stats for all players and load to BigQuery."""
    stat_type = kwargs['stat_type']
    try:
        player_ids = ti.xcom_pull(key="player_ids")
        if not player_ids:
            raise Exception("❌ No player IDs received from XCom")

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
                    raise Exception(je)
                    

                if "values" not in data or "headers" not in data:
                    logging.warning(f"❌ No valid data for player {player_id}, stat_type {stat_type}")
                    raise Exception("Missing 'values' or 'headers' in response")

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
                dft["insert_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                all_data.append(dft)

            except requests.exceptions.RequestException as re:
                logging.error(f"❌ HTTP error for player_id {player_id}: {re}")
                raise Exception(f'⚠️Exception:{re}')
            except Exception as e:
                logging.error(f"❌ Error processing player_id {player_id}: {e}")
                raise Exception(f'⚠️Exception:{e}')

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            load_df_to_bigquery(final_df, kwargs.get("TARGET_TABLE"))
        else:
            logging.warning(f"No data fetched for stat_type: {stat_type}")

    except Exception as e:
        logging.error(f"❌ Failed to fetch stats for stat_type {stat_type}: {e}")
        raise Exception(f'⚠️Exception:{e}')

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
        raise Exception(f'⚠️Exception:{e}')

def fetch_player_info(ti, **context):
    now = datetime.now()
    try:
        player_ids = ti.xcom_pull(key="missing_player_ids", include_prior_dates=True)
        if not player_ids:
            logging.warning("No player_ids found in XCom")
    except Exception as e:
        logging.error(f"❌ XCom pull failed: {e}")
        raise Exception(f'⚠️Exception:{e}')
    all_player_info = []
    for player_id in player_ids:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}"
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                logging.error(f"❌ API failed for {player_id}: {response.text}")
                raise Exception(f'⚠️Exception:{response.text}')
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
                "birthPlace": data.get("birthPlace")
            }
            all_player_info.append(player_record)
        except Exception as e:
            logging.error(f"❌ Exception while fetching {player_id}: {e}")
            raise Exception(f'⚠️Exception:{e}')
    logging.info(f"📦 Total records fetched: {len(all_player_info)}")

    if all_player_info:
        df = pd.DataFrame(all_player_info)
        df['insert_timestamp'] = now.strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"✅ DataFrame created with {len(df)} records")
        load_df_to_bigquery(df,'players_info')