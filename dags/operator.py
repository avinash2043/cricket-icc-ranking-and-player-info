from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from datetime import datetime,timedelta
import requests
import pandas as pd
import json
import csv
import logging
import re
import os
from concurrent.futures import ThreadPoolExecutor


bucket_name = Variable.get("bucket_name",'')  # Get bucket name from Airflow Variable
project_id = Variable.get("project_id",'')  # Get project ID from Airflow Variable
icc_ranking_dataset_id = Variable.get("icc_ranking_dataset_id",'')  # Get dataset ID from Airflow Variable
players_data_dataset_id = Variable.get("players_data_dataset_id",'')  # Get dataset ID from Airflow Variable
rapidapi_key = Variable.get("rapidapi_key",'')  # Get API key from Airflow Variable

headers = {
    "x-rapidapi-key": rapidapi_key,
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

def icc_rankings(**kwargs):
    run_id = kwargs['run_id']
    modify_runid = re.sub(r'[^a-zA-Z0-9\-_]', '', run_id).strip('_-')

    # Fetch ICC rankings for a given category.
    category = kwargs["category"]
    csv_folder = kwargs.get("csv_folder", "icc_ranking_files")
    format_types = ["test", "odi", "t20"]

    url = f"https://cricbuzz-cricket.p.rapidapi.com/rankings/v1/rankings/{category}"

    csv_filename = f"{category}_rankings_{modify_runid}.csv"
    local_path = f"/tmp/{csv_filename}"

    field_names = ["match_format","ranking","player_name","player_id","country","rating","points","lastUpdatedOn","insert_timestamp","run_id"]

    all_rows = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(lambda fmt: fetch_format_data(url, fmt), format_types)
        for result in results:
            # all_rows.extend(result)
            # Add run_id to each row
            for row in result:
                row["run_id"] = run_id
                all_rows.append(row)
    
    if all_rows:
        logging.info(f"✅ Successfully fetched data for category: {category}")
        # Write CSV
        with open(local_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(all_rows)
        logging.info(f"✅ CSV file created at {local_path}")
        upload_to_gcs(local_path, bucket_name, csv_folder, csv_filename)
    else:
        logging.warning(f"⚠️ No data fetched for category: {category}")

    

def fetch_format_data(url, fmt):
    params = {"formatType": fmt}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        logging.info(f"✅ API call successful for match_format: {fmt}")
        data = response.json().get("rank", [])
        return [
            {
                "match_format": fmt,
                "ranking": entry.get("rank"),
                "player_name": entry.get("name"),
                "player_id": entry.get("id"),
                "country": entry.get("country"),
                "rating": entry.get("rating"),
                "points": entry.get("points"),
                "lastUpdatedOn": entry.get("lastUpdatedOn"),
                "insert_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            for entry in data
        ]

    except Exception as e:
        logging.error(f"❌ Exception occurred for {fmt}: {e}")
        raise

def upload_to_gcs(local_path, bucket_name, csv_folder, csv_filename):
    try:
        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=f"{csv_folder}/{csv_filename}",
            filename=local_path,
            mime_type='text/csv'
        )
        logging.info(f"✅ File uploaded to GCS: {bucket_name}/{csv_folder}/{csv_filename}")
    except Exception as e:
        logging.error(f"❌ Exception occurred while uploading to GCS: {e}")
        raise
    finally:
            if os.path.exists(local_path):
                os.remove(local_path)
                logging.info(f"🧹 Local file {local_path} removed after upload.")

def load_gcs_to_bigquery(**kwargs):
    run_id = kwargs['run_id']
    modify_runid = re.sub(r'[^a-zA-Z0-9\-_]', '', run_id).strip('_-')

    category = kwargs["category"]
    csv_folder = kwargs.get("csv_folder", "icc_ranking_files")
    csv_filename = f"{category}_rankings_{modify_runid}.csv"
    gcs_uri = f"gs://{bucket_name}/{csv_folder}/{csv_filename}"
    try:
        table_id = f"{project_id}.{icc_ranking_dataset_id}.{category}_ranking_stg"

        client = bigquery.Client(project=project_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        load_job.result()  # Wait for the job to complete.
        affected_rows = client.get_table(table_id).num_rows
        logging.info(f"✅ Loaded {affected_rows} rows into BigQuery table: {table_id}")
    except Exception as e:
        logging.error(f"❌ Exception occurred while loading data into BigQuery: {e}")
        raise

def run_bq_sp(dataset_id, sp_name, params = None):
    try:
        client = bigquery.Client(project=project_id)
        query = f"CALL `{project_id}.{dataset_id}.{sp_name}`({params})"
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete.\
        logging.info(f"✅ Stored procedure {sp_name} executed successfully.")
        for row in query_job:
            logging.info(f"✅ Stored procedure {sp_name} affected_rows: {row[0]}")
    except Exception as e:
        logging.error(f"❌ Exception occurred while executing stored procedure {sp_name}: {e}")
        raise

def run_bq_query(query, params = None):
    try:
        client = bigquery.Client(project=project_id)
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        query_job.result()  # Wait for the job to complete.
        logging.info(f"✅ Query executed successfully.")
    except Exception as e:
        logging.error(f"❌ Exception occurred while executing query: {e}")
        raise

def push_to_xcom(ti, key, value,clear_old_xcoms=False):
    try:
        if clear_old_xcoms: # clear all old xcoms for the same key before pushing new value
            ti.xcom_clear(
                key=key,
                task_ids=ti.task_id,
                dag_id=ti.dag_id,
                include_prior_dates=True
                )
        else: # clear only the xcom for the current execution date before pushing new value
            ti.clear_xcom(
                key=key,
                task_ids=ti.task_id,
                dag_id=ti.dag_id,
                execution_date=ti.execution_date
                )
        # Push the new value to XCom
        ti.xcom_push(key=key, value=value)
    except Exception as e:
        logging.error(f"❌ Exception occurred while pushing to XCom: {e}")
        raise

def fetch_player_info(**kwargs):
    run_id = kwargs['run_id']
    ti = kwargs['task_instance']
    try:
        player_ids = ti.xcom_pull(key='missing_player_ids',include_prior_dates=True)
        if not player_ids:
            logging.info("❗No missing player IDs found in XCom.")
    except Exception as e:
        logging.error(f"❌ Exception occurred while fetching player IDs from XCom: {e}")
        raise

    player_info_list = []
    for player_id in player_ids:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/players/v1/player/{player_id}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            logging.info(f"✅ API call successful for player_id: {player_id}")
            player_info_data = response.json()
            dob = player_info_data.get('DoB', '')
            player_info_list.append({
                "player_id": int(player_info_data.get("id")),
                "name": player_info_data.get("name"),
                "role": player_info_data.get("role"),
                "country": player_info_data.get("intlTeam"),
                "batting_style": player_info_data.get("bat"),
                "bowling_style": player_info_data.get("bowl"),
                "DoB": dob.split('(')[0].strip() if dob else None,
                "birthPlace": player_info_data.get("birthPlace")
            })
        except Exception as e:
            logging.error(f"❌ Exception occurred for player_id {player_id}: {e}")
            raise
    logging.info(f"⬇️ Total records fetched: {len(player_info_list)}")
    
    if player_info_list:
        df = pd.DataFrame(player_info_list)
        df["insert_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["run_id"] = run_id
        player_info_schema = [
            bigquery.SchemaField("player_id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("role", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("batting_style", "STRING"),
            bigquery.SchemaField("bowling_style", "STRING"),
            bigquery.SchemaField("DoB", "DATE"),
            bigquery.SchemaField("birthPlace", "STRING"),
            bigquery.SchemaField("insert_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("run_id", "STRING")
        ]
        logging.info(f"✅ DataFrame created with {len(df)} records")
        load_df_to_bigquery(df, players_data_dataset_id, kwargs.get("TARGET_TABLE"), player_info_schema)
    else:
        logging.warning(f"⚠️ No player info data fetched for player_info_list")
    
def load_df_to_bigquery(df, dataset_id, table_name, schema=None):
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET
        )
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete.
        logging.info(f"✅ Loaded {len(df)} rows into BigQuery table: {table_id}")
    except Exception as e:
        logging.error(f"❌ Exception occurred while loading DataFrame into BigQuery: {e}")
        raise

def fetch_player_stats(**kwargs):
    stat_type = kwargs["stat_type"]
    ti = kwargs['task_instance']
    try:
        player_ids = ti.xcom_pull(key='player_ids',include_prior_dates=True)
        if not player_ids:
            logging.info("❗No player_ids found in XCom.")
    except Exception as e:
        logging.error(f"❌ Exception occurred while fetching player IDs from XCom: {e}")
        raise

    player_stats_list = []
    for player_id in player_ids:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/{stat_type}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            logging.info(f"✅ API call successful for player_id: {player_id}, stat_type: {stat_type}")
            stats_data = response.json()
            if "values" not in stats_data or "headers" not in stats_data:
                logging.warning(f"❌ No valid data for player {player_id}, stat_type {stat_type}")
                raise
            df = pd.DataFrame(
                [row["values"] for row in stats_data["values"]],
                columns=stats_data["headers"]
            )
            dft = (
                df.set_index("ROWHEADER")
                .T
                .reset_index()
                .rename(columns={"index": "Format"})
            )
            dft.insert(0, "player_id", player_id)
            dft.insert(1, "stat_type", stat_type)
            player_stats_list.append(dft)
        except Exception as e:
            logging.error(f"❌ Exception occurred for player_id {player_id}, stat_type: {stat_type}: {e}")
            raise

    if player_stats_list:
        final_df = pd.concat(player_stats_list, ignore_index=True)
        final_df["insert_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"✅ DataFrame created with {len(final_df)} records for stat_type: {stat_type}")
        load_df_to_bigquery(
            final_df, 
            players_data_dataset_id,
            kwargs.get("TARGET_TABLE"),
            schema=[bigquery.SchemaField("insert_timestamp", "TIMESTAMP")]
            )
    else:
        logging.warning(f"⚠️ No player stats data fetched for stat_type: {stat_type}")