from airflow import DAG
from airflow.models import Variable
from google.cloud import bigquery
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
# from dag_cricket_operator import icc_ranking, push_to_xcom,run_bq_query,run_bq_sp,fetch_player_stats,fetch_player_info
from cricket_operator import icc_rankings, push_to_xcom, run_bq_query, run_bq_sp, fetch_player_stats, fetch_player_info, load_gcs_to_bigquery,check_player_ids
import logging
import re

bucket_name = Variable.get("gcs_bucket_name",'')  # Get bucket name from Airflow Variable
project_id = Variable.get("gcp_project_id",'')  # Get project ID from Airflow Variable
icc_ranking_dataset_id = Variable.get("icc_ranking_dataset_id",'')  # Get dataset ID from Airflow Variable
players_data_dataset_id = Variable.get("players_data_dataset_id",'')  # Get dataset ID from Airflow Variable

'''
def check_player_ids(**kwargs):
    query = f"""SELECT distinct player_id 
                FROM `{project_id}.icc_ranking.rankings`
                WHERE CURRENT_DATE() = DATE(update_timestamp)
            """
    result = run_bq_query(query)
    # Convert to DataFrame using pandas
    df = pd.DataFrame(result.to_dataframe()) if hasattr(result, 'to_dataframe') else pd.DataFrame(result)
    player_ids = df["player_id"].tolist()
    logging.info(f"âœ…Found {len(player_ids)} player_ids")
    if player_ids:
        ti = kwargs["ti"]
        push_to_xcom(ti, key="player_ids", value=player_ids, clear_old_xcoms=True)
        logging.info(f"âœ…Pushed {len(player_ids)} player_ids to XCom")
    else:
        return "end_task"  # If no player_ids found, skip to end_task
'''
    
def find_missing_player_ids(**kwargs):
    ti = kwargs["ti"]
    player_ids = ti.xcom_pull(
        key="player_ids",
        include_prior_dates=True
    )
    if player_ids:
        logging.info(f"âœ…Pulled no.of missing_player_ids: {len(player_ids)}")
        query = f"""
            SELECT player_id 
            FROM `{project_id}.players_data.players_info` 
            WHERE player_id IN UNNEST(@player_ids)
        """
        params = [
            bigquery.ArrayQueryParameter("player_ids", "INT64", player_ids)
        ]
        rows = run_bq_query(query=query, params=params)
        existing_ids = {row.player_id for row in rows}
        missing_info_ids = list(set(player_ids) - existing_ids)
        logging.info(f"â—Existing IDs: {existing_ids}")
        logging.info(f"â—Missing IDs: {missing_info_ids}")
        push_to_xcom(ti,key="missing_player_ids", value=missing_info_ids, clear_old_xcoms=True)
        if missing_info_ids:
            return "get_missing_player_info"
        else:
            logging.info("âœ…No missing player_ids found, skipping to end_task")
            return "end_task"  # If no missing player_ids found, skip to end_task
    else:
        return "end_task"  # If no player_ids found, skip to end_task

def dummy_task(**kwargs):
    run_id = kwargs['run_id']
    modify_runid = re.sub(r'[^a-zA-Z0-9_]', '', run_id).strip('_')
    logging.info(f"{kwargs.get('task_id')} for run_id: {run_id}, modified_run_id: {modify_runid}")


# Default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 1),
    'depends_on_past': False,
    'email': ['avinashkeerthi20@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'cricket_icc_rankings_and_players_info',
    default_args = default_args,
    doc_md = 'Fetch ICC ranking using cricket operator functions',
    schedule_interval = None,
    catchup = False
) as dag:
    
    start_task = PythonOperator(
        task_id="start_task",
        doc_md="Start task for the DAG",
        python_callable=dummy_task,
    )

    icc_ranking_batsmen = PythonOperator(
        task_id = "icc_ranking_batsmen",
        doc_md = "Fetch ICC batsmen ranking for test,odi,t20 and load to gcs bucket",
        python_callable = icc_rankings,
        op_kwargs = {"category": "batsmen"}
    )

    icc_ranking_bowlers = PythonOperator(
        task_id = "icc_ranking_bowlers",
        doc_md = "Fetch ICC bowlers ranking for test,odi,t20 and load to gcs bucket",
        python_callable = icc_rankings,
        op_kwargs={"category": "bowlers"}
    )

    icc_ranking_allrounders = PythonOperator(
        task_id = "icc_ranking_allrounders",
        doc_md = "Fetch ICC allrounders ranking for test,odi,t20 and load to gcs bucket",
        python_callable = icc_rankings,
        op_kwargs = {"category": "allrounders"}
    )
    
    load_batsmen_ranking_to_bq = PythonOperator(
        task_id = "load_batsmen_ranking_to_bq",
        doc_md = "Load batsmen ranking data from GCS to BigQuery",
        python_callable = load_gcs_to_bigquery,
        op_kwargs = {"category": "batsmen"}
    )

    load_bowlers_ranking_to_bq = PythonOperator(
        task_id = "load_bowlers_ranking_to_bq",
        doc_md = "Load bowlers ranking data from GCS to BigQuery",
        python_callable = load_gcs_to_bigquery,
        op_kwargs = {"category": "bowlers"}
    )

    load_allrounders_ranking_to_bq = PythonOperator(
        task_id = "load_allrounders_ranking_to_bq",
        doc_md = "Load allrounders ranking data from GCS to BigQuery",
        python_callable = load_gcs_to_bigquery,
        op_kwargs = {"category": "allrounders"}
    )
    
    sp_update_ranking = PythonOperator(
        task_id = "sp_update_ranking",
        doc_md = "Run stored procedure to update rankings table",
        python_callable = run_bq_sp,
        op_kwargs = {
            "dataset_id": icc_ranking_dataset_id,
            "sp_name": "sp_update_rankings",
            "run_id_pass": True
        }
    )
    
    fetch_player_ids = PythonOperator(
        task_id ="fetch_player_ids",
        doc_md="Fetch player IDs from ranking table which got updated in present run",
        python_callable=check_player_ids,
    )

    get_missing_player_ids = PythonOperator(
        task_id="get_missing_player_ids",
        doc_md="Find player IDs which are missing in players_info table and push to XCom",
        python_callable=find_missing_player_ids,
    )

    get_missing_player_info = PythonOperator(
        task_id="get_missing_player_info",
        doc_md= "Fetch player info for missing player IDs and load to BigQuery",
        python_callable=fetch_player_info,
    )

    get_batting_stats = PythonOperator(
        task_id="get_batting_stats",
        doc_md="Fetch batting stats for players whose ranking got changes and load to BigQuery",
        python_callable=fetch_player_stats,
        op_kwargs={"stat_type": "batsman"},
    )

    get_bowling_stats = PythonOperator(
        task_id="get_bowling_stats",
        doc_md="Fetch bowling stats for players whose ranking got changes and load to BigQuery",
        python_callable=fetch_player_stats,
        op_kwargs={"stat_type": "bowler"},
    )


    end_task = PythonOperator(
        task_id="end_task",
        doc_md="End task for the DAG",
        python_callable=dummy_task
    )

    start_task >> [icc_ranking_batsmen, icc_ranking_bowlers, icc_ranking_allrounders]
    [
        icc_ranking_batsmen >> load_batsmen_ranking_to_bq,
        icc_ranking_bowlers >> load_bowlers_ranking_to_bq,
        icc_ranking_allrounders >> load_allrounders_ranking_to_bq
    ] >> sp_update_ranking >> fetch_player_ids
    #if fetch_player_ids is not found
    fetch_player_ids >> end_task
    #if fetch_player_ids is found
    fetch_player_ids >> [get_missing_player_ids,get_batting_stats, get_bowling_stats] >> end_task
    # if get_missing_player_ids is not found
    get_missing_player_ids >> end_task
    # if get_missing_player_ids is found
    get_missing_player_ids >> get_missing_player_info >> end_task
