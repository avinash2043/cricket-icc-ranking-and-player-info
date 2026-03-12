from airflow import DAG
from airflow.models import Variable
from google.cloud import bigquery
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from dag_cricket_operator import icc_ranking, push_to_xcom,run_bq_query,run_bq_sp,fetch_stats,fetch_player_info
import logging



bucket_name = Variable.get("gcs_bucket_name",'')  # Get bucket name from Airflow Variable
project_id = Variable.get("gcp_project_id",'')  # Get project ID from Airflow Variable
icc_ranking_dataset_id = Variable.get("icc_ranking_dataset_id",'')  # Get dataset ID from Airflow Variable
players_data_dataset_id = Variable.get("players_data_dataset_id",'')  # Get dataset ID from Airflow Variable

def check_player_ids(**context):
	query = f"""SELECT distinct player_id 
	            FROM `{project_id}.icc_ranking.rankings`
				WHERE CURRENT_DATE() = DATE(update_timestamp)
			"""
	result = run_bq_query(query)
	# Convert to DataFrame using pandas
	df = pd.DataFrame(result.to_dataframe()) if hasattr(result, 'to_dataframe') else pd.DataFrame(result)
	player_ids = df["player_id"].tolist()
	logging.info(f"✅Found {len(player_ids)} player_ids")
	if not player_ids:
		return "end_task"
	else:
		ti = context["ti"]
		push_to_xcom(ti, key="player_ids", value=player_ids)
		logging.info(f"✅Pushed {len(player_ids)} player_ids to XCom")
		
def find_missing_player_ids(ti, **context):
	player_ids = ti.xcom_pull(
		key="player_ids",
		include_prior_dates=True
		)
	if not player_ids:
		logging.warning("🚫No player_ids found in XCom")
	logging.info(f"✅Pulled no.of player_ids: {len(player_ids)}")
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
	logging.info(f"❗Existing IDs: {existing_ids}")
	logging.info(f"❓Missing IDs: {missing_info_ids}")
	if missing_info_ids != []:
		ti.xcom_push(key="missing_player_ids", value=missing_info_ids)
		logging.info(f"Pushed {len(missing_info_ids)} missing player_ids to XCom")
		return 'get_missing_player_info'
	return 'end_task'

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
	dag_id = 'rankings_and_players_info',
	default_args = default_args,
	doc_md = 'Fetch ICC ranking using cricket operator functions',
	schedule_interval = None,
	catchup = False
) as dag:

	icc_ranking_batsmen = PythonOperator(
		task_id = "icc_ranking_batsmen",
		doc_md = "Fetch ICC batsmen ranking for test,odi,t20",
		python_callable = icc_ranking,
		op_kwargs = {"category": "batsmen"}
	)

	icc_ranking_bowlers = PythonOperator(
		task_id = "icc_ranking_bowlers",
		doc_md = "Fetch ICC bowlers ranking for test,odi,t20",
		python_callable = icc_ranking,
		op_kwargs={"category": "bowlers"}
	)

	icc_ranking_allrounders = PythonOperator(
		task_id = "icc_ranking_allrounders",
		doc_md = "Fetch ICC allrounders ranking for test,odi,t20",
		python_callable = icc_ranking,
		op_kwargs = {"category": "allrounders"}
	)
	
	sp_update_ranking = PythonOperator(
		task_id = "sp_update_ranking",
		doc_md = "Run stored procedure to update rankings table",
		python_callable = run_bq_sp,
		op_kwargs = {
			"dataset_id": icc_ranking_dataset_id,
			"sp_name": "sp_update_rankings"
		}
	)
	
	fetch_player_ids = PythonOperator(
		task_id ="fetch_player_ids",
		doc_md="Fetch player IDs from ranking table which got updated in present run",
		python_callable=check_player_ids,
	)
	
	get_batting_stats = PythonOperator(
		task_id ='get_batting_stats',
		doc_md = "Fetch batting stats for players hows ranking got changes and load to BigQuery",
		python_callable=fetch_stats,
		op_kwargs={
			'stat_type': 'batting',
			'TARGET_TABLE': 'batting_stats_stg'
		}
	)

	get_bowling_stats = PythonOperator(
		task_id ='get_bowling_stats',
		doc_md = "Fetch bowling stats for players hows ranking got changes and load to BigQuery",
		python_callable=fetch_stats,
		op_kwargs={
			'stat_type': 'bowling',
			'TARGET_TABLE': 'bowling_stats_stg'
		}
	)
	
	get_missing_player_ids = PythonOperator(
		task_id="get_missing_player_ids",
		doc_md = "Find player IDs which are missing in players_info table and push to XCom",
		python_callable=find_missing_player_ids
	)
	
	get_missing_player_info = PythonOperator(
		task_id="get_missing_player_info",
		doc_md = "Fetch player info for missing player IDs and load to BigQuery",
		python_callable=fetch_player_info,
		op_kwargs={
				"player_ids": "{{ task_instance.xcom_pull(task_ids='get_missing_player_ids', key='missing_player_ids') }}"
        }
	)

	end_task = PythonOperator(
		task_id="end_task",
		python_callable=lambda: logging.info("Ending DAG execution."),
	)
	
	start_task = PythonOperator(
		task_id="start_task",
		python_callable=lambda: logging.info("Starting DAG execution."),
	)

	start_task >> [icc_ranking_batsmen, icc_ranking_bowlers, icc_ranking_allrounders] >> sp_update_ranking >> fetch_player_ids
	#if fetch_player_ids is not found
	fetch_player_ids >> end_task
	#if fetch_player_ids is found
	fetch_player_ids >> [get_batting_stats, get_bowling_stats] >> end_task
	fetch_player_ids >> get_missing_player_ids
	#if get_missing_player_ids is found
	get_missing_player_ids >> get_missing_player_info >> end_task
	#if get_missing_player_ids is not found
	get_missing_player_ids >> end_task