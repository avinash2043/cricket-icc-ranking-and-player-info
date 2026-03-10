import functions_framework
from google.cloud import bigquery
import os

@functions_framework.cloud_event
def load_csv_to_bigquery(cloud_event):
    data = cloud_event.data
    event_id = cloud_event["id"]
    print(f"event_id: {event_id} triggered load_csv_to_bigquery function")
    # Event payload
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Only process CSV files
    if not file_name.endswith(".csv"):
        print(f"Skipped non-CSV file: {file_name}")
        return

    # Env vars
    project_id = "project-2b297edf-113a-4397-829"
    dataset_id = "TEST"
    table_id = "test_table"

    uri = f"gs://{bucket_name}/{file_name}"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )

    load_job.result()  # Wait for completion

    print(f"Loaded {file_name} into {table_ref}")

    # Optional metadata logging
    time_created = data.get("timeCreated")
    updated = data.get("updated")
    print(f"Created: {time_created}, Updated: {updated}")


'''
data = cloud_event.data
event_id = cloud_event["id"]
event_type = cloud_event["type"]
bucket = data["bucket"]
name = data["name"]
metageneration = data["metageneration"]
timeCreated = data["timeCreated"]
updated = data["updated"]

print(f"Event ID: {event_id}")
print(f"Event type: {event_type}")
print(f"Bucket: {bucket}")
print(f"File: {name}")
print(f"Metageneration: {metageneration}")
print(f"Created: {timeCreated}")
print(f"Updated: {updated}")
'''