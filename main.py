from google.cloud import compute_v1, bigquery
import logging
from datetime import datetime, timedelta

def list_compute_instances():
    """Fetches all VM instances and their statuses in the specified project."""
    compute_client = compute_v1.InstancesClient()
    project_id = "fast-ability-439911-u1" 
    request = compute_v1.AggregatedListInstancesRequest(project=project_id)
    
    instances = []
    
    try:
        for zone, response in compute_client.aggregated_list(request=request):
            if response.instances:
                for instance in response.instances:
                    instance_info = {
                        "project_id": project_id,
                        "instance_name": instance.name,
                        "zone": zone.split('/')[-1],
                        "status": instance.status
                    }
                    instances.append(instance_info)
                    logging.info(f"Instance found: {instance_info}")
                    
    except Exception as e:
        logging.error(f"Error while listing instances: {e}")
    
    print(instances)
    return instances

def insert_to_bigquery(instances):
    """Inserts instance data into BigQuery, with the table named based on today's date."""
    bq_project = 'nagesh-sandbox'
    bq_dataset = 'compute_status'
    bq_table = f"vm_status_{datetime.now().strftime('%Y%m%d')}"  # Table with today's date

    print(bq_project)
    print(bq_dataset)
    print(bq_table)

    # Initialize BigQuery client
    bq_client = bigquery.Client(project=bq_project)

    # Define the table schema
    table_schema = [
        bigquery.SchemaField('project_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('instance_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('zone', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('status', 'STRING', mode='NULLABLE')
    ]

    # Create table if it doesn't exist
    table_ref = bq_client.dataset(bq_dataset).table(bq_table)
    try:
        bq_client.get_table(table_ref)
        logging.info(f"Table {bq_table} already exists.")
    except Exception:
        logging.info(f"Table {bq_table} does not exist. Creating table...")
        table = bigquery.Table(table_ref, schema=table_schema)
        bq_client.create_table(table)
        logging.info(f"Created table {bq_table}.")

    # Insert rows into BigQuery
    print(table_ref)
    print(instances)
    print("********")
    errors = bq_client.insert_rows_json(table_ref, instances)
    if errors:
        logging.error(f"Errors inserting rows into BigQuery: {errors}")
    else:
        logging.info("Successfully inserted rows into BigQuery.")

def main_entry(request):
    """Entry point for Cloud Function."""
    try:
        instances = list_compute_instances()
        if not instances:
            return "No instances found in the project.", 200

        # Insert into BigQuery
        insert_to_bigquery(instances)
        
        return {"instances": instances, "status": "success"}, 200
    
    except Exception as e:
        logging.error(f"Error in main_entry: {e}")
        return f"Internal error: {e}", 500
