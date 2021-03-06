from datetime import timedelta, datetime
import json
import urllib.parse
import Config
class DAGArgs():
    default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date':'16-08-2021',
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(hours=5),
    }
    def __init__(self, *args, **kwargs):
        print("in DAG Arguments")
    def get_daily_trigger_dataflow_body(job_name,temp_location, zone, input_data):
        body = {
            "jobName": "{jobname}".format(jobname=job_name),
            "parameters": {
                'runner': 'DataFlowRunner',
            },
            "environment": {
                "tempLocation": temp_location,
                "zone": zone
            }
        }
        return body