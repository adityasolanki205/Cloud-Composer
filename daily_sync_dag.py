import airflow
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from dag_arguments import DAGArgs
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import Config

def daily_sync_etl():
    try:
        job_name = Config.job_name
        temp_location = Config.temp_location
        zone = Config.zone
        input_data = Config.input_data
        #input_param_name = Config.input_param_name
        gcs_path = Config.template_gcs_path
        body = DAGArgs.get_daily_trigger_dataflow_body(job_name,temp_location,zone,input_data)
        credentials = GoogleCredentials.get_application_default()
        service = build('dataflow', 'v1b3', credentials=credentials,cache_discovery=False)
        request = service.projects().templates().launch(projectId=Config.project_name, gcsPath=Config.template_gcs_path, body=body)
        response = request.execute()
        print("-----execute function have been called-------")
        print(response)
        #TriggerJobUtil.trigger_job(gcs_path, body)
    except Exception as ex:
        print(['Exception', ex])
        
try:
    with models.DAG(
            'GCS_to_Bigquery_dag',
            default_args=DAGArgs().default_args,
            description='DAG for Hourly sync to cloud sql',
            max_active_runs=1,
            concurrency=4,
            catchup = False,
            schedule_interval='@hourly'
        ) as dag:
            task1 = PythonOperator(
                    task_id='daily-sync-etl',
                    python_callable=daily_sync_etl,
                    dag = dag)
except IndexError as ex:
    logging.debug("Exception",str(ex))
    
task1

