import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from trigger_job_util import TriggerJobUtil
from dag_arguments import DAGArgs
import Config

def daily_sync_etl():
    try:
        job_name = Config.job_name
        temp_location = Config.temp_location
        zone = Config.zone
        input_data = Config.input_data
        input_param_name = Config.input_param_name
        gcs_path = Config.template_gcs_path
        body = DAGArgs().get_daily_trigger_dataflow_body(job_name,temp_location,zone,input_data,input_param_name)
        TriggerJobUtil.trigger_job(gcs_path, body)
    except Exception as ex:
        print(['Exception', ex])
        
try:
    with DAG(
        'GCS_to_Bigquery_dag',
        default_args=DAGArgs().default_args,
        description='DAG for Hourly sync to cloud sql',
        max_active_runs=1,
        concurrency=4,
        catchup = False,
        schedule_interval='@hourly',
        ) as dag:
            task1 = PythonOperator(
                    task_id='daily-sync-etl',
                    python_callable=daily_sync_etl,
                    dag = dag)
except IndexError as ex:
    logging.debug("Exception",str(ex))
    
task1

