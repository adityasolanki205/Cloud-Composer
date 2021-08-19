from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)


default_arguments = {"owner": "Aditya Solanki", "start_date": days_ago(1)}


with DAG(
    "Loading-GCS-to-BQ",
    schedule_interval="0 20 * * *",
    catchup=False,
    default_args=default_arguments,
    ) as dag:
        create_cluster=DataprocClusterCreateOperator(
            task_id='create_cluster',
            project_id='silver-argon-320416',
            cluster_name='spark-cluster-{{ds_nodash}}',
            num_workers=2,
            worker_machine_type='n1-standard-1',
            storage_bucket="dataproc-testing-pyspark",
            region="us-central1",
            zone="us-central1-c",
        )

        calculate_min_temp=DataProcPySparkOperator(
            task_id='Loading_data_from_GCS_to_BQ',
            main='gs://dataproc-testing-pyspark/code/Batch.py',
            arguments=['gs://dataproc-testing-pyspark/german_data.csv'],
            cluster_name="spark-cluster-{{ ds_nodash }}",
            region="us-central1",
        )


        delete_cluster=DataprocClusterDeleteOperator(
            task_id="delete_cluster",
            project_id="silver-argon-320416",
            cluster_name="spark-cluster-{{ ds_nodash }}",
            trigger_rule="all_done",
            region="us-central1",
        )



create_cluster>>calculate_min_temp>>delete_cluster

