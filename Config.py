#Project Configurations
project_name='silver-argon-320416'
zone = 'us-central1-a'
#template_bucket
template_bucket = 'batch-pipeline-testing'
#Daily Trigger Dataflow Template Constants
job_name ='batch-pipeline'
# ValueProviders like source table name and sink table name
input_data ='gs://{bucket}/german.data'.format(bucket=template_bucket)
#Template's GCS path
template_gcs_path='gs://{bucket}/Template/batch-pipeline-template'.format(bucket=template_bucket)
#Temprory path location
temp_location='gs://{bucket}/Temp'.format(bucket=template_bucket)