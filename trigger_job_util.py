from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import Config
class TriggerJobUtil:
    @staticmethod
    def trigger_job(gcs_path, body):
        print(" --------in triggerjobs---------")
        credentials = GoogleCredentials.get_application_default()
        service = build('dataflow', 'v1b3', credentials=credentials)
        request = service.projects().templates().launch(projectId=Config.project_name, gcsPath=Config.template_gcs_path, body=body)
        response = request.execute()
        print("-----execute function have been called-------")
        print(response)

