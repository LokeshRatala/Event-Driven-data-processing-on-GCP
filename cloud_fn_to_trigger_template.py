# Cloud Function that trigger Dataproc Workflow Template
# https://cloud.google.com/dataproc/docs/tutorials/workflow-function
from google.cloud import dataproc_v1 as dataproc

# Function dependencies, for example:
# package>=version
# below requirement  we need to deploy cloud  function 
# copy and paste in  requirement.txt
## google-cloud-dataproc==5.0.3 ##

def trigger_workflow(data,context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         data (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = data
    print("Processing File: ", file["name"])

    project_id = 'lokesh-mp' # GCP project name
    region = 'us-central1'
    workflow_template = 'nyc-event-driven-data-processing-dataproc-template'  #workflow temp that you want trigger
    
    # Create a client with the endpoint set to the desired cluster region.
    client = dataproc.WorkflowTemplateServiceClient(client_options={
        'api_endpoint': f'{region}-dataproc.googleapis.com'
    })
    
    
    
    input_bucket_uri = f'gs://{file["bucket"]}/{file["name"]}'
    
    request = {
        'name': client.workflow_template_path(project_id, region, workflow_template)    ,
        'parameters': {"INPUT_BUCKET_URI":input_bucket_uri}
    }

    operation = client.instantiate_workflow_template(request)
    
    result = operation.result()
    print("Launched Dataproc Workflow:", result)