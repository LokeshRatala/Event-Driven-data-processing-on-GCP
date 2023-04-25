# Cloud Function for Dataproc Ephemeral Cluster workflow template

from google.cloud import dataproc_v1 as dataproc

# Function dependencies, for example:
# package>=version
# below requirement  we need to deploy cloud  function 
''' google-cloud-dataproc==5.0.3'''


project = 'lokesh-mp' # GCP project name
cluster_name = 'cluster-2err34'  # Dataproc cluster name
region = 'asia-east1'
zone = 'asia-east1-a'
pyspark_file = 'gs://dataproc-pyspark-jobs-buckets/parquet_gcs_to_bq.py'  # where your script is, in a gs bucket usually

# if we what to create stage bucket ,temp bucket and use only this 
# but if we dont mention dataproc create it self 
staging_bucket = 'stage-bucket-for-dataproc-ip-asia-southeast1'    # staging bucket for dataproc but it should be in same region which dataproc created 
temp_bucket = 'temp-bucket-for-dataproc-ip-asia-southeast1' # temp bucket for dataproc but it should be in same region which dataproc created 


def dp_wt_GcsToBigQuery(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    file_name = event['name']
    file_arrived_bucket_name = event['bucket']
    print(f'Processing file {file_name} in bucket {file_arrived_bucket_name}.')
	
	
    # Create a client with the endpoint set to the desired cluster region.
    dataproc_workflow_temp_client = dataproc.WorkflowTemplateServiceClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(project, zone)
    
	# set project and region which your template 
    parent = "projects/{}/regions/{}".format(project, region)
    
	# Set the template cluster config and Job to run.
    template = { "placement": {
                    "managed_cluster": {
                        "cluster_name": cluster_name,
                        "config": {
                            "gce_cluster_config": {
                                'zone_uri': zone_uri,
                                'service_account_scopes': ['https://www.googleapis.com/auth/cloud-platform'], #enable the other service with dataproc
                                'metadata': {
								    #connector need to connect with bigquery
                                    'spark-bigquery-connector-version': '0.21.0',  
                                    'biquery-connector-version'  : '1.2.0' 
                                },
                            },
                            'master_config': {
                                'num_instances': 1,
                                'machine_type_uri': 'n1-standard-2',
                                'disk_config': {
                                        'boot_disk_type': 'pd-standard',
                                        'boot_disk_size_gb': 100,
                                        'num_local_ssds': 0
                                }
                            },
                            'worker_config': {
                                'num_instances': 2,
                                'machine_type_uri': 'n1-standard-2',
                                'disk_config': {
                                        'boot_disk_type': 'pd-standard',
                                        'boot_disk_size_gb': 100,
                                        'num_local_ssds': 0
                                }
                            },
                           'software_config': {
                                #'optional_components': ['ANACONDA', ],
                                'image_version': '2.0.61-debian10',
                            },
                            'initialization_actions': [
                                {
								# initialization_actions connector scrpits need to connect with bigquery 
                                'executable_file': 'gs://goog-dataproc-initialization-actions-{}/connectors/connectors.sh'.format(region),
                                'execution_timeout': {'seconds': 1800 }
                                }
                            ]
                        }
                    }
                },
                    
                "jobs": [
                    {
                        "pyspark_job": {
                            "main_python_file_uri": pyspark_file
                        },
                        "step_id": "pyspark-job",
                    }
                ],
    }
    
	# Create the Dataproc cluster Workflow Template.
	
    operation = dataproc_workflow_temp_client.instantiate_inline_workflow_template(
        request={"parent": parent, "template": template}
    )
    result = operation.result()

    print("Workflow ran successfully.")