
""" Airflow DAG that creates a Cloud Dataproc cluster, runs the PySpark Job, and deletes the cluster.

This DAG relies on three Airflow variables
https://airflow.apache.org/docs/apache-airflow/stable/concepts/variables.html
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_region - Google Compute Engine region where Cloud Dataproc cluster should be
  created.
"""

from datetime import datetime,timedelta
import os

from airflow.models  import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateClusterOperator,
                                                               DataprocSubmitJobOperator,
                                                               DataprocDeleteClusterOperator
                                                            )
from airflow.utils import trigger_rule

# Output file for Cloud Dataproc job.
# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
output_file = os.path.join(
    '{{ var.value.gcs_bucket }}', 'wordcount',
    datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep
# Path to Hadoop wordcount example available on every Dataproc cluster.

# Variables to pass to Cloud Dataproc.
PROJECT_ID = 'lokesh-mp' # GCP project name
CLUSTER_NAME = 'spark-cluster-f34r' # Dataproc cluster name
REGION  = 'asia-south2'
ZONE = 'asia-south2-a'
PYSPARK_FILE = 'gs://dataproc-pyspark-jobs-buckets/parquet_gcs_to_bq.py'   # where your script is, in a gs bucket usually

today_date = datetime.today().strftime('%Y-%m-%d')
input_file = 'gs://project-data-bucket-1/yellow_tripdata_{}.parquet'.format(today_date) # todAY arrived file 


zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(PROJECT_ID, ZONE)

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME },
    "pyspark_job": {
        'main_python_file_uri': PYSPARK_FILE,
            'args': [
                      input_file, # argparser requires '='
            ]
    },
}

#https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig  
# refer this site changes in cluster data
CLUSTER_CONFIG= {
        #
        #'cluster_name': CLUSTER_NAME, 
            #config_bucket and temp_bucket optional because very first time dag run dataproc
            # create both buckets but second time runs it wont create buckets it use existing buckets cause of same region it creates again and again 
            #'config_bucket': staging_bucket,
            #'temp_bucket': temp_bucket,
            #'project_id': PROJECT_ID,
            'gce_cluster_config': {                
                'zone_uri': zone_uri,
                'service_account_scopes': ['https://www.googleapis.com/auth/cloud-platform'],#enable the other service with dataproc
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
                                'boot_disk_size_gb': 200,
                                'num_local_ssds': 0
                }
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-2',
                'disk_config': {
                                'boot_disk_type': 'pd-standard',
                                'boot_disk_size_gb': 200,
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
                    'executable_file': 'gs://goog-dataproc-initialization-actions-{}/connectors/connectors.sh'.format(REGION),
                    'execution_timeout': {'seconds': 1800 }
                }
            ]
    }
yesterday = datetime.combine(
    datetime.today() - timedelta(days= 1),
    datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': PROJECT_ID,
    'region': REGION,

}


with DAG(
        'Spark_ephemeral_cluster',
        # Continue to run DAG once per day
        schedule_interval=timedelta(days=1), 
        # every day at 2 am dag runs using corntab time 
        # schedule_interval = '0 2 * * *',
        default_args=default_dag_args) as dag:

    start_task = DummyOperator(
        task_id = 'Start'
    )

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
        #The Airflow engine passes a few variables by default that are accessible in all templates
        project_id = PROJECT_ID,
        cluster_name = CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        region= REGION,
        labels = {'dev' : 'development'}
    )

    # Run the  PySpark job on the Cloud Dataproc cluster
    # master node.
    run_dataproc_pyspark = DataprocSubmitJobOperator(
        task_id='run_dataproc_pyspark',
        project_id = PROJECT_ID,
        job=PYSPARK_JOB,
        region= REGION,)

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id = PROJECT_ID,
        cluster_name= CLUSTER_NAME,
        region= REGION,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)


    end_task = DummyOperator(
        task_id = 'End'
    )
    # Define DAG dependencies.
    start_task >> create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster >> end_task