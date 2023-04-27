#Event-Driven-data-processing-cloud-function
Data Pipelines 
Both Batch and Streaming Data Pipelines 

ETL Batch Processing Data Pipeline
Extract From GCS Transformation in Dataproc PySpark and finally Load into BigQuery

we are mainly foucsing on two Concepts 
1. Separation of storage and Compute

instead of storing data in dataproc cluster that is in Hadoop distributed file system we have stored data into a gcs bucket and we have read the data into 
pyspark dataframs or rdd  and then we have applied series of transformation and we have written that data into bigquery

2. Ephemeral Cluster

create data cluster and  run pyspark job then delete data clusters once the job is completed these are the three steps we have automated using shell script or airflow dag 

a) dataproc_ephemeral_cluster_bash_file.sh
b) spark_ephemeral_dag.py


ETL Streaming Processing Data Pipeline

Cloud Function 

we are create Cloud function Triggered by a change to a Cloud Storage bucket and cretae dataproc Workflow Template for processess the file when ever a file arrives in 
GCS bucket and loaded into BigQuery

Cloud_Function_DP_WT_GcsToBigquery.py

