#!/bin/bash
 
# Create Dataproc Ephemeral Cluster with CLI commands   Manually

# Troubleshoot or any changes to command  "gcloud dataproc clusters create --help" gives info about arguments 
 
#--bucket stage-bucket-for-dataproc-ip-asia-south1 \
#--temp-bucket temp-bucket-for-dataproc-ip-asia-south1 \
#--zone asia-east1-a \

gcloud dataproc clusters create spark-cluster-12e3  \
  --project lokesh-mp \
  --region asia-east2 \
  --single-node \
  --master-machine-type n1-standard-2 \
  --master-boot-disk-size 300 \
  --image-version 2.0-debian10 \
  --scopes cloud-platform  \
  --tags spark-job-dev \
  --initialization-actions gs://goog-dataproc-initialization-actions-asia-east2/connectors/connectors.sh \
  --metadata biquery-connector-version=1.2.0   \
  --metadata gcs-connector-version=1.9.11 \
  --metadata spark-bigquery-connector-version=0.21.0  \
  --metadata bigquery-connector-url=gs://pah/to/custom/hadoop/bigquery/connector.jar  \
  --metadata spark-bigquery-connector-url=gs://path/to/custom/spark/bigquery/connector.jar




#To submit a PySpark job, run:

#$ gcloud dataproc jobs submit pyspark --cluster my-cluster  my_script.py
			
			
gcloud dataproc jobs submit pyspark gs://dataproc-pyspark-jobs-buckets/parquet_gcs_to_bq.py --cluster spark-cluster-12e3 --region asia-east2


#To delete a cluster, run:

#$ gcloud dataproc clusters delete my-cluster --region=us-central1
		
gcloud dataproc clusters delete spark-cluster-12e3 --region asia-east2