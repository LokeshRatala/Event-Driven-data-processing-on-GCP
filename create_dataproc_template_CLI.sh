
# Create Dataproc Template using Command line 
# Following Google’s suggested process, we create a workflow template using the workflow-templates create command.
# https://cloud.google.com/dataproc/docs/concepts/workflows/use-workflows#gcloud-command

gcloud dataproc workflow-templates create nyc-event-driven-data-processing-dataproc-temp \
    --region=us-central1

# Configuring or selecting a cluster

gcloud dataproc workflow-templates set-managed-cluster nyc-event-driven-data-processing-dataproc-temp \
    --cluster-name nyc-dataprocessing-cluster1\
    --region us-central1 \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size 100 \
    --image-version 2.0-debian10 \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 100 \
    --image-version 2.0-debian10 \
    --scopes cloud-platform  \
    --tags spark-job-dev \
    --initialization-actions gs://goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh \
    --metadata biquery-connector-version=1.2.0   \
    --metadata gcs-connector-version=1.9.11 \
    --metadata spark-bigquery-connector-version=0.21.0  \
    --metadata bigquery-connector-url=gs://pah/to/custom/hadoop/bigquery/connector.jar  \
    --metadata spark-bigquery-connector-url=gs://path/to/custom/spark/bigquery/connector.jar


#add job to template

gcloud dataproc workflow-templates add-job pyspark \
    gs://dataproc-pyspark-jobs-buckets/pyspark-nyc-data-process.py \
    --region=us-central1 \
    --step-id=nyc-data-processing1 \
    --workflow-template=nyc-event-driven-data-processing-dataproc-temp \
    -- INPUT_BUCKET_URI  #JOB_ARGS


# After this we need Parameterize the input bucket variable to pass to the workflow template for cloud function  go through this below page 

# https://cloud.google.com/dataproc/docs/tutorials/workflow-function#parameterize_the_workflow_template


