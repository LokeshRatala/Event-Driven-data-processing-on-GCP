# PySpark job from GCS to BigQuery on Daily Basis for one file 

from pyspark.sql import SparkSession
from datetime import datetime
import time

PROJECT_ID = 'lokesh-mp' # your project ID 
BUCKET_NAME = "project-data-bucket-1"  # bucket which data is coming 
TEMP_BUCKET = "dataproc-pyspark-jobs-buckets" # temporary bucket for spark to write temp data
DATASET_ID = 'NYC_dataset' # dataset name 
TABLE_NAME = 'NYC_taxi_riders' # table name 

# Fixed file name and file formate  but date changes daily "current date"
file_pefix = "yellow_tripdata_"
file_suffix = '.parquet'
today_date = datetime.today().strftime('%Y-%m-%d')

full_filename = '{}{}{}'.format(file_pefix,today_date,file_suffix)


# create SparkSession (sparksession is entry point to the spark functionality )
spark= SparkSession.builder     \
                   .appName("ParquetGcstoBigQuery")  \
                   .getOrCreate()

print("saprksession created")

# temp conf data
spark.conf.set('temporaryGcsBucket',TEMP_BUCKET)

# reading data from GCS
df= spark.read.format("parquet") \
              .load(f"gs://{BUCKET_NAME}/{full_filename}")

#df.printSchema()
#df.count()

# Spark Transformations
df1=df.select(df.VendorID        \
              ,df.tpep_pickup_datetime    \
              ,df.tpep_dropoff_datetime   \
              ,df.passenger_count       \
              ,df.trip_distance     \
              ,df.tip_amount  \
              )

#df1.count()
df1.show(5)
#start = time.time()
print("Write to biquery started")

df1.write.mode('append').format('bigquery') \
         .option('temporaryGcsBucket', TEMP_BUCKET) \
         .option('createDisposition', 'CREATE_IF_NEEDED') \
         .option('table','{}.{}.{}'.format(PROJECT_ID,DATASET_ID,TABLE_NAME)) \
         .save()

print("write to bigquery done")

spark.stop()
