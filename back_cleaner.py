# Python imports
import re
import time
import sys

# A Spark Session is how we interact with Spark SQL to create Dataframes
from pyspark.sql import SparkSession

# PySpark function for replacing characters using a regex.
# Use this to remove newline characters.
from pyspark.sql.functions import regexp_replace, col

# Library for interacting with Google Cloud Storage
from google.cloud import storage

# This will help catch some PySpark errors
from py4j.protocol import Py4JJavaError

# Create a SparkSession under the name "stackoverflow". Viewable via the Spark UI
spark = SparkSession.builder.appName("stackoverflow").getOrCreate()

bucket_name = sys.argv[1]
print(f"name of bucket is {bucket_name}")

# Establish a tag to process
tag = 'c++'

# Set Google Cloud Storage temp location
path = "tmp" + str(time.time())

table= "bigquery-public-data.stackoverflow.posts_questions"


try:
    df = spark.read.format('bigquery').option('table', table).load()
except Py4JJavaError:
    print(f"{table} does not exist. ")
    sys.exit(0)

filtered_data = (
    df
    .select(
        regexp_replace(col("title"), "\n", " "),
        regexp_replace(col("body"), "\n", " ")
    )
    .where(df.tags == tag)
)
tmp_output_path = "gs://" + bucket_name + "/" + path 


(
    filtered_data
    # Data can get written out to multiple files / partition.
    # This ensures it will only write to 1.
    .coalesce(1)
    .write
    # Gzip the output file
    .options(codec="org.apache.hadoop.io.compress.GzipCodec")
    # Write out to csv
    .csv(tmp_output_path)
)


regex = "part-[0-9a-zA-Z\-]*.csv.gz"
new_path = "/".join(["stackoverflow_question_posts.csv.gz"])


# Create the storage client
storage_client = storage.Client()

# Create an object representing the original bucket
source_bucket = storage_client.get_bucket(bucket_name)

# Grab all files in the source bucket. Typically there is also a _SUCCESS file
# inside of the directory, so make sure to find just a single csv file.
blobs = list(source_bucket.list_blobs(prefix=path))

# Locate the file that represents our partition. Copy to new location.
for blob in blobs:
    if re.search(regex, blob.name):
        source_bucket.copy_blob(blob, source_bucket, new_path)

# Lastly, delete the temp directory.
for blob in blobs:
    blob.delete()