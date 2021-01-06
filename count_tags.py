# These allow us to create a schema for our data
from pyspark.sql.types import StructField, StructType, StringType, LongType

# A Spark Session is how we interact with Spark SQL to create Dataframes
from pyspark.sql import SparkSession

# This will help catch some PySpark errors
from py4j.protocol import Py4JJavaError

# Create a SparkSession under the name "stackoverflow". Viewable via the Spark UI
spark = SparkSession.builder.appName("stackoverflow").getOrCreate()

# Create a two column schema consisting of a string and a long integer
fields = [StructField("tags", StringType(), True),
          StructField("count", LongType(), True)]
schema = StructType(fields)

# Create an empty DataFrame. We will continuously union our output with this
tags_counts = spark.createDataFrame([], schema)



# In the form of <project-id>.<dataset>.<table>
table = f"bigquery-public-data.stackoverflow.posts_questions"

# If the table doesn't exist we will simply continue and not
# log it into our "tables_read" list
try:
    table_df = (spark.read.format('bigquery').option('table', table)
                .load())
except Py4JJavaError as e:
        raise

# We perform a group-by on tags, aggregating by the count and then
# unioning the output to our base dataframe
tags_counts = (
    table_df.groupBy("tags").count().union(tags_counts)
)



# From our base table, we perform a group-by, summing over the counts.
# We then rename the column and sort in descending order both for readability.
# show() will collect the table into memory output the table to std out.

tags_counts.groupBy("tags").sum("count").withColumnRenamed("sum(count)", "count").sort("count", ascending=False).show()
