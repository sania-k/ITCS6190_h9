from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
df_raw = spark.readStream \
    .format("socket") \
    .option("host","localhost") \
    .option("port",9999) \
    .load \

# Parse JSON data into columns using the defined schema
df_parsed = df_raw.select(from_json(col("value"), schema).alias("data")) \
            .select(
                col("data.trip_id").alias("trip_id"),
                col("data.driver_id").alias("driver_id"),
                col("data.distance_km").alias("distance_km"),
                col("data.fare_amount").alias("fare_amount"),
                col("data.timestamp").alias("timestamp")
            )

# Convert timestamp column to TimestampType and add a watermark
df_parsed = df_parsed.withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
df_parsed = df_parsed.withWatermark("event_time", "1 minute")

# Compute aggregations: total fare and average distance grouped by driver_id
agg = df_parsed.groupBy(col("driver_id")) \
    .agg(
        sum(col("fare_amount")).alias("total_fare"),
        avg(col("distance_km")).alias("avg_distance")
    )

# Define a function to write each batch to a CSV file
def write_batch_to_csv(batch_df,batch_id):
    (
        batch_df.coalesce(1)
        .write
        .mode("append")    # overwrite since each batch has its own folder
        .option("header", "true")
        .csv(f"./output/task_2/batch_{batch_id}")  # <-- batch_id in folder name
    )   

# Use foreachBatch to apply the function to each micro-batch
query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch_to_csv) \
    .option("checkpointLocation", "./checkpoints/task2_checkpoint") \
    .start()


query.awaitTermination()
