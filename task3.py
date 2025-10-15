# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, avg, sum, window
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

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
df_windowed = df_parsed.groupBy(
    window(col("event_time", "5 minutes", "1 minute")) \
    .agg(sum(col("fare_amount")).alias("sum_fare_amount"))
)

# Extract window start and end times as separate columns
df_windowed = df_windowed.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("sum_fare_amount")
)

# Define a function to write each batch to a CSV file with column names
def write_batch_to_csv(batch_df,batch_id):
    (
        batch_df.coalesce(1)
        .write
        .mode("append")    # overwrite since each batch has its own folder
        .option("header", "true")
        .csv(f"./output/task_3/batch_{batch_id}")  # <-- batch_id in folder name
    )   

# Save the batch DataFrame as a CSV file with headers included    
# Use foreachBatch to apply the function to each micro-batch
query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch_to_csv) \
    .option("checkpointLocation", "./checkpoints/task3_checkpoint") \
    .start()

query.awaitTermination()
