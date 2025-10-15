from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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
    .load() \

# Parse JSON data into columns using the defined schema
df_parsed = df_raw.select(from_json(col("value"), schema).alias("data")) \
            .select(
                col("data.trip_id").alias("trip_id"),
                col("data.driver_id").alias("driver_id"),
                col("data.distance_km").alias("distance_km"),
                col("data.fare_amount").alias("fare_amount"),
                col("data.timestamp").alias("timestamp")
            )


def write_batch(batch_df, batch_id):
    # Write to CSV
    batch_df.coalesce(1).write \
        .mode("append") \
        .option("header", "true") \
        .csv(f"./output/task_1/batch_{batch_id}")
    
    # Also print to console
    print(f"Batch {batch_id}:")
    batch_df.show(truncate=False)

query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "./checkpoints/task1_checkpoint") \
    .start()


query.awaitTermination()
