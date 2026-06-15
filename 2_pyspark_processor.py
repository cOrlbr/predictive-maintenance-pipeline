import os
import sys

# 1. Kill the System Spark Ghost (Forces it to use the clean 'venv' Spark)
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# 2. Hardcode the Local Network and Linux Java Engine
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# 3. Force Spark to use the Virtual Environment's Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

print("Initializing PySpark Streaming Context...")

print("Initializing PySpark Streaming Context...")

spark_version = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"
cassandra_package = "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0"

spark = SparkSession.builder \
    .appName("PredictiveMaintenance") \
    .config("spark.jars.packages", f"{kafka_package},{cassandra_package}") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("UDI", IntegerType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Air temperature [K]", DoubleType(), True),
    StructField("Process temperature [K]", DoubleType(), True),
    StructField("Rotational speed [rpm]", IntegerType(), True),
    StructField("Torque [Nm]", DoubleType(), True),
    StructField("Tool wear [min]", IntegerType(), True)
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telemetry_stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

analyzed_stream = parsed_stream.withColumn(
    "Failure_Risk",
    when((col("Torque [Nm]") > 60) & (col("Rotational speed [rpm]") < 1400), "HIGH RISK")
    .when((col("Torque [Nm]") > 50), "WARNING")
    .otherwise("NORMAL")
)

# Aligning PySpark output EXACTLY to the Cassandra Schema
clean_stream = analyzed_stream.select(
    col("Product ID").alias("machine_id"),
    current_timestamp().alias("timestamp"),
    col("Torque [Nm]").alias("torque"),
    col("Rotational speed [rpm]").alias("rotational_speed"),
    col("Failure_Risk").alias("risk_level")
)

print("Routing live telemetry to Apache Cassandra...")

query = clean_stream.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "predictive_maintenance") \
    .option("table", "telemetry") \
    .option("checkpointLocation", "./checkpoints") \
    .start()

query.awaitTermination()
