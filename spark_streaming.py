from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

topic_name = "bike-station-info"
bootstrap_servers = "172.31.30.11:9092,172.31.39.201:9092,172.31.52.183:9092"

spark = (
    SparkSession
    .builder
    .appName("kafka_streaming")
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')
    .master("local[*]")
    .getOrCreate()
)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic_name)
    .option("startingOffsets", "earliest")
    .load()
)

schema = T.StructType([
    T.StructField("rackTotCnt", T.IntegerType()),
    T.StructField("stationName", T.StringType()),
    T.StructField("parkingBikeTotCnt", T.IntegerType()),
    T.StructField("shared", T.FloatType()),
    T.StructField("stationLatitude", T.FloatType()),
    T.StructField("stationLongitude", T.FloatType()),
    T.StructField("stationId", T.StringType()),
    T.StructField("timestamp", T.TimestampType()),
])

value_df = kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("value"))

processed_df = value_df.selectExpr(
    "value.rackTotCnt",
    "value.stationName",
    "value.parkingBikeTotCnt",
    "value.shared",
    "value.stationLatitude",
    "value.stationLongitude",
    "value.stationId",
    "value.timestamp"
)

df_console = processed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoint_dir/kafka_streaming") \
    .trigger(processingTime="5 seconds") \
    .start()

df_console.awaitTermination()
