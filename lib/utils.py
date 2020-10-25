from pyspark import SparkConf
import configparser


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("config/spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def read_kafka_stream(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .option("includeHeaders", "true") \
        .load()


def write_kafka_stream(srcdataframe):
    return srcdataframe.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "target") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()


def write_kafka_stream_sink_kafka(streamingdf):
    return streamingdf.writeStream \
        .format("kafka") \
        .queryName("Flattened Invoice Writer") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "invoices-target") \
        .option("outputMode", "append") \
        .option("checkpointLocation", "chk-point-dir-kafka") \
        .start()
