import sys

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

from lib.utils import get_spark_app_config, read_kafka_stream, write_kafka_stream, write_kafka_stream_sink_kafka

from pyspark.sql import SparkSession

from lib.logger import Log4j

from pyspark.sql.functions import col, explode, expr, from_json

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting  pypspark kafka application")

    # Reading from kafka topic

    # kafka_df = read_kafka_stream(spark)

    # kafka_df.printSchema()

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    kafka_df = read_kafka_stream(spark)

    value_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("value"))

    # Batch load without transformation
    # explode_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
    #                                  "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType",
    #                                  "value.DeliveryAddress.City",
    #                                  "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode",
    #                                  "explode(value.InvoiceLineItems) as LineItem")

    # flattened_df = explode_df \
    #     .withColumn("ItemCode", expr("LineItem.ItemCode")) \
    #     .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
    #     .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
    #     .withColumn("ItemQty", expr("LineItem.ItemQty")) \
    #     .withColumn("TotalValue", expr("LineItem.TotalValue")) \
    #     .drop("LineItem")

    # Streaming load with target kafka topic
    notification_df = value_df.select(col("value.InvoiceNumber"), col("value.CustomerCardNo"), col("value.TotalAmount")) \
        .withColumn("LoyalityPoints", col("TotalAmount") * 0.2)
    #  invoice_writer_query = write_kafka_stream(notification_df)

    kakfa_target_df = notification_df.selectExpr("InvoiceNumber as Key",
                                                 """to_json(named_struct(
                                                     'CustomerCardNo',CustomerCardNo,
                                                     'TotalAmount' ,TotalAmount,
                                                     'LoyalityPoints',TotalAmount * 0.2)) as value
                                                 """)

    invoice_kafka_query = write_kafka_stream_sink_kafka(kakfa_target_df)
    logger.info("Completing  pyspark kafka application")
    invoice_kafka_query.awaitTermination()
