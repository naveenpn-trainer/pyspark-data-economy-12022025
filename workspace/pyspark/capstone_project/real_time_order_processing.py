from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    # Step 01 : Created SparkSession (MongoDB URL, Loaded MongoDB drivers)
    jars = "../resources/lib/kafka/commons-pool2-2.11.1.jar,../resources/lib/kafka/kafka-clients-2.1.1.jar,../resources/lib/kafka/spark-sql-kafka-0-10_2.12-3.2.1.jar,../resources/lib/kafka/spark-token-provider-kafka-0-10_2.12-3.4.1.jar,../resources/lib/mongodb/mongodb-driver-sync-4.3.1.jar,../resources/lib/mongodb/mongodb-driver-core-4.3.1.jar,../resources/lib/mongodb/mongo-spark-connector_2.12-10.1.1.jar,../resources/lib/mongodb/bson-4.3.1.jar"
    spark = (SparkSession
             .builder
             .appName("Real time order processing")
             .config("spark.jars",jars)).getOrCreate()

    # Step 02 : Load Customer dataset
    customer_df = spark.read.csv("../resources/dataset/customers.csv",
                                 header=True,
                                 inferSchema=True)

    customer_df.show()

    # Step 03 : Read orders data from Kafka
    kafka_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers","localhost:9092")
                 .option("subscribe","food-orders")
                 .load())

    orders_df = kafka_df.select(col("value").cast(StringType()).alias("json_data"))

    ORDER_SCHEMA = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("restaurant_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_total", DoubleType()),
        StructField("timestamp", StringType())
    ])

    orders_df = orders_df.select(from_json(col("json_data"),ORDER_SCHEMA).alias("data")).select("data.*")

    # Convert timestamp field to TimestampType
    orders_df = orders_df.withColumn("order_timestamp", to_timestamp(col("timestamp"),"yyyy-MM-dd HH:mm:ss"))

    enriched_orders = (orders_df.withColumn("order_date", date_format(col("order_timestamp"),"yyyy-MM-dd"))
                       .withColumn("order_hour", date_format(col("order_timestamp"),"HH"))
                       )

    # Join between streaming orders and static customers
    enriched_orders = enriched_orders.join(customer_df,"customer_id", "left")

    # Detect High value orders
    enriched_orders = enriched_orders.withColumn("high_value_order",when(col("order_total")>100,True)
                                                 .otherwise(False)
                                                 )

    # Detect Fraudulent orders
    enriched_orders = enriched_orders.withColumn("fraudulent", when(col("order_total")<=0, True)
                                                 .otherwise(False))


    # Detect peak hour orders
    enriched_orders = enriched_orders.withColumn("peak_hour",
                                                 when(
                                                     (col("order_hour").between("12","14")) |
                                                     (col("order_hour").between("19","21")),
                                                    True)
                                                 .otherwise(False)
                                                 )

    result_df = enriched_orders.select("order_id","name","location","order_timestamp","order_date","order_hour","high_value_order","fraudulent","peak_hour")

    # Write data to MongoDB
    (result_df
     .writeStream
     .format("mongodb")
     .option("spark.mongodb.write.connection.uri","mongodb://localhost:27018/food_delivery.enriched_orders")
     .option("database","food_delivery")
     .option("collection","enriched_orders")
     .option("checkpointLocation","/tmp/mongdb_checkpoint")
     .outputMode("append")
     ).start().awaitTermination()



