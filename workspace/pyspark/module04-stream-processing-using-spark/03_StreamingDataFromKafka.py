from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
if __name__ == '__main__':
    jars = "../resources/lib/kafka/commons-pool2-2.11.1.jar,../resources/lib/kafka/kafka-clients-2.1.1.jar,../resources/lib/kafka/spark-sql-kafka-0-10_2.12-3.2.1.jar,../resources/lib/kafka/spark-token-provider-kafka-0-10_2.12-3.4.1.jar"
    spark = (SparkSession.builder.appName("Streaming data from Socket")
             .master("local")
             .config("spark.sql.shuffle.partitions",2)
             .config("spark.jars",jars).getOrCreate())

    kafka_configuration = {
        "kafka.bootstrap.servers":"localhost:9092",
        "subscribe":"my-demo-topic"
    }
    input_df = spark.readStream.load(format="kafka",
                                     **kafka_configuration)
    # input_df = (spark.readStream.format("kafka")
    #             .option("kafka.bootstrap.servers","localhost:9092")
    #             .option("subscribe","my-demo-topic").l)
    input_df.printSchema()
    result_df = input_df.select(col("value").cast(StringType()))
    result_df.writeStream.start(format="console",
                               outputMode="append").awaitTermination()