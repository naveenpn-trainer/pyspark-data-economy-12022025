from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Streaming data from Socket")
             .master("local").config("spark.sql.shuffle.partitions",2).getOrCreate())

    # Read data from socket as input source
    input_df = spark.readStream.load(format="socket",
                                     host="localhost",
                                     port="8888")

    print(input_df.isStreaming)
    # Incremental query
    result_df = (input_df.select(explode(split(col("value"),",")).alias("words"))
                 .groupBy(col("words")).count())



    # Outsink
    result_df.writeStream.format("console").outputMode("complete").start().awaitTermination()