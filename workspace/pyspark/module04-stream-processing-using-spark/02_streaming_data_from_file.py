
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode
from pyspark.sql.types import *

def write_to_db(df, batch_id):
    connection_url = "jdbc:mysql://localhost:3306/streaming_db"
    jdbc_properties ={
        "user":"root",
        "password":"qwerty",
        "driver":"com.mysql.cj.jdbc.Driver"
    }
    df.write.jdbc(url=connection_url,
                  table="crime_tbl",
                  mode="overwrite",
                  properties=jdbc_properties)

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Streaming data from Socket")
             .master("local")
             .config("spark.sql.shuffle.partitions",2)
             .config("spark.jars","../resources/lib/jdbc/mysql-connector-java-8.0.28.jar").getOrCreate())

    CRIME_SCHEMA = StructType([
        StructField("code", StringType()),
        StructField("region", StringType()),
        StructField("crime_type", StringType()),
    ])

    crime_df = spark.readStream.csv("../resources/dataset/crime_data/input", schema=CRIME_SCHEMA)

    # Incremental Query
    # result_df = crime_df.filter(col("region")=="Downtown")

    # Output Sink (Console)
    # result_df.writeStream.format("console").outputMode("append").start().awaitTermination()
    #result_df.writeStream.start(format="console",
                                # outputMode="append").awaitTermination()

    # Complete Mode
    result_df = crime_df.groupBy(col("region")).count();
    # result_df.writeStream.start(format="console",
    #                             outputMode="complete").awaitTermination()

    (result_df.writeStream.foreachBatch(lambda df, id:write_to_db(df,id))
     .start(outputMode="complete").awaitTermination())