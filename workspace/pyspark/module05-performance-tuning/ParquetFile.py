from pyspark.sql import SparkSession
import pyarrow.parquet as pq

spark = SparkSession.builder.appName("Demo APp").master("local").getOrCreate()
df = spark.read.parquet("../resources/dataset/users/parquet_format/users.parquet")
df.show()
print(df.count())

parquet = pq.ParquetFile("../resources/dataset/users/parquet_format/users.parquet")
print(parquet.metadata.num_rows)
