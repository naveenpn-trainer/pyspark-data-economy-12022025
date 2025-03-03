from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Struct Type Demo").master("local").getOrCreate()

    product_df = spark.read.json(path="../resources/dataset/product_Information_001.json",
                                 multiLine=True)
    product_df.show()
    product_df.printSchema()

    product_df.select(col("details")).show(truncate=False)