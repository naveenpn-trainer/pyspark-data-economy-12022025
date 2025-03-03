from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max

if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 sep="|",
                                 header=True,
                                 inferSchema=True,
                                 quote="'")
    # employee_df.groupBy(col("col_desig")).count().show()
    employee_df.groupBy(col("col_desig")).agg(count("*").alias("total_count"),
                                              max("col_exp").alias("max_exp")).show()
