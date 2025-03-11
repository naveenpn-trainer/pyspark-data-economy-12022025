
from pyspark.sql import SparkSession
if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    print(type(spark))

    sc = spark.sparkContext
    print(type(sc))

    numbers_rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    print(type(numbers_rdd))