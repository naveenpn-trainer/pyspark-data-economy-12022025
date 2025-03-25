from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    # dfr = spark.read
    # print(type(dfr))
    # df = dfr.csv(path="../resources/dataset/users/csv_format/users_001.csv")
    # print(type(df))
    # Load CSV File into DataFrame
    # df = spark.read.csv(path="../resources/dataset/users/csv_format/users_001.csv",
    #                     header=True)
    # df.show(n=7, truncate=False)

    # Load Custom Delimited file into DataFrame
    # df = spark.read.csv(path="../resources/dataset/users/delimited_format/users_001.dat",
    #                     header=True,
    #                     sep="|",
    #                     inferSchema=True)
    # df.show(n=5)
    # df.printSchema()

    # Load JSON file into DataFrame
    # df = spark.read.json(path="../resources/dataset/users/json_format/users_003.json",
    #                      multiLine=True)
    # df.show(n=4)
    # # print(df.rdd.getNumPartitions())

    df = spark.read.parquet("../resources/dataset/users/parquet_format/*")
    print(df.count())
