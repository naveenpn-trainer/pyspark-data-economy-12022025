from pyspark.sql import SparkSession


def create_pair_rdd(record):
    fields = record.split(",")
    return fields[3].lower(), 1


if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    sc = spark.sparkContext
    users_rdd = sc.textFile("../resources/dataset/users/csv_format/users_001.csv")
    header = users_rdd.first()

    users_rdd_wo = users_rdd.filter(lambda e: e != header)
    # users_pair_rdd = users_rdd_wo.map(lambda e: create_pair_rdd(e))
    users_pair_rdd = users_rdd_wo.map(create_pair_rdd)

    print(users_pair_rdd.take(10))

    result_rdd = users_pair_rdd.reduceByKey(lambda x, y: x + y)
    result_rdd.saveAsTextFile("../resources/dataset/output_rdd")
