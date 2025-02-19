from pyspark.sql import SparkSession
if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    sc = spark.sparkContext
    users_rdd = sc.textFile("../resources/dataset/users/csv_format/users_001.csv")
    header = users_rdd.first()

    users_rdd_wo = users_rdd.filter(lambda e:e!=header)
    print(f"Number of records: {users_rdd_wo.count()}")
    rdd_01 = users_rdd_wo.map(lambda record:record.split(",")[0]+","+record.split(",")[3])

    for record in rdd_01.take(5):
        print(record)

