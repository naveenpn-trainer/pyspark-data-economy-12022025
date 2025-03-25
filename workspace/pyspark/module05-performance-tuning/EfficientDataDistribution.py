from pyspark.sql import SparkSession

def get_partition_info(df):
    return df.rdd.mapPartitionsWithIndex(lambda id,iter:[(id, list(iter))]).collect()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Demo APp").master("local").getOrCreate()
    print(spark.conf.get("spark.sql.files.maxPartitionBytes"))

    spark.conf.set("spark.sql.files.maxPartitionBytes",150)
    df = spark.read.csv(path="../resources/dataset/users/csv_format/users_001.csv",
                        header=True)
    print(f"Number of partitions: {df.rdd.getNumPartitions()}")
    for partition , data in get_partition_info(df):
        print(f"Partition: {partition}: {data}")
    print("Repartitioning")
    df_repartitioned = df.repartition(2)

    for partition , data in get_partition_info(df_repartitioned):
        print(f"Partition: {partition}: {data}")

