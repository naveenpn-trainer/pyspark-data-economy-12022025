from pyspark.sql import SparkSession

'''
Modes
1. PERMISSIVE (Default)
2. DROPMALFORMED
3. FAILFAST
'''
if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    df = spark.read.json(path="../resources/dataset/access_logs.json",
                         mode="FAILFAST")
    df.show()