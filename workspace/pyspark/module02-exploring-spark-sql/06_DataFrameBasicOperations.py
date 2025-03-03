from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 sep="|",
                                 header=True,
                                 inferSchema=True,
                                 quote="'")
    employee_df.show()
    employee_df.printSchema()

    # Select columns
    # df_01 = employee_df.select("col_id","col_desig")
    # df_01.show()

    # employee_df.select(col("col_id"), col("col_desig").alias("designation")).show()

    # employee_df.filter(col("col_company")=='Cisco').show()

    employee_df.filter(col("col_desig").isin("Developer","Team Lead")).show()