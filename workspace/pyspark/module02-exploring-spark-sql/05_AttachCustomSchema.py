from pyspark.sql import SparkSession
from pyspark.sql.types import *
if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    EMPLOYEE_SCHEMA = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("experience", IntegerType()),
        StructField("gender", StringType()),
        StructField("dob", TimestampType()),
        StructField("company", StringType()),
        StructField("designation", StringType()),
        StructField("doj", StringType()),
        StructField("skills", StringType()),
        StructField("prev_exp_salary", StringType()),
    ])
    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 sep="|",
                                 header=True,
                                 schema=EMPLOYEE_SCHEMA,
                                 quote="'")

    employee_df.show()