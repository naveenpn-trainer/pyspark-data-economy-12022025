from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, array_contains, lit, array_min, least, when

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Complex Datatypes").master("local").getOrCreate()

    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 header=True,
                                 sep="|",
                                 inferSchema=True,
                                 quote="'")
    employee_df.show()
    employee_df.printSchema()
    result_df = (employee_df.withColumn("skills", split(col("col_skills"), ","))
                 .withColumn("actual_expected_salary", split(col("col_actual_expected_salary"), ","))
                 .drop("col_skills", "col_actual_expected_salary"))

    result_df.show()

    # df_01 = (result_df.withColumn("actual_salary", col("actual_expected_salary").getItem(0))
    #          .withColumn("expected_salary",col("actual_expected_salary").getItem(1)))
    #
    # df_01.show()

    (result_df.withColumn("base_salary", array_min(col("actual_expected_salary")))
     .withColumn("offered_salary",
                 when(array_contains(col("skills"), "PySpark"), col("base_salary") + col("base_salary") * 0.3))).show()
