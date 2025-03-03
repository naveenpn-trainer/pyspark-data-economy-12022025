from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, array_contains, lit

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Complex Datatypes").master("local").getOrCreate()

    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 header=True,
                                 sep="|",
                                 inferSchema=True,
                                 quote="'")
    employee_df.show()
    employee_df.printSchema()

    derived_df = (employee_df.withColumn("skills", split(col("col_skills"), ","))
                  .withColumn("previous_expected_salary",
                              split(col("col_previous_expected_salary"), ",").cast("array<int>"))
                  .drop("col_skills", "col_previous_expected_salary")
                  .select(col("col_name").alias("name"),
                          col("col_exp"),
                          col("skills"),
                          col("previous_expected_salary")))
    derived_df.show(truncate=False)
    derived_df.printSchema()

    derived_df.filter(array_contains(col("skills"), "PySpark" and "Kafka")).select("name", "skills").show(
        truncate=False)
    # 1. Fetc all records whose previous salary is more then their expected salary
    # 2. Fetch max salary for reach record and increment by 30%
    # result_df =  (derived_df.withColumn("previous_salary", col("previous_expected_salary").getItem(0))
    #  .withColumn("expected_salary",
    #              col("previous_expected_salary").getItem(
    #                  1)).show())

    # (derived_df.withColumn("exploded_salaries", explode(col("previous_expected_salary")))).show()
    # result_df.filter((array_contains(col("skills"),"PySpark")) & (array_contains(col("skills"),"Kafka"))).select("name","skills").show(truncate=False)


    # skills_to_hire = ["PySpark","Kafka"]
    # for skill in skills_to_hire:
    #     derived_df = derived_df.filter(array_contains(col("skills"),skill))