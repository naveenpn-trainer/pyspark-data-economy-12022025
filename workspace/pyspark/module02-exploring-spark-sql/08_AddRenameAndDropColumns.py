from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, lit, when

if __name__ == '__main__':
    # Step 01 - Create SparkSession
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 sep="|",
                                 header=True,
                                 inferSchema=True,
                                 quote="'")

    query = """
    CASE
        WHEN col_exp BETWEEN 0 AND 5 THEN 'Junior'
        WHEN col_exp > 5 AND col_exp<10 THEN 'Mid Level'
        WHEN col_exp >=10 THEN 'Senior'
        ELSE 'Experience Not defiled'
        END AS category
    """
    employee_df.selectExpr("*",query).show()

    employee_df.createOrReplaceTempView("employee_vw")
    # spark.sql(query).show()


    # (employee_df.withColumn("category",lit("Not defined"))
    #  .drop("col_skills","col_doj").withColumnRenamed("col_desig","designation").show())

    # employee_df.withColumn("category",when((col("col_exp")<=5) & (col("col_exp")>=0),"Junior")
    #                        .when((col("col_exp")>5) & (col("col_exp")<10),"Mid Level")
    #                        .when(col("col_exp")>=10,"Senior")
    #                        .otherwise("Experience Not Defined")
    #                        ).show()