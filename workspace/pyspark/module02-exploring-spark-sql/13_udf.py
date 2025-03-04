from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


# Python UDF
def convert_to_tile_case(field):
    return field.title()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Custom UDF").master("local").getOrCreate()
    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 sep="|",
                                 inferSchema=True,
                                 header=True,
                                 quote="'")
    employee_df.show()

    # If 'e' is not None, apply the convert_to_tile_case python function
    convert_to_title_case_lamba = lambda e:convert_to_tile_case(e) if not e is None else ""

    # TO Use with DataFrame API
    convert_to_title_fn = udf(convert_to_title_case_lamba, StringType())

    # employee_df.select(col("id"), convert_to_title_fn(col("name")).alias("name")).show()
    employee_df.createOrReplaceTempView("employee_vw")

    # Register UDF TO Use with Spark SQL
    spark.udf.register("convert_to_title_fn",convert_to_title_case_lamba, StringType() )
    spark.sql("SELECT id, convert_to_title_fn(name) from employee_vw").show()

    for fn in spark.catalog.listFunctions():
        if fn.className.startswith("org.apache.spark.sql.UDFRegistration"):
            print(fn.name)

    #
    # RuntimeError: Python in worker
    # has
    # different
    # version
    # 3.10
    # than
    # that in driver
    # 3.9, PySpark
    # cannot
    # run
    # # with different minor versions.Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.