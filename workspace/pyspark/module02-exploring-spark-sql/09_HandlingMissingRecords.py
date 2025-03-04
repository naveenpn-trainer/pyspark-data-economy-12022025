from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Handling Missing Records").master("local").getOrCreate()

    employee_df = spark.read.csv(path="../resources/dataset/employee.csv",
                                 header=True,
                                 inferSchema=True,
                                 sep="|",
                                 quote="'")
    employee_df.show()
    employee_df.printSchema()
    '''
        Drop entire record even if any one of the column is null
    '''
    # employee_df.na.drop().show()

    # Drop entire record only if mandatory columns as null
    # madatory_columns = ["id","name"]
    # employee_df.na.drop(subset=madatory_columns).show()

    # Fill all null values with a default value
    # employee_df.na.fill(0).na.fill("Not Defined").show()

    # Fill name column with 'Anonymous' and company with 'Not Defined'
    # (employee_df.na.fill("Not Defined", subset=["company","doj"])
    #  .na.fill("Anonymous", subset=["name"])).show()

    # employee_df.na.fill({"company": "Not Defined",
    #                      "doj": "Not Defined",
    #                      "name": "Anonymous",
    #                      "id": -1}).show()

    '''
        Fill null values with mean, avg
    '''
    from pyspark.sql.functions import avg
    print(employee_df.select(avg("exp")).collect())
    avg_age = int(employee_df.select(avg("exp")).collect()[0][0])
    employee_df.na.fill({
        "exp":avg_age
    }).show()

