from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("Demo App")
             .master("local")
             .config("spark.jars","../resources/lib/jdbc/sqlite-jdbc-3.48.0.0.jar")
             .getOrCreate())

    jdbc_properties ={
        "driver":"org.sqlite.JDBC"
    }
    df = spark.read.jdbc(url="jdbc:sqlite:../resources/dataset/window_functions.db",
                         table="employee_wf",
                         properties=jdbc_properties)
    df.show()
    df.printSchema()