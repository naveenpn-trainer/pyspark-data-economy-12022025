from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("Demo App")
             .master("local")
             .config("spark.jars","../resources/lib/jdbc/mysql-connector-java-8.0.28.jar")
             .getOrCreate())

    jdbc_properties ={
        "user":"root",
        "password":"qwerty",
        "driver":"com.mysql.cj.jdbc.Driver"
    }
    df = spark.read.jdbc(url="jdbc:mysql://localhost:3306/crime_analysis",
                         table="crime",
                         properties=jdbc_properties)
    df.show()
    df.printSchema()