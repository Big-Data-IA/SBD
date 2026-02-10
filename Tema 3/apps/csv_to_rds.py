from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CSV_TO_RDS") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Read CSV
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/opt/spark-data/users.csv")

# Clean column names (VERY important for JDBC)
df = (
    df.withColumnRenamed("Name", "name")
      .withColumnRenamed("Address", "address")
      .withColumnRenamed("Age", "age")
      .withColumnRenamed("Salary", "salary")
)

# JDBC connection
jdbc_url = "jdbc:postgresql://localstack:5432/postgres"

connection_properties = {
    "user": "spark",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver"
}

# Write to RDS
df.write \
  .mode("overwrite") \
  .jdbc(
      url=jdbc_url,
      table="users",
      properties=connection_properties
  )

spark.stop()
