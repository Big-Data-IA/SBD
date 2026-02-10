from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CSV_TO_RDS")

logger.info("Iniciando SparkSession...")

spark = (
    SparkSession.builder
    .appName("CSV_TO_RDS")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

logger.info("SparkSession iniciada correctamente.")

# Leer CSV
input_path = "/opt/spark-data/users.csv"
logger.info(f"Leyendo CSV desde: {input_path}")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

# Limpiar nombres de columnas (muy importante para JDBC)
df = (
    df.withColumnRenamed("Name", "name")
      .withColumnRenamed("Address", "address")
      .withColumnRenamed("Age", "age")
      .withColumnRenamed("Salary", "salary")
)

logger.info("Columnas renombradas correctamente. Mostrando 5 filas de ejemplo:")
df.show(5, truncate=False)

# Conexión JDBC a PostgreSQL
jdbc_url = "jdbc:postgresql://localstack:5432/postgres"
connection_properties = {
    "user": "spark",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver"
}

# Escribir datos en la tabla "users"
logger.info("Escribiendo datos en PostgreSQL...")
df.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="users",
    properties=connection_properties
)

logger.info("Job completado con éxito.")

spark.stop()
logger.info("SparkSession detenida.")
