from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SPARK_S3_LOCALSTACK")

logger.info("Iniciando SparkSession...")

spark = (
    SparkSession.builder
    .appName("SPARK_S3_LOCALSTACK")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.791")
    .getOrCreate()
)

logger.info("SparkSession iniciada correctamente.")

h_conf = spark.sparkContext._jsc.hadoopConfiguration()
h_conf.setInt("fs.s3a.connection.timeout", 60000)
h_conf.setInt("fs.s3a.socket.timeout", 60000)

# Leer CSV
input_path = "/opt/spark-data/users.csv"
logger.info(f"Leyendo datos desde: {input_path}")
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Filtrado
df_filtered = df.filter(df["age"] > 18)
logger.info(f"Filas después del filtro (>18 años): {df_filtered.count()}")

# Escribir en S3 (LocalStack)
output_path = "s3a://bucket-bda/lol"
logger.info(f"Escribiendo datos en: {output_path}")

df_filtered.write.mode("overwrite").option("header", True).csv(output_path)

logger.info("Job completado con éxito.")
spark.stop()
logger.info("SparkSession detenida.")
