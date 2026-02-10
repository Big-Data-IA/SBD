from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SPARK_S3_LOCALSTACK") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Read CSV from shared volume
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/opt/spark-data/users.csv")

# Write to S3 (LocalStack)
df.write \
    .mode("overwrite") \
    .csv(
        "s3a://bucket-sbd/output",
        header=True
    )

spark.stop()
