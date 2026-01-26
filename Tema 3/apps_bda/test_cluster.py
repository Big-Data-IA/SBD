import os
import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def count_words_df(filename):
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("WordCount") \
        .getOrCreate()
    
    # Read file as DataFrame
    df = spark.read.text(filename)

    # Split lines into words, explode to get one word per row
    words_df = df.select(explode(split(col("value"), " ")).alias("word"))

    # Count words
    word_counts = words_df.groupBy("word").count()

    # Save output
    output_path = f"/opt/spark-data/{get_random_string(8)}"
    word_counts.write.mode("overwrite").csv(output_path)

    print(f"Word count saved in: {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    filename = "/opt/spark-data/random_text.txt"
    count_words_df(filename)


#$SPARK_HOME/bin/spark-submit /opt/spark-apps/test_cluster.py
