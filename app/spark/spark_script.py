from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField

def main():
    hdfs_url = "hdfs://hadoop:9000"
    spark = SparkSession.builder \
        .appName("Input data") \
        .config("spark.hadoop.fs.defaultFS", hdfs_url) \
        .getOrCreate()

    file_path = "hdfs://hadoop:9000/data/input.jsonl"
    df = spark.read \
        .format("json") \
        .load(file_path)
    
    print(f"Count = {df.count()}")
    df.printSchema()

if __name__ == "__main__":
    main()