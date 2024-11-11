from pyspark.sql import SparkSession

def main():
    hdfs_url = "hdfs://hadoop:9000"
    spark = SparkSession.builder \
        .appName("Input data") \
        .config("spark.hadoop.fs.defaultFS", hdfs_url) \
        .getOrCreate()

    file_path = "hdfs://hadoop:9000/data/input.json"
    df = spark.read \
        .format("json") \
        .option("multiLine", "true") \
        .load(file_path)
    
    print(f"Count = {df.count()}")

if __name__ == "__main__":
    main()