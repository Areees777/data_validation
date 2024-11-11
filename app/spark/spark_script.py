from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Input data") \
        .getOrCreate()

    file_path = "http://hadoop:50070/data/input.json"
    df = spark.read.parquet(file_path)
    print(f"Count = {df.count()}")

if __name__ == "__main__":
    main()