from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Example Spark Job") \
        .master("spark://spark_master:7077") \
        .getOrCreate()

if __name__ == "__main__":
    main()