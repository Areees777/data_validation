from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Example Spark Job") \
        .getOrCreate()

if __name__ == "__main__":
    main()