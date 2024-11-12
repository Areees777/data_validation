import logging
from pyspark.sql import DataFrame


def write_to_hdfs(df: DataFrame, file_path: str, file_format: str, file_save_mode: str) -> None:
    """
    Function to write a dataframe into hdfs
    Parameters:
    ----------
    df: DataFrame
        Input dataframe
    file_path: str
        File path where the data is gonna be saved
    file_format: str
        Desired file format
    file_save_mode: str
        Write mode
    """
    try:
        df.write \
            .format(file_format) \
            .mode(file_save_mode.lower()) \
            .save(file_path)
        logging.info(f"Data written to {file_path} in {file_format} format")
    except IOError as e:
        logging.error(f"Error writing to HDFS {file_path}: {str(e)}")


def write_to_kafka(df: DataFrame, topic: str, kafka_url: str) -> None:
    """
    Function to write a dataframe into kafka topic
    Parameters:
    ----------
    df: DataFrame
        Input dataframe
    topic: str
        Target topic where the data is gonna be ingested
    """
    try:
        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_url) \
            .option("topic", topic) \
            .save()
        logging.info(f"Data written to Kafka topic {topic}")
    except IOError as e:
        logging.error(f"Error writing to Kafka topic {topic}: {str(e)}")
