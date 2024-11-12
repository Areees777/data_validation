import argparse
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from dependencies.spark_wrapper import SparkWrapper
from configs.settings import (
    SPARK_CONF, 
    SPARK_JAR_PACKAGES, 
    HDFS_URL, 
    KAFKA_URL, 
    PROGRAM_METADATA
)
from configs.metadata import METADATA

from dependencies.io import write_to_hdfs, write_to_kafka

def apply_validations(df: DataFrame, validations: dict) -> DataFrame:
    for v in validations:
        field = v["field"]
        if "notEmpty" in v["validations"]:            
            df = df.filter(F.col(field) != "")
        elif "notNull" in v["validations"]:
            df = df.filter(F.col(field).isNotNull())
    return df

def add_fields(df: DataFrame, field_name: str, function: str) -> DataFrame:
    if function == "current_timestamp":
        df = df.withColumn(field_name, F.current_timestamp())
        return df
    
def data_validation(df: DataFrame, transformations: List[dict], sinks: List[dict]) -> None:
    for trnsf in transformations:
        if trnsf["name"] == "validation":
            logging.info(f"Starting with {trnsf['name']} step...")
            validations = trnsf["params"]["validations"]
            df_valid = apply_validations(df, validations)
            # This could be replace by left_anti join by any PK, but 
            # we don't have on this data sample.
            df_invalid = df.subtract(df_valid)
        elif trnsf["name"] == "ok_with_date":
            for field in trnsf["params"]["addFields"]:
                df_valid = add_fields(df_valid, field["name"], field["function"])
    
    for sink in sinks:
        if sink["input"] == "ok_with_date":
            for topic in sink["topics"]:
                write_to_kafka(df_valid, topic)
        elif sink["input"] == "validation_ko":
            for path in sink["paths"]:
                file_path = "".join([HDFS_URL, path, sink["name"]])
                write_to_hdfs(df_invalid, file_path, sink["format"], sink["saveMode"])                

    return df_valid, df_invalid

def run(spark: SparkSession) -> None:
    logging.info("Starting data validation")

    for flow in PROGRAM_METADATA["dataflows"]:
        flow_name = flow["name"]
        for source in flow["sources"]:
            file_path = source["path"]
            full_file_path = "".join([HDFS_URL, file_path])
            print(full_file_path)
            df = spark.read \
                .format("json") \
                .load(full_file_path)
            sinks = flow["sinks"]
            transformations = flow["transformations"]
            data_validation(df, transformations, sinks)
    logging.info("Finished data validation")
    

def main():
    """Function that executes the ETL process on every layer.
    It reads the arguments and takes the environment to be used later
    in the configuration dictionaries. Based on this variable will
    take the corresponding configurations."""
    ap = argparse.ArgumentParser()
    ap.add_argument('-i', '--env')

    args = ap.parse_args()
    environment = args.env

    jar_packages = []

    for package in SPARK_JAR_PACKAGES:
        jar_packages.append(package)

    spark_wrapper = SparkWrapper("Generics JOB ETL")
    spark_session = spark_wrapper.get_spark_session(SPARK_CONF["conf"], jar_packages)

    run(spark_session)

if __name__ == "__main__":
    main()
