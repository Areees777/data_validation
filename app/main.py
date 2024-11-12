import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame, functions as F
from dependencies.spark_wrapper import SparkWrapper
from configs.settings import (
    SPARK_CONF,
    SPARK_JAR_PACKAGES,
    HDFS_URL,
    PROGRAM_METADATA
)

from dependencies.io import write_to_hdfs, write_to_kafka


def apply_validations(df: DataFrame, validations: dict) -> DataFrame:
    """
    Applies validation rules to specified fields in a DataFrame and categorizes records as valid or invalid 
    based on validation outcomes.

    The function checks specified fields for conditions like being empty or null based on a dictionary of validations 
    and logs any validation errors in an array column `arraycoderrorbyfield`. It then splits the DataFrame into valid 
    and invalid records, where valid records have no errors, and invalid records have one or more errors.

    Parameters:
    ----------
    df : DataFrame
        The input PySpark DataFrame on which validations are applied.
    validations : dict
        A dictionary containing validation configurations for each field. Each dictionary entry should specify

    Returns:
    -------
    df_valid : DataFrame
        A DataFrame containing records that passed all validations (with an empty `arraycoderrorbyfield`).
    df_invalid : DataFrame
        A DataFrame containing records that failed one or more validations (with non-empty `arraycoderrorbyfield`).
    """
    # Creating the arraycoderrorbyfield as array
    df = df.withColumn("arraycoderrorbyfield", F.array())

    for v in validations:
        field = v["field"]
        errors = []

        if "notEmpty" in v["validations"]:
            error_condition = F.when(F.col(field) == "", F.struct(F.lit(field).alias("field"), F.lit("Error: empty field").alias("error")))
            errors.append(error_condition)

        if "notNull" in v["validations"]:
            error_condition = F.when(F.col(field).isNull(), F.struct(F.lit(field).alias("field"), F.lit("Error: null field").alias("error")))
            errors.append(error_condition)

        # For each error we are running the error condition and checking it
        # If the error condition result is not null, the error is saved into the array field
        # otherwise the array field will keep as empty. 
        for error in errors:
            df = df.withColumn(
                "arraycoderrorbyfield",
                F.when(
                    error.isNotNull(),
                    F.array_union(F.col("arraycoderrorbyfield"), F.array(error))
                ).otherwise(F.col("arraycoderrorbyfield"))
            )

    # Finally the dataframe is being filtered as invalid or valid depending on the elements
    # inside the array field.
    df_valid = df.filter(F.size(F.col("arraycoderrorbyfield")) == 0).drop("arraycoderrorbyfield")
    df_invalid = df.filter(F.size(F.col("arraycoderrorbyfield")) > 0)

    return df_valid, df_invalid


def add_fields(df: DataFrame, field_name: str, function: str) -> DataFrame:
    """
    Function to add new fields based on the function type
    Parameters:
    ----------
    df: DataFrame
        Input dataframe
    field_name: str
        Field name
    function: str
        Target function to use
    """
    if function == "current_timestamp":
        df = df.withColumn(field_name, F.current_timestamp())
        return df


def data_validation(df: DataFrame, transformations: List[dict], sinks: List[dict]) -> None:
    """
    Function to run the validations, based on transformations and sinks. 
    First of all the data is being separated by valid and invalid, and adding 
    necessary fields, and finally, writting the output into kafka or hdfs based on
    if the data is valid (kafka) or invalid (hdfs).
    Parameters:
    ----------
    df: DataFrame
        Input dataframe
    transformations: dict 
        Transformations dict
    sinks: dict
        Sinks dict
    """
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
    """
    Function to run the validations. Entrypoint.
    Parameters:
    ----------
    spark: SparkSession
    """
    logging.info("Starting data validation")

    for flow in PROGRAM_METADATA["dataflows"]:
        flow["name"]
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
    spark_wrapper = SparkWrapper("Data validation")
    spark_session = spark_wrapper.get_spark_session(SPARK_CONF["conf"], SPARK_JAR_PACKAGES)

    run(spark_session)


if __name__ == "__main__":
    main()
