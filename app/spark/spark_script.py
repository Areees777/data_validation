import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame, functions as F


HDFS_URL = "hdfs://hadoop:9000"
KAFKA_URL = "kafka:9092"

PROGRAM_METADATA = {
    "dataflows": [{
        "name": "prueba-acceso",
        "sources": [{
                "name": "person_inputs",
                    "path": "/data/input/events/person/*",
                    "format": "JSONL"
                    }
                    ],
        "transformations": [{
            "name": "validation",
            "type": "validate_fields",
                    "params": {
                        "input": "person_inputs",
                        "validations": [{
                            "field": "office",
                            "validations": ["notEmpty"]
                        }, {
                            "field": "age",
                            "validations": ["notNull"]
                        }
                        ]
                    }
        }, {
            "name": "ok_with_date",
                    "type": "add_fields",
                    "params": {
                        "input": "validation_ok",
                        "addFields": [{
                            "name": "dt",
                            "function": "current_timestamp"
                        }
                        ]
                    }
        }
        ],
        "sinks": [
            {
                "input": "ok_with_date",
                "name": "raw-ok",
                "topics": [
                        "person"
                ],
                "format": "KAFKA"
            }, {
                "input": "validation_ko",
                "name": "raw-ko",
                "paths": [
                        "/data/output/discards/person/"
                ],
                "format": "JSON",
                "saveMode": "OVERWRITE"
            }
        ]
    }
    ]
}

def apply_validations(df: DataFrame, validations: dict) -> DataFrame:
    # Inicializamos la columna arraycoderrorbyfield como un array vacío
    df = df.withColumn("arraycoderrorbyfield", F.array())

    # Aplicamos cada validación y agregamos el código de error cuando falle
    for v in validations:
        field = v["field"]
        errors = []

        if "notEmpty" in v["validations"]:
            error_condition = F.when(F.col(field) == "", F.lit({"field": field, "error": "Error: campo vacío"}))
            errors.append(error_condition)

        if "notNull" in v["validations"]:
            error_condition = F.when(F.col(field).isNull(), F.lit({"field": field, "error": "Error: campo nulo"}))
            errors.append(error_condition)

        # Agregamos los errores a arraycoderrorbyfield si existen
        for error in errors:
            df = df.withColumn(
                "arraycoderrorbyfield",
                F.when(error.isNotNull(), F.expr("array_union(arraycoderrorbyfield, array(error))"))
                .otherwise(F.col("arraycoderrorbyfield"))
            )

    # Dividimos en registros válidos e inválidos según si arraycoderrorbyfield está vacío o no
    df_valid = df.filter(F.size(F.col("arraycoderrorbyfield")) == 0).drop("arraycoderrorbyfield")
    df_invalid = df.filter(F.size(F.col("arraycoderrorbyfield")) > 0)

    return df_valid, df_invalid


# def apply_validations(df: DataFrame, validations: dict) -> DataFrame:
#     for v in validations:
#         field = v["field"]
#         if "notEmpty" in v["validations"]:
#             df = df.filter(F.col(field) != "")
#         elif "notNull" in v["validations"]:
#             df = df.filter(F.col(field).isNotNull())
#     return df


def add_fields(df: DataFrame, field_name: str, function: str) -> DataFrame:
    if function == "current_timestamp":
        df = df.withColumn(field_name, F.current_timestamp())
        return df


def write_to_hdfs(df: DataFrame, file_path: str, file_format: str, file_save_mode: str) -> None:
    try:
        df.write \
            .format(file_format) \
            .mode(file_save_mode.lower()) \
            .save(file_path)
        logging.info(f"Data written to {file_path} in {file_format} format")
    except IOError as e:
        logging.error(f"Error writing to HDFS {file_path}: {str(e)}")


def write_to_kafka(df: DataFrame, topic: str) -> None:
    try:
        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_URL) \
            .option("topic", topic) \
            .save()
        logging.info(f"Data written to Kafka topic {topic}")
    except IOError as e:
        logging.error(f"Error writing to Kafka topic {topic}: {str(e)}")


def data_validation(df: DataFrame, transformations: List[dict], sinks: List[dict]) -> None:
    for trnsf in transformations:
        if trnsf["name"] == "validation":
            logging.info(f"Starting with {trnsf['name']} step...")
            validations = trnsf["params"]["validations"]
            df_valid, df_invalid = apply_validations(df, validations)
            # This could be replace by left_anti join by any PK, but
            # we don't have on this data sample.
            # df_invalid = df.subtract(df_valid)
        elif trnsf["name"] == "ok_with_date":
            for field in trnsf["params"]["addFields"]:
                df_valid = add_fields(df_valid, field["name"], field["function"])

    for sink in sinks:
        if sink["input"] == "ok_with_date":
            for topic in sink["topics"]:
                # write_to_kafka(df_valid, topic)
                print("Escribir en Kafka")
        elif sink["input"] == "validation_ko":
            for path in sink["paths"]:
                file_path = "".join([HDFS_URL, path, sink["name"]])
                write_to_hdfs(df_invalid, file_path, sink["format"], sink["saveMode"])

    return df_valid, df_invalid


def run(spark: SparkSession) -> None:
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
    # jar_packages_url = ','.join([
    #     'spark-sql-kafka-0-10_2.12-3.3.0.jar, kafka-clients-3.7.0.jar',
    #     'commons-pool2-2.12.0.jar',
    #     'spark-token-provider-kafka-0-10_2.12-3.3.0.jar',
    #     'spark-streaming-kafka-0-10-assembly_2.12-3.3.0.jar'
    # ])

    # jar_packages_local_path = ','.join([
    #     '/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar',
    #     '/opt/bitnami/spark/jars/kafka-clients-3.7.0.jar',
    #     '/opt/bitnami/spark/jars/commons-pool2-2.12.0.jar',
    #     '/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar',
    #     '/opt/bitnami/spark/jars/spark-streaming-kafka-0-10-assembly_2.12-3.3.0.jar'
    # ])

    # .config("spark.jars", "spark-sql-kafka-0-10_2.12-3.5.3.jar") \
    # hdfs://hadoop:9000/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar
    # org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.3
    # org.apache.spark:spark-streaming-kafka-0-10_2.13:3.3.0

    spark = SparkSession.builder \
        .appName("Input data") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.3") \
        .getOrCreate()

    # data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    # columns = ["name", "value"]

    # df = spark.createDataFrame(data, columns)

    # KAFKA_URL = "kafka:9092"
    # TOPIC = "person"

    # df.write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_URL) \
    #     .option("topic", TOPIC) \
    #     .save()

    # print(spark.conf.get("spark.jars"))

    run(spark)


if __name__ == "__main__":
    main()
