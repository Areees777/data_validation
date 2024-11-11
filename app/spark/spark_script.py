import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame, functions as F


HDFS_URL = "hdfs://hadoop:9000"

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
    
def save_non_valid_records(df: DataFrame, file_path: str, file_format: str, file_save_mode: str) -> None:
    try:
        df.write \
            .format(file_format) \
            .mode(file_save_mode.lower()) \
            .save(file_path)
        logging.info(f"Able to write the file into {file_path}")
    except IOError as e:
        logging.error(f"Not able to write the file due to {e}")


def data_validation(df: DataFrame, transformations: List[dict], sinks: List[dict]) -> None:
    for trnsf in transformations:
        if trnsf["name"] == "validation":
            logging.info(f"Starting with {trnsf['name']} step...")
            validations = trnsf["params"]["validations"]
            df_valid = apply_validations(df, validations)
            df_invalid = df.subtract(df_valid)
        elif trnsf["name"] == "ok_with_date":
            for field in trnsf["params"]["addFields"]:
                df_valid = add_fields(df_valid, field["name"], field["function"])
    
    for sink in sinks:
        if sink["input"] == "ok_with_date":
            print("Escribir en Kafka")
        elif sink["input"] == "validation_ko":
            file_name = sink["name"]
            file_format = sink["format"]
            file_save_mode = sink["saveMode"]
            for path in sink["paths"]:
                file_path = "".join([HDFS_URL, path, file_name])
                save_non_valid_records(df_valid, file_path, file_format, file_save_mode)
                

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
            df_valid, df_invalid = data_validation(df, transformations, sinks)    
    logging.info("Finished data validation")


def main():
    spark = SparkSession.builder \
        .appName("Input data") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .getOrCreate()    
    run(spark)    

if __name__ == "__main__":
    main()