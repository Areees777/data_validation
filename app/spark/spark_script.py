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
            "sinks": [{
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
                        "/data/output/discards/person"
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
            df_valid = df.filter(F.col(field) != "")
            df_invalid = df.filter(F.col(field) == "")
        elif "notNull" in v["validations"]:
            df_valid = df.filter(F.col(field).isNotNull())
            df_invalid = df.filter(F.col(field).isNull())        
    return df_valid, df_invalid

def add_fields(df: DataFrame, field_name: str, function: str) -> DataFrame:
    if function == "current_timestamp":
        df = df.withColumn(field_name, F.current_timestamp())
        return df

def data_validation(df: DataFrame, transformations: List[dict], sinks: List[dict]) -> None:
    for trnsf in transformations:
        if trnsf["name"] == "validation":
            logging.info(f"Starting with {trnsf['name']} step...")
            validations = trnsf["params"]["validations"]
            df_valid, df_invalid = apply_validations(df, validations)
        elif trnsf["name"] == "ok_with_date":
            for field in trnsf["params"]["addFields"]:
                add_fields(df_valid, field["name"], field["function"])
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
            df_valid.printSchema()
            df_invalid.printSchema()

            print(f"Count valid = {df_valid.count()}")
            print(f"Count invalid = {df_invalid.count()}")
    
    logging.info("Finished data validation")


def main():
    spark = SparkSession.builder \
        .appName("Input data") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .getOrCreate()    
    run(spark)    

if __name__ == "__main__":
    main()