from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField

HDFS_URL = "hdfs://hadoop:9000"

PROGRAM_METADATA = {
    "dataflows": [{
            "name": "prueba-acceso",
            "sources": [{
                    "name": "person_inputs",
                    "path": "/data/input/events/person/",
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

def run(spark: SparkSession) -> None:
    for flow in PROGRAM_METADATA["dataflows"]:
        flow_name = flow["name"]
        for source in flow["sources"]:
            file_path = source["path"]
            file_format = source["format"].lower()
            full_file_path = "".join([HDFS_URL, file_path])
            df = spark.read \
                .format("json") \
                .load(full_file_path)
            print(f"Count = {df.count()}")

def main():
    
    spark = SparkSession.builder \
        .appName("Input data") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .getOrCreate()

    run(spark)  
    
    

if __name__ == "__main__":
    main()