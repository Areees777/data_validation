SPARK_CONF = {
    "conf": {
        "spark.driver.memory": "2g",
        "spark.executor.memory": "4g"
    }
}

SPARK_JAR_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.3"
]

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
