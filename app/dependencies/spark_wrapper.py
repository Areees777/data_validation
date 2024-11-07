from dataclasses import dataclass

from pyspark import SparkConf
from pyspark.sql import SparkSession


@dataclass(frozen=True)
class SparkWrapper:
    """Class to retrieve an Spark Session."""
    _app_name: str

    def get_spark_session(self, spark_conf: dict, packages: list = None):
        """Function that returns a Spark Session based on the provided spark
        configuration dictionary.
        Install provided packages in the configuration if they are provided."""

        if spark_conf is None:
            raise ValueError("Config file not provided in spark object")

        conf = SparkConf()
        for c in spark_conf:
            conf.set(c, spark_conf[c])

        spark = SparkSession.builder \
            .appName(self._app_name) \
            .config(conf=conf) \
            .enableHiveSupport() \
            .getOrCreate()

        if packages is not None:
            for package in packages:
                spark.sparkContext.addPyFile(package)

        return spark