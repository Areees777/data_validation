import argparse

from dependencies.spark_wrapper import SparkWrapper
from configs.settings import SPARK_CONF, SPARK_JAR_PACKAGES
from configs.metadata import METADATA


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

    return 0

