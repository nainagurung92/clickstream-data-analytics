from pyspark.sql import SparkSession
import application_configs as config


class ApplicationUtilities:

    def create_spark_session(self) -> SparkSession:
        spark = SparkSession.builder.master(config.MASTER_NAME)\
            .appName(config.APPLICATION_NAME)\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2')\
            .getOrCreate()
        return spark


