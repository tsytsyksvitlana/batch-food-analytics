import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: str = "BatchPizzaProject_ReadSources"
) -> SparkSession:
    """
    Create and configure SparkSession.

    :param app_name: Spark application name
    :return: SparkSession instance
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession created successfully")

    return spark
