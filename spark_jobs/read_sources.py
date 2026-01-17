import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

CSV_PATH = "data/raw/pizza_sales.csv"
JSON_PATH = "data/raw/ingredients.json"
PARQUET_PATH = "data/processed/pizza_sales.parquet"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "pizza_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "pizza_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "pizza_pass")
POSTGRES_TABLE = os.environ.get("POSTGRES_TABLE", "pizza_categories")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
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


def read_csv(spark: SparkSession, path: str) -> DataFrame | None:
    """
    Read CSV file with an explicit schema and basic validation.

    :param spark: SparkSession
    :param path: Path to CSV file
    :return: DataFrame or None if failed
    """
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_time", StringType(), True),
        StructField("pizza_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
    ])

    try:
        df = spark.read.option("header", True).schema(schema).csv(path)
        logger.info("CSV loaded successfully (%d rows)", df.count())

        missing_cols = [
            field.name for field in schema.fields
            if field.name not in df.columns
        ]
        if missing_cols:
            logger.warning("Missing columns in CSV: %s", missing_cols)

        if df.rdd.isEmpty():
            logger.warning("CSV file is empty")

        return df

    except AnalysisException as e:
        logger.error("Error reading CSV: %s", e)
        return None


def read_json(spark: SparkSession, path: str) -> DataFrame | None:
    """
    Read JSON file with nested structures.

    :param spark: SparkSession
    :param path: Path to JSON file
    :return: DataFrame or None if failed
    """
    try:
        df = spark.read.option("multiline", True).json(path)
        logger.info("JSON loaded successfully (%d rows)", df.count())

        if df.rdd.isEmpty():
            logger.warning("JSON file is empty")

        return df

    except AnalysisException as e:
        logger.error("Error reading JSON: %s", e)
        return None


def read_parquet(spark: SparkSession, path: str) -> DataFrame | None:
    """
    Read Parquet file containing historical data.

    :param spark: SparkSession
    :param path: Path to Parquet file
    :return: DataFrame or None if file does not exist
    """
    try:
        df = spark.read.parquet(path)
        logger.info("Parquet loaded successfully (%d rows)", df.count())

        if df.rdd.isEmpty():
            logger.warning("Parquet file is empty")

        return df

    except AnalysisException:
        logger.warning("No Parquet data found at %s", path)
        return None


def read_postgres(
    spark: SparkSession,
    host: str,
    port: str,
    db: str,
    user: str,
    password: str,
    table: str
) -> DataFrame | None:
    """
    Read a small PostgreSQL table using JDBC (single-threaded).

    For tiny tables like 'pizza_categories', do not use partitioning,
    because partitioning requires a numeric column, lowerBound, and upperBound.

    :param spark: SparkSession
    :param host: PostgreSQL host
    :param port: PostgreSQL port
    :param db: Database name
    :param user: Username
    :param password: Password
    :param table: Table name
    :return: DataFrame or None if failed
    """
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    try:
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
        logger.info(
            "PostgreSQL table '%s' loaded successfully (%d rows)",
            table,
            df.count()
        )
        if df.rdd.isEmpty():
            logger.warning("PostgreSQL table '%s' is empty", table)
        return df
    except Exception as e:
        logger.error(
            "Error reading PostgreSQL table '%s': %s",
            table,
            e
        )
        return None



def main() -> None:
    """
    Main entry point for reading all data sources.
    """
    spark = create_spark_session()

    pizza_sales_df = read_csv(spark, CSV_PATH)
    ingredients_df = read_json(spark, JSON_PATH)
    historical_df = read_parquet(spark, PARQUET_PATH)
    pizza_categories_df = read_postgres(
        spark,
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
        POSTGRES_USER,
        POSTGRES_PASSWORD,
        POSTGRES_TABLE
    )

    if pizza_sales_df is not None:
        logger.info("Sample pizza_sales data")
        pizza_sales_df.show(5, truncate=False)
        processed_path = "data/processed/pizza_sales.parquet"
        pizza_sales_df.write.mode("overwrite").parquet(processed_path)
        logger.info("CSV data written to Parquet at %s", processed_path)

    if ingredients_df is not None:
        logger.info("Sample ingredients data")
        ingredients_df.show(5, truncate=False)

    if historical_df is not None:
        logger.info("Sample historical Parquet data")
        historical_df.show(5, truncate=False)

    if pizza_categories_df is not None:
        logger.info("Sample pizza_categories data")
        pizza_categories_df.show(5, truncate=False)

    spark.stop()
    logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
