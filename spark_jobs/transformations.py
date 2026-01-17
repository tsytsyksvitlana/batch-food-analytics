import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofweek

from spark_jobs.config import (
    PARQUET_PATH,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_TABLE,
    POSTGRES_USER
)
from spark_jobs.read_sources import (
    create_spark_session,
    read_parquet,
    read_postgres
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def enrich_sales_data(
    sales_df: DataFrame,
    categories_df: DataFrame,
    ingredients_df: DataFrame
) -> DataFrame:
    """
    Enrich sales data by joining with categories and ingredients and computing total_price.
    """
    # 2. Partition input data for parallel processing
    # Use repartition to demonstrate distribution (e.g., by 4 partitions)
    df = sales_df.repartition(4)

    # 3. Filter data based on business conditions (e.g., only positive quantities)
    df = df.filter(col("quantity") > 0)

    # Join sales with categories
    df = df.join(categories_df, on="pizza_name", how="left")

    # Join with ingredients
    df = df.join(ingredients_df, on="pizza_name", how="left")

    # Add a total_price column
    df = df.withColumn("total_price", col("quantity") * col("price"))

    # Add a day_of_week column (1=Sunday, 7=Saturday)
    df = df.withColumn("day_of_week", dayofweek(col("order_date")))

    logger.info("Enrichment completed")
    return df


def aggregate_sales(df: DataFrame) -> DataFrame:
    """
    Example aggregations:
    - Total sales per category
    - Total sales per pizza
    """
    # Total revenue per category
    category_sales = df.groupBy("category").sum("total_price").withColumnRenamed("sum(total_price)", "total_revenue")

    # Total sales per pizza
    pizza_sales = df.groupBy("pizza_name").sum("total_price").withColumnRenamed("sum(total_price)", "total_revenue")

    logger.info("Aggregation completed")
    return category_sales, pizza_sales


def main():
    spark = create_spark_session("BatchPizzaProject_Transformations")

    sales_df = read_parquet(spark, PARQUET_PATH)
    categories_df = read_postgres(spark, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE)

    from read_sources import JSON_PATH, read_json
    ingredients_df = read_json(spark, JSON_PATH)

    if sales_df and categories_df and ingredients_df:
        enriched_df = enrich_sales_data(sales_df, categories_df, ingredients_df)
        enriched_df.show(5, truncate=False)

        category_sales, pizza_sales_agg = aggregate_sales(enriched_df)
        category_sales.show()
        pizza_sales_agg.show()

        enriched_df.write.mode("overwrite").parquet("data/enriched/enriched_sales.parquet")
        category_sales.write.mode("overwrite").parquet("data/enriched/category_sales.parquet")
        pizza_sales_agg.write.mode("overwrite").parquet("data/enriched/pizza_sales.parquet")
        logger.info("Enriched and aggregated data written to Parquet")

    spark.stop()
    logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
