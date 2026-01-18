import logging

from pyspark.sql.functions import col, count

from spark_jobs.config import (
    JSON_PATH,
    PARQUET_PATH,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_TABLE,
    POSTGRES_USER
)
from spark_jobs.jobs.read_sources import read_json, read_parquet, read_postgres
from spark_jobs.spark_session import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    spark = create_spark_session("PizzaBatchAnalytics")

    # Load data sources
    sales_df = read_parquet(spark, PARQUET_PATH)
    ingredients_df = read_json(spark, JSON_PATH)
    categories_df = read_postgres(
        spark,
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
        POSTGRES_USER,
        POSTGRES_PASSWORD,
        POSTGRES_TABLE
    )

    if not all([sales_df, ingredients_df, categories_df]):
        logger.error("One or more data sources failed to load")
        return

    # Business Question 1:
    # How many cali_ckn pizzas were ordered on 2015-01-04?
    cali_ckn_count = sales_df.filter(
        (col("pizza_name") == "cali_ckn") &
        (col("order_date") == "2015-01-04")
    ).count()

    logger.info(
        "Number of cali_ckn pizzas ordered on 2015-01-04: %d",
        cali_ckn_count
    )

    # Business Question 2:
    # What ingredients does the pizza ordered on 2015-01-02 at 18:27:50 have?
    order_df = sales_df.filter(
        (col("order_date") == "2015-01-02") &
        (col("order_time") == "18:27:50")
    )
    rows = order_df.select("pizza_name").limit(1).collect()

    if not rows:
        logger.warning("No orders found for 2015-01-02 18:27:50")
        spark.stop()
        return

    pizza_name = rows[0]["pizza_name"]

    ingredients = ingredients_df.filter(
        col("pizza_name") == pizza_name
    ).select("ingredients").collect()[0]["ingredients"]

    logger.info(
        "Ingredients for pizza ordered on 2015-01-02 at 18:27:50: %s",
        ingredients
    )

    # Business Question 3:
    # Most sold pizza category between 2015-01-01 and 2015-01-08
    filtered_sales = sales_df.filter(
        (col("order_date") >= "2015-01-01") &
        (col("order_date") <= "2015-01-08")
    )

    joined_df = filtered_sales.join(
        categories_df,
        on="pizza_name",
        how="left"
    )

    top_category_df = (
        joined_df
        .groupBy("category")
        .agg(count("*").alias("total_orders"))
        .orderBy(col("total_orders").desc())
    )

    logger.info("Most sold pizza category in the given period:")
    top_category_df.show(1, truncate=False)

    # Save results to Parquet
    top_category_df.write.mode("overwrite").parquet(
        "data/analytics/top_category.parquet"
    )

    logger.info("Analytics results saved to Parquet")

    spark.stop()
    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
