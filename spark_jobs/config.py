import os

CSV_PATH = "../data/raw/pizza_sales.csv"
JSON_PATH = "../data/raw/ingredients.json"
PARQUET_PATH = "../data/processed/pizza_sales.parquet"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "pizza_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "pizza_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "pizza_pass")
POSTGRES_TABLE = os.environ.get("POSTGRES_TABLE", "pizza_categories")
