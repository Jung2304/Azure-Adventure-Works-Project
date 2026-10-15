from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

returns_rules = {
    "date_not_null": "ReturnDate IS NOT NULL",
    "ter_key_not_null": "TerritoryKey IS NOT NULL",
    "product_key_not_null": "ProductKey IS NOT NULL",
    "quantity_not_zero": "ReturnQuantity > 0"
}

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "returns_stg")
def returns_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", "true")
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Returns/"))

# Silver: transform and validate
@dp.expect_all_or_drop(returns_rules)
@dp.table(name = "returns_silver_table")
def returns_silver_table():
    return (dp.read_stream("returns_stg"))
                
