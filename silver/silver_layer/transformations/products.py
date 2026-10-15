from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

products_rules = {
    "key_not_null": "ProductKey IS NOT NULL",
    "subkey_not_null": "ProductSubcategoryKey IS NOT NULL",
    "name_not_null": "ProductName IS NOT NULL",
    "model_not_null": "ModelName IS NOT NULL",
    "product_cost_not_zero": "ProductCost > 0",
    "product_price_not_zero": "ProductPrice > 0"
}

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "products_stg")
def products_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", "true")
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Products/"))

# Silver: transform and validate
@dp.expect_all_or_drop(products_rules)
@dp.table(name = "products_silver_table")
def products_silver_table():
    df = dp.read_stream("products_stg")
    return (df
            .withColumn("ProductName", trim(split(col("ProductName"), ",")[0])))
                
