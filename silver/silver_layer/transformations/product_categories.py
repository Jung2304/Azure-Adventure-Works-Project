from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

product_categories_rules = {
    "key_not_null": "ProductCategoryKey IS NOT NULL",
    "name_not_null": "CategoryName IS NOT NULL"
}

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "product_categories_stg")
def product_categories_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", "true")
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Product_Categories/"))

# Silver: transform and validate
@dp.expect_all_or_drop(product_categories_rules)
@dp.table(name = "product_categories_silver_table")
def product_categories_silver_table():
    return (dp.read_stream("product_categories_stg"))
                
