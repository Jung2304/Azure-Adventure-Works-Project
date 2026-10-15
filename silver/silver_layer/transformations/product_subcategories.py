from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

product_subcategories_rules = {
    "subkey_not_null": "ProductSubcategoryKey IS NOT NULL",
    "name_not_null": "SubcategoryName IS NOT NULL",
    "product_key_not_null": "ProductCategoryKey IS NOT NULL"
}

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "product_subcategories_stg")
def product_subcategories_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", "true")
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Product_Subcategories/"))

# Silver: transform and validate
@dp.expect_all_or_drop(product_subcategories_rules)
@dp.table(name = "product_subcategories_silver_table")
def product_subcategories_silver_table():
    return (dp.read_stream("product_subcategories_stg"))
                
