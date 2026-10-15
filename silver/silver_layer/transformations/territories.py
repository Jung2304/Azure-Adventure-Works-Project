from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

territories_rules = {
    "key_not_null": "SalesTerritoryKey IS NOT NULL",
    "region_not_null": "Region IS NOT NULL",
    "country_not_null": "Country IS NOT NULL",
    "continent_not_null": "Continent IS NOT NULL"
}

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "territories_stg")
def territories_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", "true")
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Territories/"))

# Silver: transform and validate
@dp.expect_all_or_drop(territories_rules)
@dp.table(name = "territories_silver_table")
def territories_silver_table():
    return (dp.read_stream("territories_stg"))
                
