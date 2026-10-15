from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

calendar_rules = {
    "date_not_null": "Date IS NOT NULL"
}

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "calendar_stg")
def calendar_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", "true")
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Calendar/"))

# Silver: transform and validate
@dp.expect_all_or_drop(calendar_rules)
@dp.table(name = "calendar_silver_table")
def calendar_silver_table():
    df = dp.read_stream("calendar_stg")
    return (df
            .withColumn("Month", month(col("Date")))
            .withColumn("Year", year(col("Date"))))

                
