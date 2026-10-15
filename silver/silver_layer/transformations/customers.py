from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

customers_rules = {
    "key_not_null": "CustomerKey IS NOT NULL",
    "email_not_null": "EmailAddress IS NOT NULL",
    "birth_not_null": "BirthDate IS NOT NULL",
    "first_name_not_null": "FirstName IS NOT NULL",
    "last_name_not_null": "LastName IS NOT NULL"
}

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "customers_stg")
def customers_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", "true")
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Customers/"))

# Silver: transform and validate
@dp.expect_all_or_drop(customers_rules)
@dp.table(name = "customers_silver_table")
def customers_silver_table():
    df = dp.read_stream("customers_stg")
    return (df
            .withColumn("FullName", concat_ws(' ', col("Prefix"), col("FirstName"), col("LastName")))       # Concatenate With Separator
            .withColumn("Age", floor(months_between(current_date(), col("BirthDate")) / 12)))
                
