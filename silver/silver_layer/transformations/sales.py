from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

sales_rules = {
    "order_date_not_null": "OrderDate IS NOT NULL",
    "stock_date_not_null": "StockDate IS NOT NULL",
    "orde_num_not_null": "OrderNumber IS NOT NULL",
    "product_key_not_null": "ProductKey IS NOT NULL",
    "customer_key_not_null": "CustomerKey IS NOT NULL",
    "ter_key_not_null": "TerritoryKey IS NOT NULL",
    "quantity_not_zero": "OrderQuantity > 0"
}

# Explicit schema to avoid incorrect date inference
sales_schema = StructType([
    StructField("OrderDate", StringType()),
    StructField("StockDate", StringType()),
    StructField("OrderNumber", StringType()),
    StructField("ProductKey", IntegerType()),
    StructField("CustomerKey", IntegerType()),
    StructField("TerritoryKey", IntegerType()),
    StructField("OrderLineItem", IntegerType()),
    StructField("OrderQuantity", IntegerType()),
    StructField("RecordVersion", IntegerType(), True)       # True: Allows NULL records
])

# Bronze: Read files, using Auto Loader by default in pipelines
@dp.view(name = "sales_stg")
def sales_stg():
    return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .schema(sales_schema)
                .load("abfss://bronze@adventureworkadls2.dfs.core.windows.net/Sales_*/"))

# Silver: Parse dates and validate
@dp.expect_all_or_drop(sales_rules)
@dp.table(name="sales_silver_table")
def sales_silver_table():
    df = dp.read_stream("sales_stg")

    return (df
            .withColumn("RecordVersion", coalesce(
              col("RecordVersion"), lit(1)
            )
        )
            .withColumn("OrderDate", coalesce(
              try_to_timestamp(col("OrderDate"), lit("M/d/yyyy")),
              try_to_timestamp(col("OrderDate"), lit("yyyy-MM-dd"))
            )
        )
        .withColumn("StockDate", coalesce(
              try_to_timestamp(col("StockDate"), lit("M/d/yyyy")),
              try_to_timestamp(col("StockDate"), lit("yyyy-MM-dd"))
            )
        )
    )
