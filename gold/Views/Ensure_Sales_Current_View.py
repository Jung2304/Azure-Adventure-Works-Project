# Databricks notebook source

import re

# Parameters
dbutils.widgets.text("catalog", "adb_adventureworks_project")
dbutils.widgets.text("silver_schema", "silver_dev")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")

# Validate identifier components
for name in (catalog, silver_schema):
  if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
    raise ValueError(f"Invalid SQL identifier: {name}")

source_table = f"{catalog}.{silver_schema}.sales_silver_table"
current_view = f"{catalog}.{silver_schema}.sales_current_view"

# Create or update current-state view
spark.sql(f"""
CREATE OR REPLACE VIEW {current_view}
AS

WITH ranked_sales AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY OrderNumber, OrderLineItem
            ORDER BY COALESCE(RecordVersion, 1) DESC
        ) AS rn
    FROM {source_table}
)

SELECT
    OrderDate,
    StockDate,
    OrderNumber,
    ProductKey,
    CustomerKey,
    TerritoryKey,
    OrderLineItem,
    OrderQuantity,
    COALESCE(RecordVersion, 1) AS RecordVersion
FROM ranked_sales
WHERE rn = 1
""")

print(f"PASS: Current-state view ensured: {current_view}")