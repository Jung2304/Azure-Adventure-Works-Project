# Adventure Works | End-to-End Azure Data Engineering

[![Azure](https://img.shields.io/badge/Azure-ADLS_Gen2-0078D4?logo=microsoftazure&logoColor=white)](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-Structured_Streaming-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/streaming/index.html)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID_Transactions-007DBA?logo=delta&logoColor=white)](https://delta.io/)
[![Lakeflow](https://img.shields.io/badge/Lakeflow-Declarative_Pipelines-16A34A?logo=databricks&logoColor=white)](https://learn.microsoft.com/en-us/azure/databricks/ldp/)
[![Databricks Asset Bundles](https://img.shields.io/badge/DABs-Deployment_as_Code-7353BA?logo=databricks&logoColor=white)](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
[![GitHub](https://img.shields.io/badge/GitHub-Version_Control-181717?logo=github&logoColor=white)](https://github.com/Jung2304/Azure-Adventure-Works-Project)

An end-to-end **Azure Data Engineering project** that processes Adventure Works sales data from Azure Data Lake Storage Gen2 into analytics-ready Delta tables in Azure Databricks. The solution uses **incremental ingestion, declarative data transformations, dimensional modeling, Delta Lake MERGE, automated quality checks, and Databricks Asset Bundles** for deployment.

## Architecture
<p align="center">
  <img width="1692" height="953" alt="image" src="https://github.com/user-attachments/assets/392d97e8-2829-4910-bd47-cf96cab270b2" />
</p>

**Orchestration:** A five-task Databricks Job runs `refresh_silver` → `build_dims_gold` → `build_fact_gold` → `refresh_monthly_sales` → `quality_check`.  
<p align="center">
  <img width="2268" height="1153" alt="Screenshot 2026-10-15 225237" src="https://github.com/user-attachments/assets/453b975a-1b40-4d0c-b40d-56ac9700ed0f" />
</p>
The pipeline and Job are deployed separately for development, using `silver_dev` and `gold_dev` to avoid overwriting the existing `silver` and `gold` tables.  

**Deployment:** Databricks Asset Bundles (DABs) defines the pipeline and Job as code. A development target writes to separate `silver_dev` and `gold_dev` schemas.

## Technology stack

| Component | Technology | Purpose |
| --- | --- | --- |
| Data lake | Azure Data Lake Storage Gen2 | Stores source CSV files |
| Processing | Azure Databricks, PySpark, Spark SQL | Ingestion and transformation |
| Incremental ingestion | Databricks Auto Loader (`cloudFiles`) | Discovers newly arrived CSV files |
| Managed pipelines | Lakeflow Spark Declarative Pipelines (`pyspark.pipelines`) | Silver datasets and expectations |
| Storage format | Delta Lake | Reliable tables and upserts |
| Analytics model | Star schema | Reusable fact and dimension tables |
| Orchestration | Databricks Jobs | Dependency-driven execution |
| Deployment | Databricks Asset Bundles, Databricks CLI | Versioned development deployment |
| Version control | Git and GitHub | Source control |

## Data source and modeling

**Dataset:** [Adventure Works (Kaggle)](https://www.kaggle.com/datasets/ukveteran/adventure-works). The project uses sales, calendar, customers, products, product categories, product subcategories, territories, and returns files.

- **Silver:** Eight cleaned entity tables, including `sales_silver_table`, with data-quality expectations.
- **Gold fact:** `fact_sales` at **one row per `(OrderNumber, OrderLineItem)`**, containing order quantity and *estimated* revenue, cost, and gross profit.
- **Gold dimensions:** `dim_product`, `dim_customer`, `dim_territory`, and `dim_date`.
- **Gold aggregate:** `agg_monthly_sales`, one row per month, containing orders, units sold, estimated revenue, estimated cost, estimated gross profit, average order value, and gross margin.

> **Financial metric caveat:** The sales source does not include transaction-level sale amounts. Revenue is estimated as `OrderQuantity × ProductPrice`, and cost as `OrderQuantity × ProductCost`. These use product-level values and may not reflect actual historical prices, discounts, or realized profitability.

## Star Schema
<p align="center">
  <img width="1267" height="952" alt="Screenshot 2026-10-15 231249" src="https://github.com/user-attachments/assets/f938f05b-171d-43ea-a7c8-a57510b47390" />
</p>

## Key engineering features

### Incremental ingestion and data quality

- Auto Loader ingests yearly CSV folders from ADLS Gen2 into the Silver pipeline.
- Lakeflow expectations validate required fields and positive quantities.
- Mixed historical `M/d/yyyy` and simulated `yyyy-MM-dd` dates are parsed explicitly to avoid dropping valid records due to schema inference.
- A new `Sales_2018` CSV was successfully discovered without rewriting historical source files.

### Incremental Gold processing

- A Delta Lake `MERGE` matches sales lines using `(OrderNumber, OrderLineItem)`.
- New lines are inserted; changed lines can be updated; unchanged lines are left untouched.
- A repeat run with unchanged input affected **zero rows**, demonstrating idempotent upsert behavior.
- Dimensions and the small monthly aggregate are rebuilt from their sources as part of the workflow.

> The current MERGE reads the full Silver source and incrementally updates the Gold target. Source-side incremental filtering or change-data-feed processing is a potential future optimization.

### Automated checks

The final Job task raises errors if important invariants fail:

1. Sales order-line keys are unique.
2. Estimated revenue reconciles from Silver to Gold fact.
3. Estimated revenue reconciles from Gold fact to monthly aggregate.


## Repository structure

```text
Azure-E2E-Adventure-Works-DE-Project/
│
├─ AdventureWorksData/
├─ analytics/
│  └─ agg_monthly_sales.ipynb
├─ experiments/
│  └─ query_testing.ipynb
├─ gold/
│  ├─ Dimension_Builder/
│  │  └─ Dimension_Tables.ipynb
│  ├─ Fact_Tables/
│  │  └─ Fact_Builder.ipynb
│  └─ Quality_Check/
│     └─ Gold_Data_Quality_Checks.ipynb
├─ resources/
│  ├─ adventure_works_job.yml
│  └─ silver_pipeline.yml
├─ silver/
│  └─ silver_layer/
│     └─ transformations/
│        ├─ calendar.py
│        ├─ customers.py
│        ├─ product_categories.py
│        ├─ product_subcategories.py
│        ├─ products.py
│        ├─ returns.py
│        ├─ sales.py
│        └─ territories.py
├─ Testing_Simulate/
│  └─ extend_dim_date.ipynb
├─ .gitignore
├─ databricks.yml
└─ README.md
```

## Deployment and execution

**Prerequisites:** Access to an Azure Databricks workspace and configured ADLS Gen2 source files, a Unity Catalog catalog with appropriate permissions, and an authenticated Databricks CLI profile. Update project-specific workspace, catalog, storage, and schema references for your own environment. The initial development `gold_dev.fact_sales` target must exist with the expected schema before the first `MERGE`.

```bash
# Authenticate to your Databricks workspace (once)
databricks auth login --host https://<your-workspace-host>

# Inspect and deploy the development bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle summary -t dev

# Execute the end-to-end workflow
databricks bundle run adventure_works_job -t dev
```

## Analytics produced

- Monthly estimated revenue trend and order-volume analysis.
- Average order value trend.
- Product category contribution and changes in product mix.

The analytical findings are illustrative of the provided dataset and product-price assumptions, not audited sales results.

## Lessons learned

- Validate table grain and join cardinality before building facts and dimensions.
- Handle mixed CSV date formats explicitly and monitor expectation drop counts.
- Treat Auto Loader discovery, Silver validation, Delta `MERGE`, and aggregate refresh as separate correctness concerns.
- Isolate development schemas and parameterize notebooks for safe bundle deployment.

## 👤 Author & Acknowledgment

* **GitHub:** [@Jung2304](https://github.com/Jung2304)
* **Project Repository:** [Azure-Adventure-Works-Project](https://github.com/Jung2304/Azure-Adventure-Works-Project.git)
