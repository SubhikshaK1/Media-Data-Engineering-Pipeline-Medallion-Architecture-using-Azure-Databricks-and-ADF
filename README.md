# Media-Data-Engineering-Pipeline-Medallion-Architecture-using-Azure-Databricks-and-ADF
Designed and developed a scalable Medallion Architecture (Lakehouse) for end-to-end data ingestion, integration, and transformation using Azure Data Factory (ADF) and Azure Databricks, processing raw Netflix data into business ready assets

![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/apache%20spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

## 📑 Table of Contents
1. [Project Overview](#1-project-overview)
2. [Project Materials](#2-project-materials)
3. [Project Plan](#3-project-plan)
4. [Analyzing Requirements](#4-analyzing-requirements)
5. [Design the Data Architecture](#5-design-the-data-architecture)
6. [Choose the Right Approach](#6-choose-the-right-approach)
7. [Design the Layers of Data Warehouse (DWH)](#7-design-the-layers-of-data-warehouse-dwh)
8. [Architecture Diagram](#8-architecture-diagram)
9. [Project Initialization](#9-project-initialization)
10. [Tech Stack](#10-tech-stack)
11. [Data Pipeline Workflow & ETL/ELT](#11-data-pipeline-workflow--etlelt)
12. [Code Implementation](#12-code-implementation)
13. [Challenges & Solutions](#13-challenges--solutions)
14. [Testing & Validation](#14-testing--validation)
15. [How to Run the Project](#15-how-to-run-the-project)

---

## 1. Project Overview
**Problem Statement:** Organizations often struggle to manage incremental data ingestion, handle evolving schemas, enforce strict data quality rules, and dynamically schedule parallel transformations at scale. 
**Objective:** To build a fully automated, dynamic, and parameter-driven ETL/ELT pipeline utilizing the Medallion architecture (Bronze, Silver, Gold). The pipeline ingests distributed data sources, strictly governs data quality via declarative frameworks, and serves optimized data to end-users for reporting.
**Real-World Use Case:** Processing a comprehensive Netflix dataset (including master data for titles, alongside lookup tables for directors, cast, countries, and categories). This pipeline dynamically integrates historical bulk data and continuous incremental feeds into a unified Data Lakehouse, delivering real-time analytics to Business Intelligence tools.

---

## 2. Project Materials
* **Cloud Platform:** Microsoft Azure (Free Tier valid for 30 days, using `$200` credits).
* **Source Code / Repositories:** GitHub repository containing the core raw files.
* **Datasets:** Netflix Movies and TV Shows dataset (Kaggle public dataset formatted as CSV). Consists of a master `netflix_titles.csv` file and smaller lookup files (`directors.csv`, `cast.csv`, `categories.csv`, `countries.csv`).
* **BI Tool:** Power BI Desktop (for downstream analytics).

---

## 3. Project Plan
The project execution is structured into five core phases:
1. **Phase 1: Infrastructure & Security Setup:** Provision Azure Data Lake Storage Gen2 (ADLS Gen2), Azure Data Factory (ADF), Azure Databricks. Establish security bridges via Azure Access Connectors and configure Databricks Unity Catalog.
2. **Phase 2: Ingestion (The Raw & Bronze Layers):** Develop dynamic HTTP REST API connections in ADF to pull GitHub data in parallel. Implement Databricks AutoLoader for streaming incremental data.
3. **Phase 3: Transformation (The Silver Layer):** Develop dynamic, parameterized PySpark notebooks to handle data cleansing, type casting, window functions, and text splitting. Orchestrate via Databricks Workflows (incorporating `If/Else` evaluation).
4. **Phase 4: Declarative ETL & Data Quality (The Gold Layer):** Build a Delta Live Tables (DLT) pipeline to generate streaming materialized views, enforcing strict data quality expectations (Drop/Fail/Warn on bad records).
5. **Phase 5: Serving & BI Integration:** Provision a Serverless SQL Warehouse and securely connect the Gold Layer directly to Power BI using Databricks Partner Connect.

---

## 4. Analyzing Requirements
**Functional Requirements:**
* System must asynchronously fetch mapping data (lookup tables) via HTTP REST APIs.
* System must identify and incrementally load newly arriving data files exactly once.
* Pipelines must be dynamically parameterized (e.g., using a single ADF Copy Activity for multiple tables via a `ForEach` loop).
* Data must be strictly governed and quality-checked before entering the presentation layer.

**Non-Functional Requirements:**
* **Scalability:** Must handle fluctuating batch sizes via Spark’s distributed compute.
* **Idempotency:** Pipelines must be rerun-safe without creating duplicate records (leveraging Checkpoint locations and Delta format).
* **Security:** Role-based access control must be managed centrally via Databricks Unity Catalog rather than fragmented storage keys.

**Assumptions:**
* Raw data files arrive in `.csv` format. 
* Business analysts require SQL-friendly interfaces to consume data natively.

---

## 5. Design the Data Architecture
**Source → Processing → Storage → Consumption**

1. **Source:** Netflix data resides in a GitHub repository, separated into Master Data (Titles) and Lookup Tables (Cast, Directors, etc.).
2. **Ingestion & Processing:** 
    * **ADF** validates source file availability and uses Web/Copy activities to pull historical mapping data in parallel.
    * **Databricks AutoLoader** (`cloudFiles`) acts as a streaming ingestion engine monitoring the ADLS directory for new incoming master files. 
3. **Storage (Medallion via ADLS Gen2 & Unity Catalog):**
    * **Raw/Bronze:** Ingested CSV data is immediately converted into Delta Lake format to support ACID transactions.
    * **Silver:** Data undergoes schema enforcement, deduplication, handling of `NULL` records, and flag creation using PySpark.
    * **Gold:** Highly aggregated, business-ready views managed by Delta Live Tables (DLT).
4. **Consumption:** Databricks SQL Warehouse acts as the high-performance compute engine serving Gold layer queries directly to Power BI.

---

## 6. Choose the Right Approach
* **Why AutoLoader over ADF Watermarking?** Traditional ADF watermark tables are static and require manual management of high-watermark DB tables. Databricks AutoLoader uses directory listing and native `RocksDB` checkpoints to manage exactly-once incremental ingestion with zero administrative overhead. 
* **Why Delta Live Tables (DLT) over Standard Pipelines?** DLT provides a declarative ETL framework. Instead of writing boilerplate code to handle checkpoints, dependencies, and state, we simply define *what* the target table should look like. Furthermore, DLT natively integrates Data Quality Checks (`@dlt.expect_all_or_drop`), which standard notebooks lack.
* **Why Unity Catalog over Legacy Hive Metastore?** Unity Catalog unifies governance, securely tying ADLS Gen2 external locations via Azure Access Connectors, mitigating the need for hardcoded SAS tokens or Account Keys inside notebooks.
* *(Alternative mentioned but not implemented)*: Azure Synapse Analytics could be used as a final Data Warehouse, but Databricks SQL Warehouses (Lakehouse) successfully eliminate the need to duplicate data into a separate DW.

---

## 7. Design the Layers of Data Warehouse (DWH)
* **Raw Layer:** Ephemeral landing zone for CSV files arriving directly from external sources.
* **Bronze Layer (Staging):** An exact replica of raw data but stored in Delta Lake format. Schema is inferenced dynamically; unmapped columns are sent to a `_rescued_data` column (Schema Evolution).
* **Silver Layer (Cleansing):** Data is cleaned and transformed. `NULL` values are handled (e.g., Duration replaced with zeros), column types are cast (Strings to Integers), and text delimiters are parsed. Stored as Delta tables.
* **Gold Layer (Serving):** Dimensional tables and facts are strictly typed and quality-verified. Designed loosely around Star Schema principles for analytical consumption.

---

## 8. Architecture Diagram

```text
[Sources]                         [Orchestration & Ingestion]                [Storage & Transformation - ADLS Gen2 / Delta Lake]           [Consumption]
   │                                           │                                              │                                                  │
   ├──> GitHub (CSV Files) ────────> Azure Data Factory (Validation + ForEach) ──────> [Raw / Bronze Layer]                                      │
   │                                           │                                              │ ──> PySpark Transformations ──┐                  │
   └──> Local / External APIS ─────> Databricks AutoLoader (Streaming) ──────────────> [Bronze Layer]                         │                  │
                                                                                                                              V                  │
                                                                                                                       [Silver Layer]            │
                                                                                                                              │ ──> DLT ──┐      │
                                                                                                                              V           V      │
                                                                                                                       [Gold Layer] (Unity Cat) ─> Power BI
```
**Step-by-Step Flow:**
1. ADF checks for file availability via a Validation Activity, fetches API metadata, and triggers a dynamic `ForEach` loop to copy HTTP CSV files to the Bronze container.
2. AutoLoader constantly monitors a directory, ingesting new Master Data CSVs into Bronze Delta tables.
3. Databricks Workflows trigger PySpark notebooks passing parameters via `dbutils.widgets` to perform Silver transformations. 
4. Delta Live Tables read from Silver, enforce constraints, and generate the final Gold tables.
5. Power BI connects via `.pbids` file to the Databricks SQL Warehouse.

---

## 9. Project Initialization
**Environment Setup & Folder Structure:**
1. **Resource Group:** Create `RG_Netflix_Project` (Canada Central).
2. **ADLS Gen2:** Provision a Storage Account with *Hierarchical Namespace Enabled*. Create containers: `raw`, `bronze`, `silver`, `gold`, `metastore`.
3. **Data Factory:** Provision `ADF-Netflix`. Configure an HTTP Linked Service (pointing to GitHub base URL) and an ADLS Gen2 Linked Service.
4. **Databricks & Unity Catalog:** 
   * Deploy Premium/Trial Databricks Workspace. 
   * Create an **Azure Access Connector**, assign it `Storage Blob Data Contributor` on the ADLS Gen2 account.
   * Attach the Access Connector inside the Databricks Account Console to instantiate the Unity Catalog Metastore. 
   * Create external locations pointing precisely to the ADLS Gen2 container paths.

---

## 10. Tech Stack
* **Cloud Provider:** Microsoft Azure
* **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Orchestration:** Azure Data Factory (ADF), Databricks Workflows
* **Compute & Processing:** Azure Databricks (PySpark, Spark SQL, Spark Streaming)
* **Data Governance & Formats:** Unity Catalog, Delta Lake, Delta Live Tables (DLT), RocksDB (Checkpointing)
* **Reporting & Consumption:** Databricks SQL Warehouse, Power BI

---

## 11. Data Pipeline Workflow & ETL/ELT
### The ADF Dynamic Ingestion Pipeline
* **Validation Step:** Pipeline waits until `netflix_titles.csv` physically exists in the container before proceeding.
* **Get Metadata:** Web activity polls the GitHub API for relative file configurations.
* **ForEach / Array Parameter:** An array of JSON parameters (e.g., `[{"folderName":"directors", "fileName":"directors"}]`) is passed into a `ForEach` loop.
* **Parameterized Copy Activity:** Inside the loop, the relative URL dynamically updates `@item().fileName` to copy 4 different API endpoints concurrently.

### Databricks Workflow Orchestration
* **Task Values:** Uses `dbutils.jobs.taskValues.set()` to pass a JSON array of directories from a lookup notebook to processing notebooks.
* **If/Else Execution:** Workflows use current date (`ISO time`) to dynamically evaluate `If/Else` paths. E.g., The heavy Master Data calculation is scheduled only when `weekday == 7` (Sunday). 

---

## 12. Code Implementation
Refer `Media Data Folder`
- Incremental Ingestion using AutoLoader (Bronze)
- PySpark Transformations & Window Functions (Silver)
- Declarative ETL with Delta Live Tables (Gold)


---

## 13. Challenges & Solutions
* **Challenge:** Traditional static ADF pipelines require duplicated Copy Activities for every single new data source.
  * **Solution:** Developed an Array-based parameter loop (`ForEach`) combined with Web API dynamic content, allowing *N* files to be ingested using a single pipeline block.
* **Challenge:** Maintaining state and deduplication in continuous streaming environments.
  * **Solution:** Implemented Databricks AutoLoader utilizing underlying `RocksDB` folder states to manage exact-once idempotency.
* **Challenge:** Pipeline breaking due to incoming CSV schema drift.
  * **Solution:** AutoLoader implicitly handles Schema Evolution. New columns are dynamically appended to the Delta table schema without immediately failing the batch stream.

---

## 14. Testing & Validation
* **ADF Validation:** The pipeline execution is gate-kept by an ADF Validation activity evaluating path persistence. It natively halts execution until source data signals complete transmission.
* **Data Quality (DLT Expectations):** Using `@dlt.expect_all_or_drop`. If `show_id` evaluates to NULL, the record is flagged in the Event Log metrics (under "dropped records") but does not crash the overall pipeline load. Allows for CI/CD tracking of data degradation. 

---

## 15. How to Run the Project
1. Clone this repository to your local machine.
2. Ensure you have an Active Azure Subscription and deploy the resources tracking the required naming conventions (Data Factory, ADLS Gen2, Databricks).
3. Import the `/ADF/` ARM templates to your Azure Data Factory instance to instantiate the connections, arrays, and parameter pipelines.
4. Import the provided notebooks (`.ipynb` files) located in `/Databricks/` into your Databricks workspace.
5. Create an **Azure Access Connector**, assign Role-Based Access Controls to ADLS Gen2, and configure the Unity Catalog via the Databricks Account Console.
6. Trigger the ADF Pipeline `validation_github` to ingest API data into the `Bronze` storage.
7. Run the configured **Delta Live Tables Pipeline** in Databricks (Ensure you are utilizing a Job Cluster, not an Interactive Cluster, to respect standard quota cores).
8. Use Databricks Partner Connect to download the `.pbids` file and open it in Power BI to view the Gold reporting models.

---
*If you find this project helpful, drop a ⭐ on the repository!*
```
