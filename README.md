# 📈💱 Data Analytics Pipeline for Financial Services leveraging dbt & Snowflake

This project illustrates how to build and deploy a scalable data transformation and analytics pipeline leveraging dbt & Snowflake for Financial Services data to analyze stock exchange trading performance, including profit and loss calculations.

The dbt pipeline models/transforms Finance & Economics data product available in Snowflake Markeyplace, establishes data tests, generates documentation, orchestrates deployment to production, and continuously integrates changes in GitHub repository.

---

## 🚀 Overview

- **Setup Snowflake**: Create warehouse and database used by dbt to materialize SQL models. Get Finance & Economics data from Snowflake Marketplace.
- **Setup dbt**: Create and initialize a dbt project having Snowflake as a data warehouse and a GitHub repository to commit the project structure and create a new branch for development.
- **Develop**: Declare dbt sources. Create staging, intermediate, incremental facts models, seeds. Create data tests. Generate documentation. Merge development branch into the main branch used for deployment.
- **Deploy**: Run dbt on a schedule by scheduling a dbt job to orchestrate the execution of models in production environment. Verify deployment in Snowflake.

## 📒 Guide notebook

└── Data_Analytics_Pipeline_dbt_Snowflake.ipynb 

---

## ⚙️ Pipeline lineage

![Data lineage](/images/dbt_pipeline_lineage/dbt_pipeline_lineage.png)

---

## 📂 Project structure

![Folder structure](/images/dbt_project_docs/dbt_project_structure.png)



