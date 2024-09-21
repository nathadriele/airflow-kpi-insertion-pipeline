## Automated KPI Data Insertion for Data Warehouse Using Apache Airflow

### Overview
This project implements an Apache Airflow DAG to automate the insertion of KPI data into a Data Warehouse. The DAG gathers information such as transaction times and storage usage from various systems and inserts them into corresponding tables for further analysis and monitoring. This pipeline ensures that the data is updated periodically and is available for reporting.

### Problem Description
In data engineering, **Data Warehouses** store vast amounts of data originating from various systems. It is crucial to maintain accurate and up-to-date KPIs, such as transaction times and storage utilization. **Manual updates** of such metrics can lead to **errors** and inefficiencies, making it unsustainable as the data grows in volume and complexity. By leveraging Apache Airflow, this project ensures that these metrics are collected, transformed, and loaded automatically and consistently.

### Objective
The objective of this project is to:
- Automate the insertion of KPI data related to transaction times and storage usage into a Data Warehouse.
- Ensure regular, timely updates of these KPIs for monitoring and performance analysis.

### Prerequisites
- Python 3.7+: Ensure Python 3.7 or later is installed.
- Apache Airflow 2.7+: Install and configure Apache Airflow 2.7 or later. Refer to the [Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).
- PostgreSQL 12+: Set up a PostgreSQL 12 or later database with connection details configured in Airflow.
- Airflow Connection: Configure a PostgreSQL connection in Airflow with the ID data_warehouse_conn_id.

### Code Explanation

**DAG Configuration**
The DAG defined in this project is scheduled to run every hour (0 * * * *), ensuring regular updates of KPI data. The key tasks include:

- Inserting Transaction KPI Data: This task calculates and inserts the elapsed transaction time from multiple systems.
- Inserting Storage Usage KPI Data: This task monitors storage utilization and inserts usage percentages into the Data Warehouse.

**DAG Tasks**
- insert_kpi_transaction_data: This Python function extracts the most recent transaction times from various systems and calculates the elapsed time in minutes. The results are inserted into the data_warehouse.load_times table.
- insert_kpi_storage_usage: This function calculates the percentage of used storage compared to the available capacity. The data is inserted into the data_warehouse.storage_usage table for monitoring purposes.

### Contribution to Data Engineering
This project demonstrates key concepts in Data Engineering, including:
- Automated ETL (Extract, Transform, Load): A fully automated pipeline for collecting KPIs, transforming them, and loading them into a Data Warehouse.
- Airflow DAG Management: How to use Apache Airflow for orchestrating ETL jobs, which is an industry standard for data engineering workflows.
- Data Quality: Ensures that KPIs are inserted correctly and at regular intervals, supporting accurate and timely data for decision-making.
