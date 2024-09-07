## Automated KPI Data Insertion for Data Warehouse Using Apache Airflow

### Overview
This project implements an Apache Airflow DAG to automate the insertion of KPI data into a Data Warehouse. The DAG gathers information such as transaction times and storage usage from various systems and inserts them into corresponding tables for further analysis and monitoring. This pipeline ensures that the data is updated periodically and is available for reporting.

### Problem Description
Data warehouses store large volumes of data from multiple systems, and it is critical to maintain updated metrics such as transaction times and storage usage. Manually updating these metrics can be error-prone and inefficient. Automating the process using Apache Airflow helps ensure that data is regularly collected, transformed, and loaded into the warehouse in a consistent and scalable manner.
