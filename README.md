# 🍺 Open Brewery ETL Pipeline

Este projeto implementa um pipeline de dados completo utilizando a arquitetura de medalhão (Bronze → Silver → Gold) com Airflow, PySpark e Streamlit, extraindo dados da API pública [Open Brewery DB](https://api.openbrewerydb.org).

This project deploys a complete data pipeline using a medalion architecture (Bronze → Silver → Gold) with Airflow, PySpark and Streamlit, to extract data from an Open API [Open Brewery DB](https://api.openbrewerydb.org).

## 📚 Visão Geral

## 📚 Overview

O objetivo do projeto é demonstrar domínio técnico em:
The goal of this project is to demonstrate technical skills: 

- Engenharia de dados com Airflow e PySpark
- Orchestrating and applying data engineering with Airflow and PySpark

- Arquitetura de dados com camadas bronze, silver e gold
- Build a data architecture using the bronze, silver, and gold layers

- Monitoração e tratamento de falhas no pipeline
- Monitoring and handling failures on pipeline

- Visualização interativa com Streamlit
- Viewing interactive using Streamlit 


---
## Arquitetura
## 🧱 Architeture

```text
API Open Brewery
      ↓
[Bronze Layer] - Raw (Parquet)
      ↓
[Silver Layer] - Dados limpos, normalizados, particionados por país
[Silver Layer] - Cleaned data, normalized, partitioned by país
      ↓
[Gold Layer] - Dados agregados (por estado e tipo de cervejaria)
[Gold Layer] - Aggregated data (by country and brewery type)
      ↓
[Streamlit] - Interface interativa para análise
[Streamlit] - Interactive interface for analytics
```

🛠️ Tecnologias Utilizadas
## 🛠️ The tools used are as follows:
- **Apache Airflow 2.8.1**: Used for orchestrating the pipeline.
- **Python 3.8**: Used for data requests and collections.
- **PySpark 3.5.1**: Used for data processing.
- **Docker**: Used for containerization.
- **Streamlit** (Gold Layer): Build a simple dashboard.

## 🧠 Solution Design
Bronze: Dados brutos salvos da API em parquet
Bronze: Raw data is stored in parquet

Silver: Dados tratados (nulls, tipos, renomeação), salvos particionados por country
Silver: Processed data (nulls, types and renaming), are stored by country (is or are ??????)

Gold: Agregações por type, state, country
Gold: Aggregated by type, state, country

### 🔍 Monitoring and Handling erros strategy
validation: It's applied one validation to each layer during the process, getting counts registries and check if there's empty files. 
validation: This processs checks for empty files and obtains count records for each layer
    validate_bronze_data: Validate the raw parquet is stored correctly
    validate_silver_data: Validate critical nulls and schemas problems
    validate_gold_data: Validate the final file stored

email_alert(): Whether/IF any problems occur during this process, a callback error will be returned (Here, I simulated this using logs)


### ❓ Decisions and Design Choices
- **Parquet files**: I chose parquet file because it's ideal for analytical queries, selective column reading, optimized compression and indexing
- **Airflow**: I chose Airflow as a pipeline orchestrator because of its solidity. visibility and flexibility. It allows us modeling pipelines with clear dependencies between steps, configure retries and warnings, and monitor executions via a web interface. It integrates perfectly with pyspark. 
- **PySpark**: PySpark was chosen as a processing engine because it allows us to apply transformations, distributed aggregations, and execute parallel processes. Furthermore, PySpark enables us to scale the same local code to Databricks, EMR, or Kubernetes clusters.

## 🚀 Running Instructions

### Requirements
- Docker
- Docker Compose
- Python 3.8+
- PySpark 3.5+
- Apache Airflow 2.8

### 1. Clone the Repository
```bash
git clone 
cd 
```

### 2. Create a .env file
The .env file should contain the following variables:
```
# Caminhos do Datalake
BRONZE_DIR=/opt/airflow/datalake/bronze
SILVER_DIR=/opt/airflow/datalake/silver
GOLD_DIR=/opt/airflow/datalake/gold

```
### 3. Build and Start Docker Services
Use the following command to build the Docker containers and start the services.
```bash
docker-compose up --build
```
### 4. Access the Airflow Web Interface
If services are up, access the Airflow interface by navigating to:
```
http://localhost:8080
```
Log in with the default credentials (airflow/airflow).

### 5. Trigger the DAG
In the Airflow interface, locate the DAG named `brewery_etl_dag`. You can then trigger the pipeline to fetch data from API, transform it, and create the gold view.