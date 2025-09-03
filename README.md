# Proyecto de Ingeniería de Datos en Databricks

## 📍 Tecnología principal: Databricks + Delta Lake + Apache Spark + Azure

## Overview
In this project, I built an ETL job in Databricks using PySpark with a Medallion architecture (Bronze, Silver, Gold) to process sales data from .csv files stored in Azure Data Lake Storage Gen2. ✅

## Map
```
databricks-etl-project/
├── README.md
├── data/
│   ├── bronze/                # Datos crudos ingeridos desde Azure Data Lake
│   │   ├── clientes.csv
│   │   ├── productos.csv
│   │   ├── categorias.csv
│   │   └── ventas.csv
│   ├── silver/                # Datos limpios y transformados
│   │   ├── clientes_silver.delta
│   │   ├── productos_silver.delta
│   │   ├── categorias_silver.delta
│   │   └── ventas_silver.delta
│   └── gold/                  # Datos consolidados listos para analítica
│       └── modelo_ventas.delta
├── notebooks/                 # Notebooks en Databricks
│   ├── taller_autoloader.ipynb
└── img/                       # Scripts de apoyo en PySpark
    ├── etl_job.py
    ├── transformations.py
    └── utils.py
```

## Scripts Description
Taller Autoloader.py: Extracts raw data from csv in Azure Storage Gen2.

## Jobs & Pipelines
The jobs are configured with the following parameters: {"location":"clientes","table_name":"clientes_autoloader"} ✅
