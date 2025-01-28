# Databricks notebook source
# MAGIC %md
# MAGIC #####1. Ingesta del dataset **`customers`** en la capa **`landing`**  del datalake

# COMMAND ----------


dbutils.fs.cp("/databricks-datasets/retail-org/customers/customers.csv", "/mnt/dlk/landing/app_retail/customers/customers.csv")