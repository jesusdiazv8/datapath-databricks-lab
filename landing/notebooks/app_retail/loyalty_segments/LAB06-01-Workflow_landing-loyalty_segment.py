# Databricks notebook source
# MAGIC %md
# MAGIC #####1. Ingesta del dataset **`loyalty_segment`** en la capa **`landing`**  del datalake

# COMMAND ----------


dbutils.fs.cp("/databricks-datasets/retail-org/loyalty_segments/loyalty_segment.csv", "/mnt/dlk/landing/app_retail/loyalty_segments/loyalty_segment.csv")