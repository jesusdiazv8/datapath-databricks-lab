# Databricks notebook source
df_segmento_cliente=spark.read.format("csv").option("header","true").load("dbfs:/databricks-datasets/retail-org/loyalty_segments/loyalty_segment.csv")

# COMMAND ----------

dir_output = "dbfs:/mnt/dlk/bronze/app_retail/loyalty_segments/data"

df_segmento_cliente\
 .write\
 .format("parquet")\
 .mode("overwrite")\
 .save(dir_output)

# COMMAND ----------

df_segmento_cliente.limit(5).display()