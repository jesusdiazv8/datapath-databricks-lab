# Databricks notebook source
df_customers=spark.read.format("csv").option("header","true").load("dbfs:/databricks-datasets/retail-org/customers/customers.csv")

# COMMAND ----------

dir_output = "dbfs:/mnt/dlk/bronze/app_retail/customers/data"

df_customers\
 .write\
 .format("delta")\
 .mode("overwrite")\
 .save(dir_output)

# COMMAND ----------

df_customers.limit(5).display()
