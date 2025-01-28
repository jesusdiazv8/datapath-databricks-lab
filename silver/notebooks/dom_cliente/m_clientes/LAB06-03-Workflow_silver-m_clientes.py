# Databricks notebook source
df_m_customers=spark.read.format("parquet").load("dbfs:/mnt/dlk/bronze/app_retail/customers/data")
df_m_customers.createOrReplaceTempView("tmp_m_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tmp_m_customer_silver;
# MAGIC CREATE TABLE tmp_m_customer_silver AS
# MAGIC SELECT c.customer_id AS codcliente,
# MAGIC c.customer_name AS nomcliente,
# MAGIC UPPER(TRIM(c.city)) AS ciudad,
# MAGIC UPPER(TRIM(c.region)) AS region,
# MAGIC UPPER(TRIM(c.district)) AS distrito,
# MAGIC TRY_CAST(c.lon AS DECIMAL(10,4)) AS longitud,
# MAGIC TRY_CAST(c.lat AS DECIMAL(10,4)) AS latitud,
# MAGIC TRY_CAST(c.units_purchased AS FLOAT) AS cantidad,
# MAGIC TRY_CAST(c.loyalty_segment AS INTEGER) AS idsegmento
# MAGIC FROM tmp_m_customer c

# COMMAND ----------

df_m_customers_silver=spark.sql("SELECT * FROM tmp_m_customer_silver")

# COMMAND ----------

df_m_customers_silver\
 .write\
 .format("delta")\
 .mode("overwrite")\
 .option("overwriteSchema", "true")\
 .saveAsTable("sch_silver_tb.m_clientes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sch_silver_tb.m_clientes LIMIT 5;

# COMMAND ----------

df_m_customers_silver.limit(5).display()