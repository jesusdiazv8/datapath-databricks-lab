# Databricks notebook source
df_m_segmentacion=spark.read.format("parquet").load("dbfs:/mnt/dlk/bronze/app_retail/loyalty_segments/data")
df_m_segmentacion.createOrReplaceTempView("tmp_m_segmentacion_clientes")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tmp_m_segmentacion_clientes_silver;
# MAGIC CREATE TABLE tmp_m_segmentacion_clientes_silver AS
# MAGIC SELECT TRY_CAST(s.loyalty_segment_id AS INTEGER) AS idsegmento,
# MAGIC UPPER(TRIM(s.loyalty_segment_description)) AS descsegmento
# MAGIC FROM tmp_m_segmentacion_clientes s;

# COMMAND ----------

df_m_segmentacion_silver=spark.sql("SELECT * FROM tmp_m_segmentacion_clientes_silver")

# COMMAND ----------

df_m_segmentacion_silver\
 .write\
 .format("delta")\
 .mode("overwrite")\
 .option("overwriteSchema", "true")\
 .saveAsTable("sch_silver_tb.m_segmentacion_clientes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sch_silver_tb.m_segmentacion_clientes LIMIT 5;

# COMMAND ----------

df_m_segmentacion_silver.limit(5).display()