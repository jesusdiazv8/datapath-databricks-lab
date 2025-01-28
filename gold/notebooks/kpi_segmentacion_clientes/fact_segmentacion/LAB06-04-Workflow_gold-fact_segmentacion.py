# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sch_gold_tb.fact_segmentacion;
# MAGIC CREATE TABLE sch_gold_tb.fact_segmentacion AS
# MAGIC SELECT c.region,
# MAGIC c.idsegmento,
# MAGIC s.descsegmento,
# MAGIC COUNT(DISTINCT c.codcliente) AS cant_cliente
# MAGIC FROM sch_silver_tb.m_clientes c
# MAGIC INNER JOIN sch_silver_tb.m_segmentacion_clientes s ON(s.idsegmento=c.idsegmento)
# MAGIC GROUP BY c.region,
# MAGIC c.idsegmento,
# MAGIC s.descsegmento;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sch_gold_tb.fact_segmentacion LIMIT 5;