# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparar o ambiente
# MAGIC Rode uma vez. Depois, envie os arquivos para `/Volumes/workspace/gs/chegada/<fonte>/` pela interface.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gs")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gs.chegada")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gs.controle_contagens (
  etapa STRING, tabela STRING, linhas BIGINT, arquivos BIGINT, medido_em TIMESTAMP
)
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gs.controle_qualidade (
  etapa STRING, tabela STRING, regra STRING, barradas BIGINT, medido_em TIMESTAMP
)
""")

for fonte in ["chuva", "previsao", "ibge", "sensor", "relatos", "setores", "cadastro"]:
    dbutils.fs.mkdirs(f"/Volumes/workspace/gs/chegada/{fonte}")

display(dbutils.fs.ls("/Volumes/workspace/gs/chegada"))
