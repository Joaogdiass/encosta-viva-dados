# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze (requisito 05)
# MAGIC Auto Loader lê só os arquivos novos. O checkpoint garante que rodar de novo não duplica nada.
# MAGIC
# MAGIC Rode com o widget `etapa` igual a `lote1`, depois `lote2` e, por fim, `reexecucao`. Tire print da contagem final.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("etapa", "lote1")
etapa = dbutils.widgets.get("etapa")

BASE = "/Volumes/workspace/gs/chegada"
FONTES = ["chuva", "previsao", "ibge", "sensor", "relatos", "setores", "cadastro"]

# COMMAND ----------


def carregar_bronze(fonte):
    consulta = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(f"workspace.gs.bronze_{fonte}")
    )
    consulta.awaitTermination()


for fonte in FONTES:
    carregar_bronze(fonte)

# COMMAND ----------

for fonte in FONTES:
    tabela = f"workspace.gs.bronze_{fonte}"
    spark.sql(f"""
        INSERT INTO workspace.gs.controle_contagens
        SELECT '{etapa}', '{tabela}', COUNT(*), COUNT(DISTINCT arquivo_origem), current_timestamp()
        FROM {tabela}
    """)

display(spark.sql("""
    SELECT etapa, tabela, linhas, arquivos, medido_em
    FROM workspace.gs.controle_contagens
    ORDER BY tabela, medido_em
"""))
