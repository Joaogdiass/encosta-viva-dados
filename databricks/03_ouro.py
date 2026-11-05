# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro (requisitos 07 e 08)
# MAGIC Modelo dimensional e arquivo do MVP. A Ouro é recalculada inteira a partir da Prata.
# MAGIC
# MAGIC O nível de risco é uma sugestão explicada por fatores: quem decide remover pessoas é a Defesa Civil.

# COMMAND ----------

# MAGIC %run ./funcoes

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", FUSO)


def prata(tabela):
    return spark.table(f"workspace.gs.prata_{tabela}")


def salvar(df, tabela, sem_fuso=False):
    nome = f"workspace.gs.{tabela}"
    if sem_fuso:
        # O Delta exige habilitar TIMESTAMP_NTZ, que mantém a hora local em qualquer sessão.
        spark.sql(f"CREATE TABLE IF NOT EXISTS {nome} (_inicial INT)")
        spark.sql(f"ALTER TABLE {nome} SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome)


# COMMAND ----------

fato = construir_fato(prata("chuva"), prata("previsao"), prata("sensor"), prata("relato"))
salvar(fato, "fato_risco_setor")
fato = spark.table("workspace.gs.fato_risco_setor")

salvar(construir_dim_setor(prata("setor"), prata("municipio")), "dim_setor")
salvar(construir_dim_tempo(fato), "dim_tempo", sem_fuso=True)
salvar(construir_dim_fonte(spark), "dim_fonte")
salvar(construir_dim_sensor(prata("sensor")), "dim_sensor", sem_fuso=True)
salvar(construir_dim_morador(prata("morador")), "dim_morador_cadastro")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência de LGPD para o relatório

# COMMAND ----------

display(spark.table("workspace.gs.dim_morador_cadastro").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arquivo do MVP
# MAGIC Baixe `risco_setores.json` do Volume e substitua `mvp/dados/risco_setores.json` no repositório.

# COMMAND ----------

dados = exportar_mvp(fato, spark.table("workspace.gs.dim_setor"), spark.table("workspace.gs.dim_tempo"))
dados["amostra"] = "Sensores, relatos e eventos de chuva extrema são simulados."
destino = "/Volumes/workspace/gs/chegada/_mvp/risco_setores.json"
dbutils.fs.mkdirs("/Volumes/workspace/gs/chegada/_mvp")
with open(destino, "w", encoding="utf-8") as arquivo:
    json.dump(dados, arquivo, ensure_ascii=False, indent=1)
print(destino)
