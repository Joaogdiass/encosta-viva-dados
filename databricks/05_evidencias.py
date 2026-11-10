# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Evidências para o relatório
# MAGIC Mostra contagens, qualidade, tabelas, LGPD e as 5 consultas em uma única execução.

# COMMAND ----------

import os

caminho = os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), "04_consultas.sql")
with open("/Workspace" + caminho, encoding="utf-8") as arquivo:
    consultas = [c.strip() for c in arquivo.read().split(";") if "SELECT" in c]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagens da Bronze

# COMMAND ----------

display(spark.sql("""
    SELECT etapa, replace(tabela, 'workspace.gs.', '') AS tabela, linhas, arquivos, medido_em
    FROM workspace.gs.controle_contagens
    ORDER BY tabela, medido_em
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linhas por tabela e por etapa

# COMMAND ----------

display(spark.sql("""
    SELECT replace(tabela, 'workspace.gs.bronze_', '') AS tabela,
           MAX(CASE WHEN etapa = 'lote1' THEN linhas END) AS lote1,
           MAX(CASE WHEN etapa = 'lote2' THEN linhas END) AS lote2,
           MAX(CASE WHEN etapa = 'reexecucao' THEN linhas END) AS reexec2,
           MAX(CASE WHEN etapa = 'lote3' THEN linhas END) AS lote3,
           MAX(CASE WHEN etapa = 'reexecucao_lote3' THEN linhas END) AS reexec3
    FROM workspace.gs.controle_contagens
    GROUP BY tabela
    ORDER BY tabela
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras de qualidade da Prata

# COMMAND ----------

ultima = spark.sql("SELECT max_by(etapa, medido_em) AS etapa FROM workspace.gs.controle_qualidade").first()["etapa"]
display(spark.sql(f"""
    SELECT etapa, tabela, regra, barradas
    FROM workspace.gs.controle_qualidade
    WHERE etapa = '{ultima}'
    ORDER BY barradas DESC, tabela, regra
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas da camada Ouro

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.gs").filter("tableName LIKE 'fato%' OR tableName LIKE 'dim%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## LGPD: moradores protegidos por hash

# COMMAND ----------

display(spark.table("workspace.gs.dim_morador_cadastro").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 1

# COMMAND ----------

display(spark.sql(consultas[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 2

# COMMAND ----------

display(spark.sql(consultas[1]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 3

# COMMAND ----------

display(spark.sql(consultas[2]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 4

# COMMAND ----------

display(spark.sql(consultas[3]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta 5

# COMMAND ----------

display(spark.sql(consultas[4]).drop("nome", "fonte_pico"))
