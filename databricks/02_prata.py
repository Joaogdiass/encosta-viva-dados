# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata (requisito 06)
# MAGIC Trata tipos, nulos e inválidos, conta o que cada regra barrou e aplica MERGE pela chave.
# MAGIC
# MAGIC O widget `salt` gera o hash dos moradores. Não salve o valor no repositório.

# COMMAND ----------

# MAGIC %run ./funcoes

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", FUSO)

dbutils.widgets.text("etapa", "lote1")
dbutils.widgets.text("salt", "")
etapa = dbutils.widgets.get("etapa")
salt = dbutils.widgets.get("salt")

# COMMAND ----------


def merge_prata(df, tabela, chaves):
    nome = f"workspace.gs.{tabela}"
    if not spark.catalog.tableExists(nome):
        df.limit(0).write.saveAsTable(nome)
    df.createOrReplaceTempView("lote_prata")
    condicao = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    spark.sql(f"""
        MERGE INTO {nome} AS alvo
        USING lote_prata AS novo
        ON {condicao}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def registrar_qualidade(tabela, contagens):
    for regra, barradas in contagens:
        spark.sql(f"""
            INSERT INTO workspace.gs.controle_qualidade
            VALUES ('{etapa}', '{tabela}', '{regra}', {barradas}, current_timestamp())
        """)


def bronze(fonte):
    return spark.table(f"workspace.gs.bronze_{fonte}")


# COMMAND ----------

setores, q = prata_setores(bronze("setores"))
registrar_qualidade("prata_setor", q)
merge_prata(setores, "prata_setor", ["setor_id"])

passos = [
    ("prata_municipio", ["municipio_ibge"], lambda: prata_municipios(bronze("ibge"))),
    ("prata_chuva", ["setor_id", "data_hora"], lambda: prata_chuva(bronze("chuva"))),
    ("prata_previsao", ["setor_id", "data_hora"], lambda: prata_previsao(bronze("previsao"))),
    ("prata_sensor", ["setor_id", "sensor_id", "data_hora"], lambda: prata_sensor(bronze("sensor"))),
    ("prata_relato", ["relato_id"], lambda: prata_relatos(bronze("relatos"), spark.table("workspace.gs.prata_setor"))),
    ("prata_morador", ["morador_hash"], lambda: prata_moradores(bronze("cadastro"), salt)),
]
for tabela, chaves, transformar in passos:
    df, q = transformar()
    registrar_qualidade(tabela, q)
    merge_prata(df, tabela, chaves)

# COMMAND ----------

display(spark.sql(f"""
    SELECT tabela, regra, barradas
    FROM workspace.gs.controle_qualidade
    WHERE etapa = '{etapa}'
    ORDER BY tabela, regra
"""))
