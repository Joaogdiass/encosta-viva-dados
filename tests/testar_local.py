"""Teste local do pipeline, sem Airflow e sem Databricks.

Uso: py tests/testar_local.py [--usar-chegada] [--atualizar-mvp]

Coleta dados reais e simulados de 2026-10-25 a 2026-11-05 em uma pasta temporária
(ou reaproveita a pasta chegada/ com --usar-chegada), carrega no Spark local e roda
Prata, Ouro e as consultas SQL.
Não cobre Auto Loader nem MERGE, que precisam do Databricks.
"""
import json
import os
import shutil
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(RAIZ / "dags"), str(RAIZ / "databricks")]

import funcoes as fn  # noqa: E402
from encosta import coleta  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

INICIO, FIM = date(2026, 10, 25), date(2026, 11, 5)


def coletar(chegada: Path) -> None:
    dia = INICIO
    while dia <= FIM:
        ds = dia.isoformat()
        coleta.coletar_chuva(ds, chegada)
        coleta.coletar_previsao(ds, chegada)
        coleta.coletar_ibge(ds, chegada)
        coleta.exportar_sensor(ds, chegada)
        coleta.exportar_relatos(ds, chegada)
        coleta.exportar_referencias(ds, chegada)
        print(ds, coleta.conferir_chegada(ds, chegada))
        dia += timedelta(days=1)


def main() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    temporario = Path(tempfile.mkdtemp(prefix="encosta_"))
    if "--usar-chegada" in sys.argv:
        chegada = RAIZ / "chegada"  # reaproveita os arquivos gerados pelo Airflow
    else:
        chegada = temporario / "chegada"
        coletar(chegada)

    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", fn.FUSO)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    def bronze(fonte):
        # Monta o dataset direto na JVM: no Windows, o Spark local não lista pastas
        # sem o winutils e os workers Python podem falhar. O Databricks não tem esse limite.
        linhas = spark._jvm.java.util.ArrayList()
        for arquivo in sorted((chegada / fonte).glob("*.json")):
            for linha in arquivo.read_text(encoding="utf-8").splitlines():
                linhas.add(linha)
        dataset = spark._jsparkSession.createDataset(linhas, spark._jvm.org.apache.spark.sql.Encoders.STRING())
        return DataFrame(spark._jsparkSession.read().json(dataset), spark).withColumn(
            "carregado_em", F.current_timestamp()
        )

    setores, q_setor = fn.prata_setores(bronze("setores"))
    municipios, q_mun = fn.prata_municipios(bronze("ibge"))
    chuva, q_chuva = fn.prata_chuva(bronze("chuva"))
    previsao, q_prev = fn.prata_previsao(bronze("previsao"))
    sensor, q_sensor = fn.prata_sensor(bronze("sensor"))
    relatos, q_rel = fn.prata_relatos(bronze("relatos"), setores)
    moradores, _ = fn.prata_moradores(bronze("cadastro"), "salt-somente-de-teste")

    for tabela, q in (("setor", q_setor), ("municipio", q_mun), ("chuva", q_chuva),
                      ("previsao", q_prev), ("sensor", q_sensor), ("relato", q_rel)):
        print(f"Qualidade prata_{tabela}: {q}")

    fato = fn.construir_fato(chuva, previsao, sensor, relatos).cache()
    dim_setor = fn.construir_dim_setor(setores, municipios)
    dim_tempo = fn.construir_dim_tempo(fato)
    dim_sensor = fn.construir_dim_sensor(sensor)
    dim_morador = fn.construir_dim_morador(moradores)

    for nome, df in (("fato_risco_setor", fato), ("dim_setor", dim_setor), ("dim_tempo", dim_tempo),
                     ("dim_fonte", fn.construir_dim_fonte(spark)), ("dim_sensor", dim_sensor),
                     ("dim_morador_cadastro", dim_morador)):
        df.createOrReplaceTempView(nome)
        print(f"{nome}: {df.count()} linhas")

    print("Distribuição de níveis:")
    fato.groupBy("nivel_ordem", "nivel_risco").count().orderBy("nivel_ordem").show(truncate=False)

    # Verificações do que a entrega exige.
    colunas_morador = set(dim_morador.columns)
    assert not colunas_morador & {"nome", "telefone", "morador_id"}, "Dado pessoal na Ouro"
    duplicadas = fato.groupBy("setor_id", "tempo_id").count().filter("count > 1").count()
    assert duplicadas == 0, f"{duplicadas} chaves duplicadas na fato"
    assert fato.filter("nivel_ordem >= 3").count() > 0, "Nenhum alerta gerado"
    assert all(n > 0 for r, n in q_sensor if r != "chave_nula"), f"Regra sem barrar: {q_sensor}"

    sql = (RAIZ / "databricks" / "04_consultas.sql").read_text(encoding="utf-8").replace("workspace.gs.", "")
    consultas = [c for c in sql.split(";") if "SELECT" in c]
    assert len(consultas) == 5, f"Esperadas 5 consultas, achei {len(consultas)}"
    for numero, consulta in enumerate(consultas, 1):
        resultado = spark.sql(consulta)
        print(f"Consulta {numero}: {resultado.count()} linhas")
        resultado.show(10, truncate=False)
        assert resultado.count() > 0, f"Consulta {numero} sem resultado"

    if "--atualizar-mvp" in sys.argv:
        dados = fn.exportar_mvp(fato, dim_setor, dim_tempo)
        dados["amostra"] = "Amostra de teste local. Sensores, relatos e eventos de chuva extrema são simulados."
        destino = RAIZ / "mvp" / "dados" / "risco_setores.json"
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(json.dumps(dados, ensure_ascii=False, indent=1), encoding="utf-8")
        print("MVP atualizado:", destino)

        amostras = RAIZ / "amostras"
        for fonte in coleta.FONTES:
            origem = chegada / fonte / f"{fonte}_{FIM.isoformat()}.json"
            alvo = amostras / f"{fonte}_amostra.json"
            alvo.parent.mkdir(exist_ok=True)
            with origem.open(encoding="utf-8") as linhas:
                alvo.write_text("".join(list(linhas)[:3]), encoding="utf-8")

    spark.stop()
    shutil.rmtree(temporario, ignore_errors=True)
    print("TESTE LOCAL OK")


if __name__ == "__main__":
    main()
