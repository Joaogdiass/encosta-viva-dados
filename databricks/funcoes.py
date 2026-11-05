# Databricks notebook source
# Funções compartilhadas pelos notebooks. Também são importadas pelo teste local.
import hashlib
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

FUSO = "America/Sao_Paulo"

# Limites físicos das regras de qualidade.
CHUVA_MAX_MM_H = 150.0

# Limiares ILUSTRATIVOS do motor de risco: calibre com PPDC, CEMADEN e IPT e cite a fonte.
LIMIAR_CHUVA_72H = 80.0
LIMIAR_UMIDADE_ATENCAO = 80.0
LIMIAR_UMIDADE_SATURADO = 90.0
LIMIAR_PREVISAO_6H = 30.0
LIMIAR_CHUVA_1H_FORTE = 15.0
LIMIAR_CHUVA_1H_MAXIMO = 20.0
LIMIAR_DELTA_INCLINACAO = 2.0

NIVEIS = {1: "Observação", 2: "Atenção", 3: "Alerta", 4: "Alerta máximo"}


def aplicar_regras(df, regras):
    """Remove as linhas de cada regra, em ordem, e devolve o total barrado por regra."""
    contagens = []
    for nome, condicao in regras:
        barradas = F.coalesce(condicao, F.lit(False))
        contagens.append((nome, df.filter(barradas).count()))
        df = df.filter(~barradas)
    return df, contagens


def manter_mais_recente(df, chaves, ordem):
    janela = Window.partitionBy(*chaves).orderBy(F.col(ordem).desc())
    return df.withColumn("_ordem", F.row_number().over(janela)).filter("_ordem = 1").drop("_ordem")


def _explodir_horario(bronze):
    """O Open-Meteo devolve listas paralelas: arrays_zip + explode gera uma linha por hora."""
    return (
        bronze
        .select(
            "setor_id", "coletado_em", "latitude_grade", "longitude_grade", "elevacao_grade",
            F.explode(F.arrays_zip(
                F.col("hourly.time").alias("time"),
                F.col("hourly.precipitation").alias("precipitation"),
                F.col("hourly.soil_moisture_0_to_1cm").alias("soil_moisture"),
            )).alias("h"),
        )
        .select(
            "setor_id",
            F.to_timestamp("h.time").alias("data_hora"),
            F.col("h.precipitation").cast("double").alias("precipitacao_mm"),
            F.col("h.soil_moisture").cast("double").alias("umidade_modelo_m3m3"),
            F.col("latitude_grade").cast("double").alias("latitude_grade"),
            F.col("longitude_grade").cast("double").alias("longitude_grade"),
            F.col("elevacao_grade").cast("double").alias("elevacao_grade"),
            F.to_timestamp("coletado_em").alias("coletado_em"),
        )
    )


def _regras_open_meteo(df):
    return aplicar_regras(df, [
        ("chave_nula", F.col("setor_id").isNull() | F.col("data_hora").isNull()),
        ("chuva_fora_do_limite", (F.col("precipitacao_mm") < 0) | (F.col("precipitacao_mm") > CHUVA_MAX_MM_H)),
        # O Open-Meteo usa m³/m³ (0 a 1), e não percentual como o sensor.
        ("umidade_modelo_fora_de_0_a_1", (F.col("umidade_modelo_m3m3") < 0) | (F.col("umidade_modelo_m3m3") > 1)),
    ])


def prata_chuva(bronze):
    df, contagens = _regras_open_meteo(_explodir_horario(bronze))
    return manter_mais_recente(df.fillna({"precipitacao_mm": 0.0}), ["setor_id", "data_hora"], "coletado_em"), contagens


def prata_previsao(bronze):
    df, contagens = _regras_open_meteo(_explodir_horario(bronze))
    df = df.select("setor_id", "data_hora", "precipitacao_mm", "coletado_em").fillna({"precipitacao_mm": 0.0})
    return manter_mais_recente(df, ["setor_id", "data_hora"], "coletado_em"), contagens


def prata_sensor(bronze):
    df = bronze.select(
        "setor_id", "sensor_id",
        F.to_timestamp("ts").alias("data_hora"),
        F.col("umidade_solo_pct").cast("double").alias("umidade_solo_pct"),
        F.col("inclinacao_graus").cast("double").alias("inclinacao_graus"),
        F.col("chuva_mm").cast("double").alias("chuva_mm"),
        F.col("bateria_pct").cast("double").alias("bateria_pct"),
        F.col("carregado_em"),
    )
    df, contagens = aplicar_regras(df, [
        ("chave_nula", F.col("setor_id").isNull() | F.col("sensor_id").isNull() | F.col("data_hora").isNull()),
        ("umidade_fora_de_0_a_100", (F.col("umidade_solo_pct") < 0) | (F.col("umidade_solo_pct") > 100)),
        ("chuva_fora_do_limite", (F.col("chuva_mm") < 0) | (F.col("chuva_mm") > CHUVA_MAX_MM_H)),
        ("horario_no_futuro", F.col("data_hora") > F.current_timestamp()),
    ])
    chaves = ["setor_id", "sensor_id", "data_hora"]
    total = df.count()
    df = manter_mais_recente(df, chaves, "carregado_em")
    contagens.append(("leitura_duplicada", total - df.count()))
    return df, contagens


def prata_relatos(bronze, setores):
    df = bronze.select(
        "relato_id", "setor_id",
        F.to_timestamp("ts").alias("data_hora"),
        "tipo_sinal",
        F.col("confianca_visao").cast("double").alias("confianca_visao"),
        F.col("confirmado_visao").cast("boolean").alias("confirmado_visao"),
        "carregado_em",
    )
    conhecidos = setores.select(F.col("setor_id").alias("_setor_ok")).distinct()
    df = df.join(conhecidos, df.setor_id == conhecidos._setor_ok, "left")
    df, contagens = aplicar_regras(df, [
        ("chave_nula", F.col("relato_id").isNull() | F.col("data_hora").isNull()),
        ("setor_desconhecido", F.col("_setor_ok").isNull()),
    ])
    return manter_mais_recente(df.drop("_setor_ok"), ["relato_id"], "carregado_em"), contagens


def prata_setores(bronze):
    df = bronze.select(
        "setor_id", "nome", "bairro", "municipio_ibge", "grau_risco",
        F.col("populacao_estimada").cast("int").alias("populacao_estimada"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        "carregado_em",
    )
    df, contagens = aplicar_regras(df, [
        ("chave_nula", F.col("setor_id").isNull()),
        ("coordenada_invalida", ~F.col("lat").between(-34, 6) | ~F.col("lon").between(-74, -34)),
    ])
    return manter_mais_recente(df, ["setor_id"], "carregado_em"), contagens


def prata_municipios(bronze):
    df = bronze.select(
        F.col("municipio_ibge").cast("string").alias("municipio_ibge"),
        F.col("nome_ibge").alias("municipio_nome"),
        F.col("populacao_2022").cast("long").alias("populacao_municipio_2022"),
        F.to_timestamp("coletado_em").alias("coletado_em"),
    )
    df, contagens = aplicar_regras(df, [
        ("chave_nula", F.col("municipio_ibge").isNull()),
        ("populacao_invalida", F.col("populacao_municipio_2022").isNull() | (F.col("populacao_municipio_2022") <= 0)),
    ])
    return manter_mais_recente(df, ["municipio_ibge"], "coletado_em"), contagens


def prata_moradores(bronze, salt):
    """LGPD: nome e telefone não passam da Bronze. Só o hash com salt segue adiante."""
    if not salt:
        raise ValueError("Informe o salt no widget. Ele não pode ficar no repositório.")
    df = bronze.select(
        F.sha2(F.concat(F.lit(salt), F.col("morador_id")), 256).alias("morador_hash"),
        "setor_id", "canal_preferido", "carregado_em",
    )
    return manter_mais_recente(df, ["morador_hash"], "carregado_em"), []


def construir_fato(chuva, previsao, sensor, relatos):
    """Fato por setor e hora, com o nível de risco e os fatores que pesaram."""
    sensor_h = (
        sensor.withColumn("hora", F.date_trunc("hour", "data_hora"))
        .groupBy("setor_id", "hora")
        .agg(
            F.sum("chuva_mm").alias("chuva_sensor"),
            F.avg("umidade_solo_pct").alias("umidade_solo_media"),
            F.max("inclinacao_graus").alias("inclinacao_max"),
        )
    )
    chuva_h = chuva.select(
        "setor_id", F.col("data_hora").alias("hora"),
        F.col("precipitacao_mm").alias("chuva_api"), "umidade_modelo_m3m3",
    )
    relatos_h = (
        relatos.withColumn("hora", F.date_trunc("hour", "data_hora"))
        .groupBy("setor_id", "hora")
        .agg(
            F.count("*").alias("qtd_relatos"),
            F.sum(F.col("confirmado_visao").cast("int")).alias("qtd_relatos_confirmados"),
        )
    )

    segundos = F.col("hora").cast("long")
    janela = Window.partitionBy("setor_id").orderBy(segundos)
    prev_janela = Window.partitionBy("setor_id").orderBy(F.col("hora").cast("long")).rangeBetween(3600, 6 * 3600)
    previsao_h = (
        previsao.select("setor_id", F.col("data_hora").alias("hora"), "precipitacao_mm")
        .withColumn("previsao_6h", F.sum("precipitacao_mm").over(prev_janela))
        .select("setor_id", "hora", "previsao_6h")
    )

    df = (
        chuva_h.join(sensor_h, ["setor_id", "hora"], "full")
        .join(relatos_h, ["setor_id", "hora"], "left")
        .join(previsao_h, ["setor_id", "hora"], "left")
        .fillna({"qtd_relatos": 0, "qtd_relatos_confirmados": 0, "previsao_6h": 0.0})
        # A precisão por setor vem do sensor: a grade do modelo é maior que uma encosta.
        .withColumn("chuva_1h", F.greatest(F.coalesce("chuva_api", F.lit(0.0)), F.coalesce("chuva_sensor", F.lit(0.0))))
        .withColumn(
            "fonte_chuva",
            F.when(F.coalesce("chuva_sensor", F.lit(0.0)) > F.coalesce("chuva_api", F.lit(0.0)), "sensor_simulado")
            .otherwise("open_meteo"),
        )
        .withColumn("chuva_24h", F.sum("chuva_1h").over(janela.rangeBetween(-23 * 3600, 0)))
        .withColumn("chuva_72h", F.sum("chuva_1h").over(janela.rangeBetween(-71 * 3600, 0)))
        .withColumn(
            "delta_inclinacao",
            F.max("inclinacao_max").over(janela.rangeBetween(-6 * 3600, 0))
            - F.min("inclinacao_max").over(janela.rangeBetween(-6 * 3600, 0)),
        )
    )

    umidade = F.coalesce(F.col("umidade_solo_media"), F.lit(0.0))
    delta = F.coalesce(F.col("delta_inclinacao"), F.lit(0.0))
    atencao = (F.col("chuva_72h") >= LIMIAR_CHUVA_72H) | (umidade >= LIMIAR_UMIDADE_ATENCAO)
    alerta = (
        (atencao & ((F.col("previsao_6h") >= LIMIAR_PREVISAO_6H) | (F.col("chuva_1h") >= LIMIAR_CHUVA_1H_FORTE)))
        | (F.col("qtd_relatos_confirmados") >= 1)
    )
    maximo = (
        (delta >= LIMIAR_DELTA_INCLINACAO)
        & (umidade >= LIMIAR_UMIDADE_SATURADO)
        & (F.col("chuva_1h") >= LIMIAR_CHUVA_1H_MAXIMO)
    )
    nivel = F.when(maximo, 4).when(alerta, 3).when(atencao, 2).otherwise(1)
    fatores = F.concat_ws(
        "; ",
        F.when(F.col("chuva_72h") >= LIMIAR_CHUVA_72H, F.lit("chuva acumulada em 72 h alta")),
        F.when(umidade >= LIMIAR_UMIDADE_ATENCAO, F.lit("solo muito úmido")),
        F.when(F.col("previsao_6h") >= LIMIAR_PREVISAO_6H, F.lit("previsão de chuva forte")),
        F.when(F.col("chuva_1h") >= LIMIAR_CHUVA_1H_FORTE, F.lit("chuva forte agora")),
        F.when(F.col("qtd_relatos_confirmados") >= 1, F.lit("relato confirmado pela Visão")),
        F.when(delta >= LIMIAR_DELTA_INCLINACAO, F.lit("movimento detectado no sensor")),
    )

    return df.select(
        "setor_id",
        F.date_format("hora", "yyyyMMddHH").cast("long").alias("tempo_id"),
        F.round("chuva_1h", 2).alias("chuva_1h"),
        F.round("chuva_24h", 2).alias("chuva_24h"),
        F.round("chuva_72h", 2).alias("chuva_72h"),
        F.round("previsao_6h", 2).alias("previsao_6h"),
        F.round("umidade_solo_media", 2).alias("umidade_solo_media"),
        F.round("umidade_modelo_m3m3", 3).alias("umidade_modelo_m3m3"),
        F.round("delta_inclinacao", 3).alias("delta_inclinacao"),
        "qtd_relatos",
        "qtd_relatos_confirmados",
        "fonte_chuva",
        nivel.alias("nivel_ordem"),
        F.element_at(F.create_map(*[x for k, v in NIVEIS.items() for x in (F.lit(k), F.lit(v))]), nivel).alias("nivel_risco"),
        fatores.alias("fatores"),
    )


def construir_dim_setor(setores, municipios):
    return setores.drop("carregado_em").join(municipios.drop("coletado_em"), "municipio_ibge", "left").select(
        "setor_id", "nome", "bairro", "municipio_ibge", "municipio_nome",
        "populacao_municipio_2022", "grau_risco", "populacao_estimada", "lat", "lon",
    )


def construir_dim_tempo(fato):
    hora = F.to_timestamp(F.col("tempo_id").cast("string"), "yyyyMMddHH")
    return fato.select("tempo_id").distinct().select(
        "tempo_id",
        # Sem fuso: mostra sempre a hora local, em qualquer sessão ou SQL Warehouse.
        hora.cast("timestamp_ntz").alias("data_hora"),
        F.to_date(hora).alias("data"),
        F.hour(hora).alias("hora"),
        F.date_format(hora, "EEEE").alias("dia_semana"),
        F.month(hora).isin(10, 11, 12, 1, 2, 3).alias("estacao_chuvosa"),
    )


def construir_dim_fonte(spark):
    return spark.sql("""
        SELECT * FROM VALUES
          ('chuva', 'Open-Meteo Forecast (histórico recente)', 'api', 'real', 'diária'),
          ('previsao', 'Open-Meteo Forecast (próximas horas)', 'api', 'real', 'diária'),
          ('ibge', 'IBGE Censo 2022, tabela 4709', 'api', 'real', 'diária; o dado não muda'),
          ('sensor', 'Robô sentinela', 'sensor', 'simulada', '15 min, em arquivo diário'),
          ('relatos', 'Relatos de moradores', 'relato', 'simulada', 'diária'),
          ('setores', 'Setores monitorados', 'referencia', 'ilustrativa', 'diária; muda pouco'),
          ('cadastro', 'Cadastro de moradores', 'cadastro', 'simulada', 'diária; muda pouco')
        AS t(fonte_id, nome, tipo, natureza, frequencia)
    """)


def construir_dim_sensor(sensor):
    ultima_rede = sensor.agg(F.max("data_hora").alias("_rede"))
    df = sensor.groupBy("setor_id", "sensor_id").agg(
        F.max("data_hora").alias("ultima_leitura"),
        F.max_by("bateria_pct", "data_hora").alias("bateria_pct"),
    ).crossJoin(ultima_rede)
    minutos = (F.col("_rede").cast("long") - F.col("ultima_leitura").cast("long")) / 60
    return df.select(
        "setor_id", "sensor_id",
        F.col("ultima_leitura").cast("timestamp_ntz").alias("ultima_leitura"),
        "bateria_pct",
        minutos.cast("int").alias("minutos_sem_leitura"),
        F.when(minutos > 30, "offline").otherwise("online").alias("status"),
    )


def construir_dim_morador(moradores):
    return moradores.select("morador_hash", "setor_id", "canal_preferido")


def exportar_mvp(fato, dim_setor, dim_tempo, horas_serie=72):
    """Contrato com o MVP: dados agregados por setor, sem nenhum dado pessoal."""
    com_tempo = fato.join(dim_tempo.select("tempo_id", "data_hora"), "tempo_id")
    atual = manter_mais_recente(com_tempo, ["setor_id"], "tempo_id")
    atual_map = {l["setor_id"]: l.asDict() for l in atual.collect()}
    ultima = com_tempo.agg(F.max("data_hora")).first()[0]
    limite = F.lit(ultima.isoformat()).cast("timestamp_ntz") - F.expr(f"INTERVAL {horas_serie} HOURS")
    serie = {}
    for l in com_tempo.filter(F.col("data_hora") >= limite).orderBy("data_hora").collect():
        serie.setdefault(l["setor_id"], []).append({
            "data_hora": l["data_hora"].isoformat(timespec="minutes"),
            "chuva_1h": l["chuva_1h"],
            "nivel_ordem": l["nivel_ordem"],
        })

    setores = []
    for s in dim_setor.orderBy("setor_id").collect():
        a = atual_map.get(s["setor_id"])
        if not a:
            continue
        setores.append({
            "setor_id": s["setor_id"], "nome": s["nome"], "bairro": s["bairro"],
            "municipio": s["municipio_nome"], "grau_risco": s["grau_risco"],
            "populacao_estimada": s["populacao_estimada"], "lat": s["lat"], "lon": s["lon"],
            "nivel_risco": a["nivel_risco"], "nivel_ordem": a["nivel_ordem"],
            "fatores": a["fatores"], "atualizado_em": a["data_hora"].isoformat(timespec="minutes"),
            "chuva_1h": a["chuva_1h"], "chuva_24h": a["chuva_24h"], "chuva_72h": a["chuva_72h"],
            "previsao_6h": a["previsao_6h"], "umidade_solo_media": a["umidade_solo_media"],
            "serie": serie.get(s["setor_id"], []),
        })
    return {"tabela_origem": "workspace.gs.fato_risco_setor", "gerado_ate": ultima.isoformat(timespec="minutes"),
            "setores": setores}
