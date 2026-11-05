"""Tarefas de coleta: cada função grava um arquivo JSON Lines em chegada/<fonte>/.

`ds` é a data da coleta (entra no nome do arquivo). Os dados medidos são do dia
anterior a `ds`, que já está completo no horário de execução do DAG.
"""
from __future__ import annotations

import gzip
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from encosta import simulador
from encosta.setores import MUNICIPIOS, SETORES

FUSO = "America/Sao_Paulo"
URL_FORECAST = "https://api.open-meteo.com/v1/forecast"
URL_IBGE = "https://servicodados.ibge.gov.br/api/v3/agregados/4709/periodos/2022/variaveis/93"
VARIAVEIS_HORARIAS = "precipitation,soil_moisture_0_to_1cm"
FONTES = ("chuva", "previsao", "ibge", "sensor", "relatos", "setores", "cadastro")


def pasta_chegada() -> Path:
    return Path(os.environ.get("CHEGADA_DIR", "chegada"))


def _agora() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dia_anterior(ds: str) -> date:
    return date.fromisoformat(ds) - timedelta(days=1)


def _obter_json(url: str, tentativas: int = 3):
    """GET com novas tentativas e espera crescente."""
    for tentativa in range(1, tentativas + 1):
        try:
            pedido = urllib.request.Request(url, headers={"User-Agent": "encosta-viva-gs/1.0"})
            with urllib.request.urlopen(pedido, timeout=30) as resposta:
                corpo = resposta.read()
            # O IBGE às vezes responde em gzip mesmo sem o cliente pedir.
            if corpo[:2] == b"\x1f\x8b":
                corpo = gzip.decompress(corpo)
            return json.loads(corpo)
        except (urllib.error.URLError, TimeoutError, ValueError, EOFError, OSError):
            if tentativa == tentativas:
                raise
            time.sleep(2 ** tentativa)


def _gravar(fonte: str, ds: str, registros: list[dict], chegada: Path | None = None) -> Path:
    """Grava de forma atômica, então repetir a tarefa substitui o arquivo sem corrompê-lo."""
    if not registros:
        raise ValueError(f"Fonte {fonte} não retornou registros para {ds}")
    destino = (chegada or pasta_chegada()) / fonte / f"{fonte}_{ds}.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".tmp")
    with temporario.open("w", encoding="utf-8", newline="\n") as arquivo:
        for registro in registros:
            arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
    temporario.replace(destino)
    return destino


def _open_meteo(setor: dict, **parametros) -> dict:
    consulta = {
        "latitude": setor["lat"],
        "longitude": setor["lon"],
        "timezone": FUSO,
        "hourly": VARIAVEIS_HORARIAS,
        **parametros,
    }
    resposta = _obter_json(f"{URL_FORECAST}?{urllib.parse.urlencode(consulta)}")
    return {
        "setor_id": setor["setor_id"],
        "coletado_em": _agora(),
        "latitude_grade": resposta["latitude"],
        "longitude_grade": resposta["longitude"],
        "elevacao_grade": resposta["elevation"],
        "hourly": resposta["hourly"],
    }


def coletar_chuva(ds: str, chegada: Path | None = None) -> str:
    dia = _dia_anterior(ds).isoformat()
    registros = [_open_meteo(s, start_date=dia, end_date=dia) for s in SETORES]
    return str(_gravar("chuva", ds, registros, chegada))


def coletar_previsao(ds: str, chegada: Path | None = None) -> str:
    registros = [_open_meteo(s, forecast_days=2) for s in SETORES]
    return str(_gravar("previsao", ds, registros, chegada))


def coletar_ibge(ds: str, chegada: Path | None = None) -> str:
    localidades = ",".join(MUNICIPIOS)
    resposta = _obter_json(f"{URL_IBGE}?localidades=N6[{localidades}]")
    registros = [
        {
            "municipio_ibge": serie["localidade"]["id"],
            "nome_ibge": serie["localidade"]["nome"],
            "populacao_2022": serie["serie"]["2022"],
            "coletado_em": _agora(),
        }
        for serie in resposta[0]["resultados"][0]["series"]
    ]
    return str(_gravar("ibge", ds, registros, chegada))


def exportar_sensor(ds: str, chegada: Path | None = None) -> str:
    return str(_gravar("sensor", ds, simulador.leituras_sensor(_dia_anterior(ds)), chegada))


def exportar_relatos(ds: str, chegada: Path | None = None) -> str:
    return str(_gravar("relatos", ds, simulador.relatos(_dia_anterior(ds)), chegada))


def exportar_referencias(ds: str, chegada: Path | None = None) -> list[str]:
    return [
        str(_gravar("setores", ds, SETORES, chegada)),
        str(_gravar("cadastro", ds, simulador.cadastro(), chegada)),
    ]


def conferir_chegada(ds: str, chegada: Path | None = None) -> dict:
    """Confere se cada fonte gerou arquivo legível. Falha se alguma estiver ausente."""
    base = chegada or pasta_chegada()
    contagem, problemas = {}, []
    for fonte in FONTES:
        arquivo = base / fonte / f"{fonte}_{ds}.json"
        if not arquivo.is_file() or arquivo.stat().st_size == 0:
            problemas.append(f"{fonte}: arquivo ausente ou vazio")
            continue
        try:
            with arquivo.open(encoding="utf-8") as linhas:
                contagem[fonte] = sum(1 for linha in linhas if json.loads(linha))
        except json.JSONDecodeError as erro:
            problemas.append(f"{fonte}: JSON inválido ({erro})")
    if problemas:
        raise FileNotFoundError(f"Conferência de {ds} falhou: " + "; ".join(problemas))
    return contagem
