"""Geração documentada de sensores, relatos e cadastro (todos fictícios).

A geração é determinística: a mesma data sempre produz o mesmo arquivo. Isso
mantém o pipeline reprodutível. As justificativas estão em docs/dados_simulados.md.
"""
from __future__ import annotations

import hashlib
import math
import random
from datetime import date, datetime, timedelta
from functools import lru_cache

from encosta.setores import EVENTOS_SIMULADOS, SETORES

CANAIS = ["whatsapp", "sms", "telegram"]
TIPOS_SINAL = ["rachadura_parede", "arvore_inclinada", "trinca_solo", "poste_inclinado"]
INICIO_FALHA_S05 = 20  # S05 fica sem leitura à noite: simula falha de energia do painel solar.


def _evento(setor_id: str) -> date | None:
    valor = EVENTOS_SIMULADOS.get(setor_id)
    return date.fromisoformat(valor) if valor else None


@lru_cache(maxsize=None)
def _chuva_hora(setor_id: str, dia: date, hora: int) -> float:
    """Chuva em mm/h. Fora dos eventos, garoa ocasional."""
    rng = random.Random(f"chuva-{setor_id}-{dia}-{hora}")
    chuva = rng.uniform(0.2, 2.5) if rng.random() < 0.12 else 0.0
    evento = _evento(setor_id)
    if evento:
        if dia == evento - timedelta(days=1) and hora >= 12:
            chuva += 3.0
        if dia == evento:
            chuva += 35.0 * math.exp(-(((hora - 18) / 4) ** 2))  # pico às 18h
    return chuva


def _chuva_72h(setor_id: str, momento: datetime) -> float:
    total = 0.0
    for atras in range(72):
        t = momento - timedelta(hours=atras)
        total += _chuva_hora(setor_id, t.date(), t.hour)
    return total


def _inclinacao(setor_id: str, momento: datetime, rng: random.Random) -> float:
    """Graus. A estaca cede a partir do pico do evento e não volta."""
    deslocamento = 0.0
    evento = _evento(setor_id)
    if evento:
        if momento.date() > evento:
            deslocamento = 3.0
        elif momento.date() == evento:
            horas = momento.hour + momento.minute / 60 - 18
            deslocamento = max(0.0, min(3.0, horas))
    return 3.0 + deslocamento + rng.uniform(-0.03, 0.03)


def leituras_sensor(dia: date) -> list[dict]:
    """Uma leitura a cada 15 min por setor, com erros propositais por sensor/dia.

    Os erros (umidade inválida, chuva negativa, horário futuro e duplicata)
    existem para que as regras de qualidade da Prata tenham o que barrar.
    """
    registros = []
    for setor in SETORES:
        setor_id = setor["setor_id"]
        linhas = []
        for minutos in range(0, 24 * 60, 15):
            momento = datetime.combine(dia, datetime.min.time()) + timedelta(minutes=minutos)
            if setor_id == "S05" and momento.hour >= INICIO_FALHA_S05:
                continue
            rng = random.Random(f"sensor-{setor_id}-{momento.isoformat()}")
            chuva_h = _chuva_hora(setor_id, momento.date(), momento.hour)
            umidade = 42 + 0.5 * _chuva_72h(setor_id, momento) + rng.uniform(-1.5, 1.5)
            linhas.append({
                "setor_id": setor_id,
                "sensor_id": f"SN-{setor_id}",
                "ts": momento.strftime("%Y-%m-%dT%H:%M:%S"),
                "umidade_solo_pct": round(max(0.0, min(100.0, umidade)), 2),
                "inclinacao_graus": round(_inclinacao(setor_id, momento, rng), 3),
                "chuva_mm": round(chuva_h / 4, 3),
                "bateria_pct": round(95 - momento.hour * 0.4 + rng.uniform(-1, 1), 1),
                "simulado": True,
            })

        erros = random.Random(f"erros-{setor_id}-{dia}")
        amostra = erros.sample(range(len(linhas)), 4)
        linhas[amostra[0]]["umidade_solo_pct"] = 130.0
        linhas[amostra[1]]["chuva_mm"] = -1.0
        futuro = datetime.fromisoformat(linhas[amostra[2]]["ts"]) + timedelta(days=3650)
        linhas[amostra[2]]["ts"] = futuro.strftime("%Y-%m-%dT%H:%M:%S")
        linhas.append(dict(linhas[amostra[3]]))
        registros.extend(linhas)
    return registros


def relatos(dia: date) -> list[dict]:
    """Relatos de moradores: somente a saída da Visão, nunca a foto."""
    registros = []

    def novo(setor_id: str, hora: int, tipo: str, confianca: float, confirmado: bool) -> dict:
        chave = f"{setor_id}-{dia}-{hora}-{tipo}"
        return {
            "relato_id": "R-" + hashlib.sha1(chave.encode()).hexdigest()[:12],
            "setor_id": setor_id,
            "ts": f"{dia}T{hora:02d}:10:00",
            "tipo_sinal": tipo,
            "confianca_visao": confianca,
            "confirmado_visao": confirmado,
            "simulado": True,
        }

    for setor in SETORES:
        setor_id = setor["setor_id"]
        rng = random.Random(f"relato-{setor_id}-{dia}")
        if rng.random() < 0.3:
            registros.append(novo(setor_id, rng.randint(7, 21), rng.choice(TIPOS_SINAL),
                                  round(rng.uniform(0.25, 0.55), 2), False))
        if _evento(setor_id) == dia:
            for hora, tipo in ((16, "rachadura_parede"), (17, "trinca_solo"), (19, "arvore_inclinada")):
                registros.append(novo(setor_id, hora, tipo, round(rng.uniform(0.75, 0.95), 2), True))

    # Setor inexistente de propósito: a regra de qualidade da Prata precisa barrá-lo.
    registros.append(novo("S99", 12, "rachadura_parede", 0.5, False))
    return registros


def cadastro() -> list[dict]:
    """Moradores FICTÍCIOS. O DDD 00 não existe, então nenhum telefone é real."""
    registros = []
    for setor in SETORES:
        setor_id = setor["setor_id"]
        rng = random.Random(f"cadastro-{setor_id}")
        for n in range(1, 31):
            registros.append({
                "morador_id": f"M-{setor_id}-{n:04d}",
                "nome": f"Morador Ficticio {setor_id}-{n:04d}",
                "telefone": f"+55 00 90000-{n:04d}",
                "setor_id": setor_id,
                "canal_preferido": rng.choice(CANAIS),
                "simulado": True,
            })
    return registros
