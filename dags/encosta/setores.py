"""Setores monitorados.

Coordenadas são aproximadas. `grau_risco` e `populacao_estimada` são valores
ILUSTRATIVOS, não vêm do mapeamento oficial: troque-os se obtiver a fonte oficial.
"""

SETORES = [
    {"setor_id": "S01", "nome": "Vila Sahy", "bairro": "Barra do Sahy", "municipio_ibge": "3550704",
     "grau_risco": "R4", "populacao_estimada": 2800, "lat": -23.774, "lon": -45.694},
    {"setor_id": "S02", "nome": "Morro da Oficina", "bairro": "Alto da Serra", "municipio_ibge": "3303906",
     "grau_risco": "R4", "populacao_estimada": 1500, "lat": -22.512, "lon": -43.164},
    {"setor_id": "S03", "nome": "Quitandinha", "bairro": "Quitandinha", "municipio_ibge": "3303906",
     "grau_risco": "R3", "populacao_estimada": 1900, "lat": -22.531, "lon": -43.214},
    {"setor_id": "S04", "nome": "Jardim Ângela", "bairro": "Jardim Ângela", "municipio_ibge": "3550308",
     "grau_risco": "R3", "populacao_estimada": 3200, "lat": -23.695, "lon": -46.772},
    {"setor_id": "S05", "nome": "Morro do Algodão", "bairro": "Morro do Algodão", "municipio_ibge": "3510500",
     "grau_risco": "R3", "populacao_estimada": 1200, "lat": -23.660, "lon": -45.440},
]

MUNICIPIOS = sorted({s["municipio_ibge"] for s in SETORES})

# Dias de chuva extrema SIMULADA nos sensores, para exercitar os níveis de alerta.
EVENTOS_SIMULADOS = {
    "S01": "2026-10-30",
    "S02": "2026-11-02",
    "S04": "2026-11-03",
    "S03": "2026-11-10",
    "S05": "2026-11-12",
}
