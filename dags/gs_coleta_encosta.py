"""DAG de coleta da Encosta Viva: requisitos 02 e 03 da disciplina de Big Data."""
from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

from encosta import coleta

log = logging.getLogger(__name__)


def _data_coleta(context) -> str:
    """Execuções manuais podem não ter `ds`; nesse caso, usa a data da execução."""
    return context.get("ds") or context["dag_run"].run_after.strftime("%Y-%m-%d")


def _tarefa(funcao):
    def executar(**context):
        return funcao(_data_coleta(context))
    return executar


def _enviar_telegram(texto: str) -> bool:
    """O token fica em Variable do Airflow. Sem ele, a notificação vai só para o log."""
    token = Variable.get("TELEGRAM_BOT_TOKEN", default=None)
    chat_id = Variable.get("TELEGRAM_CHAT_ID", default=None)
    if not token or not chat_id:
        return False
    dados = urllib.parse.urlencode({"chat_id": chat_id, "text": texto}).encode()
    urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=dados, timeout=30)
    return True


def _notificar(**context):
    ds = _data_coleta(context)
    conferencia = context["ti"].xcom_pull(task_ids="conferir_chegada")
    if conferencia:
        texto = f"Encosta Viva: coleta {ds} concluída. Registros: {json.dumps(conferencia)}"
    else:
        texto = f"Encosta Viva: coleta {ds} FALHOU. Veja a tarefa conferir_chegada."
    canal = "Telegram" if _enviar_telegram(texto) else "log"
    log.info("Notificação via %s: %s", canal, texto)
    if not conferencia:
        raise RuntimeError(texto)  # mantém a execução marcada como falha


with DAG(
    dag_id="gs_coleta_encosta",
    description="Coleta chuva, previsão, IBGE, sensores e relatos para a pasta chegada/",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 25, tz="America/Sao_Paulo"),
    catchup=True,  # recupera os dias anteriores, necessários para a chuva acumulada de 72 h
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["gs", "encosta-viva", "big-data"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    coletas = [
        PythonOperator(task_id=nome, python_callable=_tarefa(funcao))
        for nome, funcao in (
            ("coletar_chuva_openmeteo", coleta.coletar_chuva),
            ("coletar_previsao", coleta.coletar_previsao),
            ("coletar_ibge", coleta.coletar_ibge),
            ("exportar_leituras_sensor", coleta.exportar_sensor),
            ("exportar_relatos", coleta.exportar_relatos),
            ("exportar_referencias", coleta.exportar_referencias),
        )
    ]

    # all_done: confere e notifica mesmo que uma coleta tenha falhado.
    conferir = PythonOperator(
        task_id="conferir_chegada",
        python_callable=_tarefa(coleta.conferir_chegada),
        trigger_rule="all_done",
        retries=0,
    )
    notificar = PythonOperator(
        task_id="notificar",
        python_callable=_notificar,
        trigger_rule="all_done",
        retries=0,
    )

    inicio >> coletas >> conferir >> notificar
