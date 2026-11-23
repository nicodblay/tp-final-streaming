"""Configuración compartida (se puede cambiar con variables de entorno)."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    kafka: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topico_entrada: str = os.getenv("TOPICO_ENTRADA", "flota.ubicaciones")
    topico_salida: str = os.getenv("TOPICO_SALIDA", "flota.metricas")
    topico_invalidos: str = os.getenv("TOPICO_INVALIDOS", "flota.invalidos")
    ventana_segundos: int = int(os.getenv("VENTANA_SEGUNDOS", "60"))
    lateness_segundos: int = int(os.getenv("LATENESS_SEGUNDOS", "120"))
    early_segundos: int = int(os.getenv("EARLY_SEGUNDOS", "10"))
    job_endpoint: str = os.getenv("BEAM_JOB_ENDPOINT", "beam-job-server:8099")
    archivo_enviados: str = os.getenv("ARCHIVO_ENVIADOS", "/app/data/enviados.jsonl")
