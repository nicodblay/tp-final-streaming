"""Lógica de Beam (independiente de Kafka, para poder probarla).

Pasos: parsear y validar -> tiempo de evento -> ventana -> deduplicar -> promedio por moto.
"""

import json
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms import trigger, window
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.windowed_value import PaneInfoTiming

from flota.eventos import leer_fecha, validar


class ParsearYValidar(beam.DoFn):
    """Convierte el mensaje de Kafka en un dict; los inválidos salen por otra salida."""

    INVALIDOS = "invalidos"

    def process(self, mensaje):
        _clave, valor = mensaje
        texto = valor.decode("utf-8", errors="replace")
        try:
            evento = json.loads(texto)
            errores = validar(evento) if isinstance(evento, dict) else ["no es un objeto JSON"]
        except json.JSONDecodeError:
            errores = ["JSON mal formado"]
        if errores:
            yield pvalue.TaggedOutput(self.INVALIDOS, {"errores": errores, "original": texto})
        else:
            yield evento


def con_tiempo_de_evento(evento: dict):
    """Usar event_time (cuándo ocurrió) como timestamp de Beam."""
    return window.TimestampedValue(evento, leer_fecha(evento["event_time"]).timestamp())


def politica_temporal(ventana_s: int, lateness_s: int, early_s: int):
    """Ventana fija + early / on-time / late + modo acumulativo (como en la Tarea 2)."""
    return beam.WindowInto(
        window.FixedWindows(ventana_s),
        trigger=trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(early_s),
            late=trigger.AfterCount(1),
        ),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        allowed_lateness=Duration(seconds=lateness_s),
    )


class Deduplicar(beam.DoFn):
    """Deja pasar cada event_id una sola vez por moto y ventana (igual que en la Tarea 3).

    El timer borra los IDs guardados al terminar la ventana + lateness.
    """

    VISTOS = SetStateSpec("vistos", StrUtf8Coder())
    LIMPIEZA = TimerSpec("limpieza", TimeDomain.WATERMARK)

    def __init__(self, lateness_s: int):
        super().__init__()
        self.lateness_s = lateness_s

    def process(
        self,
        elemento,
        vistos=beam.DoFn.StateParam(VISTOS),
        ventana=beam.DoFn.WindowParam,
        limpieza=beam.DoFn.TimerParam(LIMPIEZA),
    ):
        limpieza.set(ventana.end + Duration(seconds=self.lateness_s))
        event_id = elemento[1]["event_id"]
        if event_id not in set(vistos.read()):
            vistos.add(event_id)
            yield elemento

    @on_timer(LIMPIEZA)
    def limpiar(self, vistos=beam.DoFn.StateParam(VISTOS)):
        vistos.clear()


class VelocidadPromedio(beam.CombineFn):
    """Agregación incremental: (cantidad, suma, máxima) -> promedio."""

    def create_accumulator(self):
        return 0, 0.0, 0.0

    def add_input(self, acumulador, velocidad):
        cantidad, suma, maxima = acumulador
        return cantidad + 1, suma + velocidad, max(maxima, velocidad)

    def merge_accumulators(self, acumuladores):
        acumuladores = list(acumuladores)
        return (
            sum(a[0] for a in acumuladores),
            sum(a[1] for a in acumuladores),
            max((a[2] for a in acumuladores), default=0.0),
        )

    def extract_output(self, acumulador):
        cantidad, suma, maxima = acumulador
        return {
            "pings": cantidad,
            "velocidad_promedio_kmh": round(suma / cantidad, 2) if cantidad else 0.0,
            "velocidad_maxima_kmh": maxima,
        }


class FormatearResultado(beam.DoFn):
    """Agrega ventana, pane y la clave idempotente vehicle_id|window_start."""

    def process(self, elemento, ventana=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        vehicle_id, metricas = elemento
        inicio = ventana.start.to_utc_datetime(has_tz=True).isoformat().replace("+00:00", "Z")
        fin = ventana.end.to_utc_datetime(has_tz=True).isoformat().replace("+00:00", "Z")
        yield {
            "clave": f"{vehicle_id}|{inicio}",
            "vehicle_id": vehicle_id,
            "ventana_inicio": inicio,
            "ventana_fin": fin,
            **metricas,
            "pane_index": pane.index,
            "pane_tipo": PaneInfoTiming.to_string(pane.timing),
        }


def construir_metricas(eventos, ventana_s: int, lateness_s: int, early_s: int):
    """Eventos válidos (dict) -> resultados por moto y ventana."""
    return (
        eventos
        | "Tiempo de evento" >> beam.Map(con_tiempo_de_evento)
        | "Ventana" >> politica_temporal(ventana_s, lateness_s, early_s)
        | "Clave vehicle_id"
        >> beam.Map(lambda e: (e["vehicle_id"], e)).with_output_types(
            beam.typehints.Tuple[str, Any]
        )
        | "Deduplicar" >> beam.ParDo(Deduplicar(lateness_s))
        | "Velocidad" >> beam.Map(lambda kv: (kv[0], float(kv[1]["velocidad_kmh"])))
        | "Promedio por moto" >> beam.CombinePerKey(VelocidadPromedio())
        | "Formatear" >> beam.ParDo(FormatearResultado())
    )


def a_registro_kafka(resultado: dict) -> tuple[bytes, bytes]:
    """La clave del mensaje es la clave idempotente: el consumidor hace upsert por ella."""
    return resultado["clave"].encode(), json.dumps(resultado, sort_keys=True).encode()
