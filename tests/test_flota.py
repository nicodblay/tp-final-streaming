"""Pruebas de la lógica del pipeline (sin Kafka, con TestPipeline y TestStream)."""

import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to

from flota.eventos import validar
from flota.transforms import ParsearYValidar, VelocidadPromedio, construir_metricas

T0 = 1_800_000_000 // 60 * 60  # un minuto exacto (en segundos desde 1970)


def ping(event_id, segundo, velocidad, moto="VH-001"):
    from datetime import UTC, datetime

    momento = datetime.fromtimestamp(T0 + segundo, UTC).isoformat().replace("+00:00", "Z")
    return {
        "event_id": event_id,
        "event_type": "ubicacion_vehiculo",
        "version": 1,
        "vehicle_id": moto,
        "event_time": momento,
        "lat": -25.3,
        "lon": -57.6,
        "velocidad_kmh": velocidad,
    }


def resumen(resultados):
    return resultados | beam.Map(
        lambda r: (r["vehicle_id"], r["pings"], r["velocidad_promedio_kmh"])
    )


# ---------------------------------------------------------------- lógica simple
def test_validar_detecta_errores():
    assert validar(ping("a", 0, 30)) == []
    malo = ping("b", 0, -10)
    assert validar(malo) == ["velocidad_kmh fuera de rango (0-150)"]
    sin_campo = ping("c", 0, 30)
    del sin_campo["event_time"]
    assert validar(sin_campo) == ["falta event_time"]


def test_combine_calcula_promedio_y_maxima():
    fn = VelocidadPromedio()
    acumulador = fn.create_accumulator()
    for velocidad in (20, 40, 0):
        acumulador = fn.add_input(acumulador, velocidad)
    assert fn.extract_output(acumulador) == {
        "pings": 3,
        "velocidad_promedio_kmh": 20.0,
        "velocidad_maxima_kmh": 40,
    }


def test_invalidos_van_a_otra_salida():
    mensajes = [
        (b"VH-001", json.dumps(ping("ok", 0, 30)).encode()),
        (b"VH-001", json.dumps(ping("malo", 0, 500)).encode()),
        (b"VH-001", b"{esto no es json"),
    ]
    with TestPipeline() as p:
        salida = (
            p
            | beam.Create(mensajes)
            | beam.ParDo(ParsearYValidar()).with_outputs(ParsearYValidar.INVALIDOS, main="validos")
        )
        assert_that(
            salida.validos | beam.Map(lambda e: e["event_id"]), equal_to(["ok"]), label="validos"
        )
        assert_that(
            salida.invalidos | beam.combiners.Count.Globally(), equal_to([2]), label="invalidos"
        )


# ---------------------------------------------------------------- ventanas y duplicados
def test_duplicado_no_cambia_el_promedio_y_ventana_por_event_time():
    eventos = [
        ping("p1", 5, 20),
        ping("p2", 40, 40),
        ping("p2", 40, 40),  # p2 repetido
        ping("p3", 65, 50),
    ]  # otra ventana
    with TestPipeline() as p:
        resultados = construir_metricas(p | beam.Create(eventos), 60, 120, 10)
        # Cada ventana puede emitir varios panes (early y on-time); miramos el on-time.
        on_time = resultados | beam.Filter(lambda r: r["pane_tipo"] == "ON_TIME")
        assert_that(resumen(on_time), equal_to([("VH-001", 2, 30.0), ("VH-001", 1, 50.0)]))


# ---------------------------------------------------------------- tiempo: tardíos
def correr_stream(stream):
    opciones = PipelineOptions()
    opciones.view_as(StandardOptions).streaming = True
    return TestPipeline(options=opciones), stream


def test_tardio_dentro_de_la_lateness_corrige_el_resultado():
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([ping("p1", 5, 20), ping("p2", 40, 40)])
        .advance_watermark_to(T0 + 60)  # cierra la ventana: ON_TIME 30 km/h
        .add_elements([ping("tarde", 50, 60)])  # llega tarde pero dentro de 120 s
        .advance_watermark_to_infinity()
    )
    p, stream = correr_stream(stream)
    with p:
        resultados = construir_metricas(p | stream, 60, 120, 10)
        assert_that(
            resultados
            | beam.Map(lambda r: (r["pane_tipo"], r["pings"], r["velocidad_promedio_kmh"])),
            equal_to([("ON_TIME", 2, 30.0), ("LATE", 3, 40.0)]),
        )


def test_demasiado_tardio_se_descarta():
    stream = (
        BeamTestStream()
        .advance_watermark_to(T0)
        .add_elements([ping("p1", 5, 20)])
        .advance_watermark_to(T0 + 60 + 121)  # pasó fin de ventana + 120 s
        .add_elements([ping("muy-tarde", 30, 140)])
        .advance_watermark_to_infinity()
    )
    p, stream = correr_stream(stream)
    with p:
        resultados = construir_metricas(p | stream, 60, 120, 10)
        assert_that(resumen(resultados), equal_to([("VH-001", 1, 20.0)]))
