"""Genera docs/Documento_Tecnico_TP.docx. Correr con un Python que tenga python-docx."""
import json
import sys
from pathlib import Path

from docx.shared import Cm, Pt

sys.path.insert(0, r"C:\Users\Nico2\Documents\nico\master\conocimiento_claude\_herramientas")
from informe_docx import Informe

DOCS = Path(__file__).resolve().parent
inf = Informe(str(DOCS))
for lado in ("left_margin", "right_margin", "top_margin", "bottom_margin"):
    setattr(inf.doc.sections[0], lado, Cm(2.0))
inf.doc.styles["Normal"].font.size = Pt(10.5)
_tabla = inf.table
inf.table = lambda h, f, w, highlight=(): _tabla(h, [[str(c).replace("`", "") for c in r] for r in f], w, highlight)


def codigo(texto):
    par = inf.doc.add_paragraph()
    run = par.add_run(texto)
    run.font.name, run.font.size = "Consolas", Pt(8.5)


inf.titulo("Trabajo Práctico Final — Velocidad de una flota de delivery en tiempo real")
inf.meta("Integrante: ", "Nicolás David Blanco Yaluff (trabajo individual)")
inf.meta("Materia: ", "Streaming de datos y sus aplicaciones — Maestría en Inteligencia Artificial, FP-UNA")
inf.meta("Repositorio: ", "flota-streaming (código, Docker Compose, pruebas y evidencia)")

# 1 ---------------------------------------------------------------
inf.h1("1. Problema, usuarios y decisiones que habilita")
inf.p("Una empresa de delivery quiere conocer la **velocidad promedio de cada moto por minuto** mientras trabajan sus repartidores. "
      "Lo usa la **torre de control**: si una moto anda muy lenta o lleva mucho tiempo detenida, puede avisar al cliente de un posible atraso "
      "o reasignar pedidos. Es el mismo caso de la Tarea 1 (tópicos y claves) y de la Tarea 2 (política temporal), ahora implementado.")
inf.p("La dificultad es la de toda la materia: los celulares pierden señal, reenvían pings y mandan datos atrasados. El pipeline tiene "
      "que usar **cuándo ocurrió** cada ubicación, no cuándo llegó, y no contar dos veces un ping repetido.")

# 2 ---------------------------------------------------------------
inf.h1("2. Arquitectura")
inf.fig("diagrama_arquitectura.png", "Figura 1. Fuente → Kafka → Beam (sobre Flink) → Kafka → consumidor.", 16.8)
inf.table(["Componente", "Qué hace"], [
    ["Productor sintético (Python)", "5 motos mandan su ubicación cada 5 s durante 3 minutos. Mete a propósito un duplicado, un desordenado, un tardío, uno demasiado tardío y uno inválido. Guarda lo enviado para verificar después."],
    ["Kafka (1 broker, Docker)", "Guarda los eventos de entrada y los resultados. Permite volver a leer todo (replay)."],
    ["Apache Beam + Flink", "Beam describe el pipeline y Flink lo ejecuta (PortableRunner, como en la Clase 7). KafkaIO lee y escribe en Kafka."],
    ["Consumidor", "Lee los resultados, se queda con el último valor de cada clave (upsert) y compara contra lo que envió el productor."],
], [4.3, 12.5])

# 3 ---------------------------------------------------------------
inf.h1("3. Contrato de eventos, tópicos y claves")
inf.p("Cada ubicación es un JSON con `version` para poder evolucionar el esquema. Solo se agregan campos opcionales; un cambio incompatible sería `version: 2`.")
codigo('{"event_id": "VH-001-000012", "event_type": "ubicacion_vehiculo", "version": 1,\n'
       ' "vehicle_id": "VH-001", "event_time": "2026-11-22T23:48:05Z",\n'
       ' "lat": -25.29, "lon": -57.63, "velocidad_kmh": 32.5}')
inf.bullets([
    "`event_id`: identificador único y estable; se usa para deduplicar.",
    "`vehicle_id`: clave de negocio y **clave del mensaje de Kafka**.",
    "`event_time`: cuándo ocurrió, según el celular. El productor lo pone también como timestamp del mensaje de Kafka.",
    "Payload: `lat`, `lon` y `velocidad_kmh` (se valida que esté entre 0 y 150).",
])
inf.table(["Tópico", "Clave", "Particiones", "Retención", "Propósito"], [
    ["flota.ubicaciones", "vehicle_id", "3", "7 días", "eventos de entrada"],
    ["flota.metricas", "vehicle_id|ventana", "3", "3 días", "resultados (velocidad por moto y minuto)"],
    ["flota.invalidos", "—", "1", "14 días", "eventos que no cumplen el contrato, con el motivo"],
], [3.4, 3.4, 2.2, 2.0, 5.8])
inf.p("**Clave y particiones.** Con `vehicle_id` todos los pings de una moto van a la misma partición y Kafka los guarda en orden. "
      "Verifiqué con el mismo cálculo de partición de Kafka (Tarea 1) que las 5 motos caen en las 3 particiones, así ninguna queda vacía. "
      "Para la demo alcanzan 3 particiones; para 200 motos la Tarea 1 proponía 6.")
inf.p("**Esquema de salida:** cada resultado lleva `clave`, `vehicle_id`, `ventana_inicio`, `ventana_fin`, `pings`, `velocidad_promedio_kmh`, "
      "`velocidad_maxima_kmh`, `pane_index` y `pane_tipo`.")

# 4 ---------------------------------------------------------------
inf.h1("4. Pipeline Beam: tiempo, ventanas, duplicados e idempotencia")
inf.table(["Paso", "Transformación", "Decisión"], [
    ["Leer", "ReadFromKafka (KafkaIO)", "Lee flota.ubicaciones desde el principio."],
    ["Validar", "ParDo con dos salidas", "Los inválidos van a flota.invalidos con el motivo, no se pierden en silencio."],
    ["Tiempo", "TimestampedValue(event_time)", "Se usa cuándo ocurrió el hecho (Clase 4)."],
    ["Ventana", "FixedWindows(60 s), lateness 120 s, early / on-time / late, acumulativo", "1 min y 2 min para que la demo dure minutos; en producción serían 5 y 10 min (Tarea 2)."],
    ["Deduplicar", "DoFn con estado (SetState) + timer", "Recuerda los event_id de cada moto y ventana, y los borra al vencer la lateness (Tarea 3)."],
    ["Agregar", "CombinePerKey(VelocidadPromedio)", "Agregación incremental por moto: cantidad, suma y máxima."],
    ["Escribir", "WriteToKafka con clave vehicle_id|ventana", "Salida idempotente: el consumidor hace upsert por esa clave."],
], [2.0, 6.4, 8.4])
inf.p("**Semántica de entrega, tramo por tramo.** El productor usa `acks=all` y es idempotente, así que no se pierden mensajes ni se "
      "duplican por reintentos de red. Si la app reenvía un ping, ese duplicado lo elimina el pipeline por `event_id`. Leer otra vez el "
      "tópico (replay) vuelve a procesar todo (*at-least-once*), pero la salida tiene clave estable y el consumidor reemplaza en lugar de sumar. "
      "Por eso la **tabla materializada** queda correcta. **No** se afirma *exactly-once* de punta a punta: la cola de inválidos solo agrega "
      "y repite sus mensajes en cada replay.")

# 5 ---------------------------------------------------------------
inf.h1("5. Pruebas y evidencia")
inf.p("**Pruebas automáticas** (`tests/test_flota.py`, 6 de 6 en verde, ejecutadas dentro de Docker):")
inf.table(["Prueba", "Qué demuestra"], [
    ["test_validar_detecta_errores", "el contrato rechaza velocidad fuera de rango y campos faltantes"],
    ["test_combine_calcula_promedio_y_maxima", "la agregación incremental es correcta"],
    ["test_invalidos_van_a_otra_salida", "los inválidos se separan sin frenar el pipeline"],
    ["test_duplicado_no_cambia_el_promedio_y_ventana_por_event_time", "un duplicado no cambia el resultado y cada evento cae en la ventana de su event_time"],
    ["test_tardio_dentro_de_la_lateness_corrige_el_resultado (TestStream)", "un tardío produce un pane LATE: 30 → 40 km/h"],
    ["test_demasiado_tardio_se_descarta (TestStream)", "pasado el fin de ventana + 120 s, el evento se descarta"],
], [8.0, 8.8])
inf.p("**Demo de punta a punta** (fuente → Kafka → Beam/Flink → Kafka → consumidor). Salida del consumidor en `docs/evidencia/`:")
inf.table(["Caso", "Resultado"], [
    ["Ventanas moto × minuto", "21 de 21 coinciden con lo enviado"],
    ["Duplicado (VH-001-000006)", "la ventana tiene 12 pings, no 13"],
    ["Desordenado (VH-002, 20 s tarde)", "incluido en la ventana de su event_time"],
    ["Tardío (VH-003, 90 s tarde)", "incluido en la ventana de su event_time"],
    ["Inválido (velocidad −10)", "aparece en flota.invalidos"],
    ["Replay: correr el pipeline 2 veces", "42 mensajes en flota.metricas → 21 filas tras el upsert"],
], [6.0, 10.8], highlight=(5,))

# 6 ---------------------------------------------------------------
inf.h1("6. Límites, supuestos y mejoras")
inf.bullets([
    "**Lectura acotada.** Con la lectura continua de KafkaIO (SDF), el job corría en Flink pero no emitía resultados: la lectura quedaba trabada y los checkpoints no se completaban. "
    "Lo aislé con pruebas mínimas: Kafka → Kafka y Kafka → Python → Kafka funcionan con lectura acotada y fallan sin límite. Por eso el pipeline lee con un tiempo máximo (`max_read_time`), "
    "como el smoke test del laboratorio de la Clase 7. **Consecuencia:** mientras lee, el watermark no avanza (como en el batch de la Tarea 3) y en la demo el evento «demasiado tardío» se acepta. "
    "El descarte por lateness queda demostrado con `TestStream`.",
    "Entorno de laboratorio: un broker, un TaskManager y paralelismo 1. En producción: 3 brokers con replicación 3, varios TaskManagers y checkpoints en almacenamiento durable.",
    "Datos sintéticos con semilla fija. Los horarios cambian en cada corrida porque se usa la hora real.",
    "Mejoras posibles: resolver la lectura continua (probar otra versión de Beam o Flink, o más paralelismo como el laboratorio), hacer idempotente la cola de inválidos y agregar un dashboard.",
])
inf.h1("7. Integrantes y contribuciones")
inf.p("Trabajo individual de **Nicolás David Blanco Yaluff**: diseño del caso y del contrato, productor, pipeline Beam, consumidor, "
      "infraestructura con Docker, pruebas, diagnóstico de la lectura de Kafka y documentación.")
inf.p("**Referencias.** R. Parra, Clases 1 a 7 de *Streaming de datos y sus aplicaciones* y laboratorio `fpuna-clase7-taxi-streaming`, FP-UNA, 2026. "
      "Apache Beam, *Programming Guide* y KafkaIO (beam.apache.org). Apache Kafka, *Documentation* (kafka.apache.org).")

inf.save(str(DOCS / "Documento_Tecnico_TP.docx"))
