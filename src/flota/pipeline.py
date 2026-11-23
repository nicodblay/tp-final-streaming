"""Pipeline Kafka -> Beam -> Kafka, ejecutado en Flink (como en la Clase 7).

python -m flota.pipeline
"""

import argparse
import json

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import KV

from flota.config import Config
from flota.transforms import ParsearYValidar, a_registro_kafka, construir_metricas


def servicio_kafka_java():
    """KafkaIO es Java: corre como proceso dentro del TaskManager de Flink."""
    return default_io_expansion_service(
        append_args=[
            "--defaultEnvironmentType=PROCESS",
            '--defaultEnvironmentConfig={"command":"/opt/apache/beam/boot"}',
        ]
    )


def opciones(cfg: Config) -> PipelineOptions:
    return PipelineOptions(
        [
            "--runner=PortableRunner",
            f"--job_endpoint={cfg.job_endpoint}",
            "--environment_type=PROCESS",
            '--environment_config={"command":"/opt/flota/python-sdk/boot"}',
            # Lectura acotada, como el smoke test de la Clase 7: la lectura continua (SDF)
            # de KafkaIO quedaba trabada en este Flink (ver README, "Límites").
            "--parallelism=1",
            "--job_name=flota-metricas",
            "--experiments=use_sdf_read",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--segundos-lectura", type=int, default=60,
                        help="cuánto tiempo lee de Kafka antes de terminar")
    args = parser.parse_args()
    cfg = Config()
    kafka_java = servicio_kafka_java()
    productor_kafka = {"bootstrap.servers": cfg.kafka, "enable.idempotence": "true", "acks": "all"}

    with beam.Pipeline(options=opciones(cfg)) as p:
        mensajes = p | "Leer flota.ubicaciones" >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": cfg.kafka,
                "group.id": "beam-flota-metricas",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",  # cada corrida relee todo (replay)
            },
            topics=[cfg.topico_entrada],
            timestamp_policy=ReadFromKafka.create_time_policy,
            max_read_time=args.segundos_lectura,
            expansion_service=kafka_java,
        )
        separados = mensajes | "Validar" >> beam.ParDo(ParsearYValidar()).with_outputs(
            ParsearYValidar.INVALIDOS, main="validos"
        )

        (
            construir_metricas(
                separados.validos, cfg.ventana_segundos, cfg.lateness_segundos, cfg.early_segundos
            )
            | "A registro Kafka" >> beam.Map(a_registro_kafka).with_output_types(KV[bytes, bytes])
            | "Escribir flota.metricas"
            >> WriteToKafka(
                producer_config=productor_kafka,
                topic=cfg.topico_salida,
                expansion_service=kafka_java,
            )
        )
        (
            separados.invalidos
            | "Inválido a registro"
            >> beam.Map(lambda x: (b"invalido", json.dumps(x).encode())).with_output_types(
                KV[bytes, bytes]
            )
            | "Escribir flota.invalidos"
            >> WriteToKafka(
                producer_config=productor_kafka,
                topic=cfg.topico_invalidos,
                expansion_service=kafka_java,
            )
        )


if __name__ == "__main__":
    main()
