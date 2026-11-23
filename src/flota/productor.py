"""Productor sintético: 5 motos mandan su ubicación cada 5 s, en tiempo real.

Además mete a propósito los casos que pide la consigna:
  duplicado, desordenado, tardío (aceptado), demasiado tardío e inválido.
Todo lo enviado se guarda en data/enviados.jsonl para poder verificar el resultado.

python -m flota.productor [--segundos 180] [--semilla 42]
"""

import argparse
import json
import random
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Producer

from flota.config import Config
from flota.eventos import a_iso, codificar

MOTOS = ["VH-001", "VH-002", "VH-003", "VH-004", "VH-005"]
INTERVALO = 5  # segundos entre pings


def nuevo_evento(event_id, vehicle_id, momento, velocidad, rng):
    return {
        "event_id": event_id,
        "event_type": "ubicacion_vehiculo",
        "version": 1,
        "vehicle_id": vehicle_id,
        "event_time": a_iso(momento),
        "lat": round(-25.30 + rng.uniform(-0.03, 0.03), 5),
        "lon": round(-57.60 + rng.uniform(-0.03, 0.03), 5),
        "velocidad_kmh": velocidad,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--segundos", type=int, default=180)
    parser.add_argument("--semilla", type=int, default=42)
    args = parser.parse_args()

    cfg = Config()
    rng = random.Random(args.semilla)
    productor = Producer(
        {
            "bootstrap.servers": cfg.kafka,
            "acks": "all",
            "enable.idempotence": True,
            "partitioner": "murmur2_random",  # mismo reparto por clave que el cliente Java
        }
    )
    salida = Path(cfg.archivo_enviados)
    salida.parent.mkdir(parents=True, exist_ok=True)
    registro = salida.open("w", encoding="utf-8")
    conteo: dict[str, int] = {}

    def enviar(evento, tipo):
        momento = datetime.fromisoformat(evento["event_time"].replace("Z", "+00:00"))
        productor.produce(
            cfg.topico_entrada,
            key=evento["vehicle_id"].encode(),
            value=codificar(evento),
            timestamp=int(momento.timestamp() * 1000),  # timestamp del record = event_time
        )
        registro.write(
            json.dumps({"tipo": tipo, "enviado": a_iso(datetime.now(UTC)), "evento": evento}) + "\n"
        )
        conteo[tipo] = conteo.get(tipo, 0) + 1
        if tipo != "normal":
            print(f"  -> {tipo}: {evento['event_id']} (event_time {evento['event_time']})")

    velocidad = {moto: rng.uniform(20, 40) for moto in MOTOS}
    ultimo = {}
    ticks = args.segundos // INTERVALO
    print(f"Enviando {ticks} rondas de {len(MOTOS)} motos a {cfg.topico_entrada}...")
    inicio = time.monotonic()
    for tick in range(ticks):
        ahora = datetime.now(UTC)
        for moto in MOTOS:
            velocidad[moto] = round(min(70, max(0, velocidad[moto] + rng.uniform(-5, 5))), 1)
            evento = nuevo_evento(f"{moto}-{tick:06d}", moto, ahora, velocidad[moto], rng)
            enviar(evento, "normal")
            ultimo[moto] = evento

        if tick == 6:  # la app reenvía el mismo ping (mismo event_id)
            enviar(ultimo["VH-001"], "duplicado")
        if tick == 8:  # llega 20 s después de ocurrir, detrás de pings más nuevos
            enviar(
                nuevo_evento(
                    "VH-002-desordenado", "VH-002", ahora - timedelta(seconds=20), 35.0, rng
                ),
                "desordenado",
            )
        if tick == 12:  # velocidad imposible: va a flota.invalidos
            malo = nuevo_evento("VH-005-invalido", "VH-005", ahora, 30.0, rng)
            malo["velocidad_kmh"] = -10
            enviar(malo, "invalido")
        if tick == 18:  # ocurrió hace 90 s: su ventana ya cerró, pero está dentro de los 120 s
            enviar(
                nuevo_evento("VH-003-tardio", "VH-003", ahora - timedelta(seconds=90), 90.0, rng),
                "tardio",
            )
        if tick == 24:  # ocurrió hace 6 min: supera la lateness, se descarta
            enviar(
                nuevo_evento(
                    "VH-004-demasiado-tardio", "VH-004", ahora - timedelta(minutes=6), 140.0, rng
                ),
                "demasiado_tardio",
            )

        productor.poll(0)
        time.sleep(max(0, inicio + (tick + 1) * INTERVALO - time.monotonic()))

    productor.flush()
    registro.close()
    print(f"Listo. Enviados por tipo: {conteo}. Registro en {salida}")


if __name__ == "__main__":
    main()
