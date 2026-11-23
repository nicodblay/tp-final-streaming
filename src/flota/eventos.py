"""Contrato del evento `ubicacion_vehiculo` (versión 1).

{
  "event_id": "VH-001-000012",        identificador único y estable (sirve para deduplicar)
  "event_type": "ubicacion_vehiculo",
  "version": 1,                       versión del esquema
  "vehicle_id": "VH-001",             clave de negocio y de Kafka
  "event_time": "2026-11-22T13:00:05Z",  cuándo ocurrió (reloj del celular)
  "lat": -25.29, "lon": -57.63,       payload
  "velocidad_kmh": 32.5
}

Evolución: solo se agregan campos opcionales; un cambio incompatible sería `version: 2`.
"""

import json
from datetime import UTC, datetime

CAMPOS = (
    "event_id",
    "event_type",
    "version",
    "vehicle_id",
    "event_time",
    "lat",
    "lon",
    "velocidad_kmh",
)


def a_iso(momento: datetime) -> str:
    return momento.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def leer_fecha(texto: str) -> datetime:
    fecha = datetime.fromisoformat(texto.replace("Z", "+00:00"))
    if fecha.tzinfo is None:
        raise ValueError("event_time sin zona horaria")
    return fecha.astimezone(UTC)


def validar(evento: dict) -> list[str]:
    """Devolver la lista de errores del evento (vacía si es válido)."""
    errores = [f"falta {campo}" for campo in CAMPOS if campo not in evento]
    if errores:
        return errores
    if evento["version"] != 1:
        errores.append(f"versión no soportada: {evento['version']}")
    if not str(evento["vehicle_id"]).startswith("VH-"):
        errores.append("vehicle_id inválido")
    try:
        leer_fecha(evento["event_time"])
    except (TypeError, ValueError):
        errores.append("event_time inválido")
    if (
        not isinstance(evento["velocidad_kmh"], int | float)
        or not 0 <= evento["velocidad_kmh"] <= 150
    ):
        errores.append("velocidad_kmh fuera de rango (0-150)")
    return errores


def codificar(evento: dict) -> bytes:
    return json.dumps(evento, separators=(",", ":")).encode()
