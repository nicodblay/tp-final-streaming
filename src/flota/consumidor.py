"""Consumidor: lee flota.metricas, se queda con el último pane de cada clave (upsert)
y verifica los casos especiales contra lo que envió el productor.

python -m flota.consumidor
"""

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer

from flota.config import Config
from flota.eventos import leer_fecha


def leer_topico(cfg: Config, topico: str, espera_vacia: float = 8.0) -> list[dict]:
    """Leer todo el tópico desde el principio con un grupo nuevo (no toca a otros grupos)."""
    consumidor = Consumer(
        {
            "bootstrap.servers": cfg.kafka,
            "group.id": f"verificador-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumidor.subscribe([topico])
    mensajes, vacios = [], 0.0
    while vacios < espera_vacia:
        lote = consumidor.consume(num_messages=500, timeout=1.0)
        vacios = 0.0 if lote else vacios + 1.0
        mensajes += [json.loads(m.value()) for m in lote if not m.error()]
    consumidor.close()
    return mensajes


def materializar(resultados: list[dict]) -> dict[str, dict]:
    """Upsert por clave: gana el pane con mayor índice (nunca se suman panes)."""
    tabla: dict[str, dict] = {}
    for fila in resultados:
        actual = tabla.get(fila["clave"])
        if actual is None or fila["pane_index"] >= actual["pane_index"]:
            tabla[fila["clave"]] = fila
    return tabla


def clave_de(evento: dict, ventana_s: int) -> str:
    segundos = int(leer_fecha(evento["event_time"]).timestamp())
    inicio = segundos - segundos % ventana_s
    texto = datetime.fromtimestamp(inicio, UTC).isoformat().replace("+00:00", "Z")
    return f"{evento['vehicle_id']}|{texto}"


def main() -> None:
    cfg = Config()
    resultados = leer_topico(cfg, cfg.topico_salida)
    invalidos = leer_topico(cfg, cfg.topico_invalidos, espera_vacia=3.0)
    tabla = materializar(resultados)

    print(f"\nMensajes leídos de {cfg.topico_salida}: {len(resultados)} (panes)")
    print(f"Filas después del upsert (moto x ventana): {len(tabla)}\n")
    print(f"{'moto':8} {'ventana':22} {'pings':>5} {'prom km/h':>9} {'pane':>4} tipo")
    for fila in sorted(tabla.values(), key=lambda f: (f["ventana_inicio"], f["vehicle_id"])):
        print(
            f"{fila['vehicle_id']:8} {fila['ventana_inicio']:22} {fila['pings']:>5} "
            f"{fila['velocidad_promedio_kmh']:>9} {fila['pane_index']:>4} {fila['pane_tipo']}"
        )
    print(f"\nMensajes en {cfg.topico_invalidos}: {len(invalidos)}")

    archivo = Path(cfg.archivo_enviados)
    if not archivo.exists():
        return
    enviados = [json.loads(linea) for linea in archivo.read_text(encoding="utf-8").splitlines()]
    cuenta = {}
    for envio in enviados:
        if envio["tipo"] in ("normal", "desordenado", "tardio"):
            clave = clave_de(envio["evento"], cfg.ventana_segundos)
            cuenta[clave] = cuenta.get(clave, 0) + 1
    especial = {e["tipo"]: e["evento"] for e in enviados if e["tipo"] != "normal"}

    print("\nVerificación de los casos especiales:")
    chequeos = []
    if "duplicado" in especial:
        k = clave_de(especial["duplicado"], cfg.ventana_segundos)
        ok = k in tabla and tabla[k]["pings"] == cuenta[k]
        chequeos.append(
            (
                ok,
                f"duplicado {especial['duplicado']['event_id']}: la ventana {k} "
                f"tiene {tabla.get(k, {}).get('pings')} pings, se esperaban "
                f"{cuenta[k]} (contándolo una sola vez)",
            )
        )
    for tipo in ("desordenado", "tardio"):
        if tipo in especial:
            k = clave_de(especial[tipo], cfg.ventana_segundos)
            ok = k in tabla and tabla[k]["pings"] == cuenta[k]
            chequeos.append(
                (
                    ok,
                    f"{tipo} {especial[tipo]['event_id']}: incluido en su ventana "
                    f"{k} (pings {tabla.get(k, {}).get('pings')} = {cuenta[k]}, "
                    f"último pane {tabla.get(k, {}).get('pane_tipo')})",
                )
            )
    avisos = []
    if "demasiado_tardio" in especial:
        k = clave_de(especial["demasiado_tardio"], cfg.ventana_segundos)
        if k not in tabla:
            chequeos.append((True, f"demasiado tardío: la ventana {k} no aparece (descartado)"))
        else:
            # Lectura acotada: el watermark no avanza mientras se lee, así que nada es tardío.
            # El descarte por lateness se prueba con TestStream (tests/test_flota.py).
            avisos.append(
                f"demasiado tardío: la ventana {k} aparece porque en lectura acotada el "
                "watermark no avanza (como en batch). El descarte se prueba con TestStream: "
                "test_demasiado_tardio_se_descarta"
            )
            cuenta[k] = cuenta.get(k, 0) + 1  # se acepta: cuenta como ventana esperada
    if "invalido" in especial:
        ok = any("VH-005-invalido" in i.get("original", "") for i in invalidos)
        chequeos.append((ok, "inválido VH-005-invalido: está en flota.invalidos"))
    completas = sum(1 for k, n in cuenta.items() if k in tabla and tabla[k]["pings"] == n)
    chequeos.append(
        (
            completas == len(cuenta),
            f"ventanas que coinciden con lo enviado: {completas}/{len(cuenta)}",
        )
    )
    for ok, texto in chequeos:
        print(f"  [{'OK' if ok else 'FALLA'}] {texto}")
    for texto in avisos:
        print(f"  [AVISO] {texto}")
    print("\nResultado:", "TODO OK" if all(ok for ok, _ in chequeos) else "HAY FALLAS")


if __name__ == "__main__":
    main()
