"""Genera docs/diagrama_arquitectura.png. Uso: python docs/diagrama.py"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

AQUI = Path(__file__).resolve().parent
NAR, AZUL, VERDE, GRIS, ROJO = "#E67E22", "#2E5597", "#27AE60", "#555555", "#C0392B"

fig, ax = plt.subplots(figsize=(12, 4.4))
ax.set_xlim(0, 18); ax.set_ylim(0, 6.6); ax.axis("off")


def caja(x, y, w, h, titulo, sub, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.05,rounding_size=0.15",
                                fc="white", ec=color, lw=2))
    ax.text(x + w / 2, y + h * 0.7, titulo, ha="center", va="center", fontsize=10.5,
            weight="bold", color=color)
    ax.text(x + w / 2, y + h * 0.32, sub, ha="center", va="center", fontsize=8, color="#333")


def flecha(p, q, color=GRIS, texto=""):
    ax.annotate("", xy=q, xytext=p, arrowprops=dict(arrowstyle="-|>", lw=1.6, color=color))
    if texto:
        ax.text((p[0] + q[0]) / 2, (p[1] + q[1]) / 2 + 0.2, texto, ha="center", fontsize=7.5,
                color=color)


caja(0.2, 2.4, 2.8, 1.8, "1. FUENTE", "productor sintético\n5 motos · ping c/5 s\n+ duplicado, tardío,\ninválido", NAR)
caja(3.8, 3.4, 3.4, 1.4, "2. flota.ubicaciones", "KAFKA · clave vehicle_id\n3 particiones · 7 días", AZUL)
caja(8.0, 2.0, 3.6, 2.8, "3. APACHE BEAM", "(runner: Flink)\nKafkaIO → validar\n→ ventana 1 min (event time)\n→ deduplicar (estado+timer)\n→ CombinePerKey (promedio)", VERDE)
caja(12.4, 3.4, 3.0, 1.4, "flota.metricas", "KAFKA · clave\nvehicle_id|ventana", AZUL)
caja(12.4, 0.9, 3.0, 1.4, "flota.invalidos", "KAFKA · eventos que\nno cumplen el contrato", ROJO)
caja(15.9, 3.3, 1.9, 1.6, "4. SALIDA", "consumidor\nupsert por clave\n+ verificación", GRIS)

flecha((3.0, 3.6), (3.8, 4.0), NAR)
flecha((7.2, 4.1), (8.0, 3.8))
flecha((11.6, 3.9), (12.4, 4.1), VERDE)
flecha((11.6, 2.4), (12.4, 1.6), ROJO)
flecha((15.4, 4.1), (15.9, 4.1))
ax.text(9.8, 5.5, "Todo corre con Docker Compose: Kafka · Flink (JobManager + TaskManager) · Beam Job Server",
        ha="center", fontsize=8.5, color=GRIS, style="italic")
fig.tight_layout()
fig.savefig(AQUI / "diagrama_arquitectura.png", dpi=200)
print("OK")
