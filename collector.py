import os
import time
from datetime import datetime
import pytz

from utils import (
    cargar_parques,
    obtener_horario,
    detectar_evento,
    recoger_datos
)

CONFIG_PATH = os.path.join("config", "parks.json")

print("🚀 Iniciando colector de datos...\n")

# ---------------------------------------------------------------
# Cargar parques
# ---------------------------------------------------------------
parques = cargar_parques(CONFIG_PATH)

# ---------------------------------------------------------------
# Obtener horas de apertura y cierre
# ---------------------------------------------------------------
aperturas = {}
cierres = {}

for parque in parques:
    nombre = parque["name"]
    timezone = parque["timezone"]
    zona = pytz.timezone(timezone)

    hoy = datetime.now(zona).date().isoformat()
    apertura, cierre = obtener_horario(parque["entity_id"], hoy)

    if apertura and cierre:
        aperturas[nombre] = apertura
        cierres[nombre] = cierre
        print(f"🕓 {nombre}: abre a las {apertura.astimezone(zona).strftime('%H:%M')} y cierra a las {cierre.astimezone(zona).strftime('%H:%M')}")
    else:
        print(f"⚠️ No se pudo determinar horario de {nombre}")

print()

# ---------------------------------------------------------------
# Resumen
# ---------------------------------------------------------------
resumen_parques = {
    parque["name"]: {"registros": 0, "archivo": ""}
    for parque in parques
}

# ---------------------------------------------------------------
# BUCLE PRINCIPAL
# ---------------------------------------------------------------
while True:
    ahora_global = datetime.utcnow().replace(tzinfo=pytz.utc)

    # Verificación: ¿han cerrado todos?
    if all(ahora_global >= cierre for cierre in cierres.values()):
        print("\n🏁 Todos los parques han cerrado. Finalizando recolección.")
        break

    # Recoger datos parque por parque
    for parque in parques:
        nombre = parque["name"]
        timezone = parque["timezone"]
        zona = pytz.timezone(timezone)
        ahora_local = datetime.now(zona)

        # Parques sin horarios → se saltan
        if nombre not in aperturas or nombre not in cierres:
            continue

        # Antes de la apertura
        if ahora_local < aperturas[nombre]:
            print(f"⏳ {nombre} aún no ha abierto.")
            continue

        # Después del cierre
        if ahora_local >= cierres[nombre]:
            print(f"⏹️  {nombre} ya ha cerrado.")
            continue

        # Evento activo
        evento_activo = detectar_evento(parque, ahora_local.date())

        # Recoger datos
        nuevos_registros, archivo = recoger_datos(
            parque, evento_activo, ahora_local
        )

        resumen_parques[nombre]["registros"] += nuevos_registros
        resumen_parques[nombre]["archivo"] = archivo

    print("⏳ Esperando 5 minutos para la próxima recolección...\n")
    time.sleep(5 * 60)

# ---------------------------------------------------------------
# RESUMEN FINAL
# ---------------------------------------------------------------
print("\n📊 RESUMEN DEL DÍA")
print("=" * 40)

for nombre, info in resumen_parques.items():
    if info["registros"] > 0:
        print(f"🎢 {nombre}: {info['registros']} registros → {info['archivo']}")
    else:
        print(f"🎢 {nombre}: sin datos guardados")

print("=" * 40)
print("✅ Recolección completada con éxito.")