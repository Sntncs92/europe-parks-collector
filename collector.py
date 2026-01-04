import os
import time
from datetime import datetime, timedelta
import pytz

from utils import (
    cargar_parques,
    obtener_horario,
    detectar_evento,
    recoger_datos
)

CONFIG_PATH = os.path.join("config", "parks.json")
INTERVALO_SEGUNDOS = 15 * 60  # Intervalo de 15 minutos por parque

print("🚀 Iniciando colector de datos 24/7 global...\n")

# ---------------------------------------------------------------
# Cargar parques
# ---------------------------------------------------------------
parques = cargar_parques(CONFIG_PATH)

# ---------------------------------------------------------------

# Inicializar cache de horarios por parque
# ---------------------------------------------------------------
horarios_cache = {}
for parque in parques:
    horarios_cache[parque["name"]] = {
        "fecha": None,
        "apertura": None,
        "cierre": None,
        "last_collected": None
    }

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
# Resumen por parque
# ---------------------------------------------------------------
resumen_parques = {parque["name"]: {"registros": 0, "archivo": ""} for parque in parques}

# ---------------------------------------------------------------
# Bucle principal
# ---------------------------------------------------------------
while True:
    ahora_utc = datetime.utcnow().replace(tzinfo=pytz.utc)

    for parque in parques:
        nombre = parque["name"]
        timezone = parque["timezone"]
        zona = pytz.timezone(timezone)
        ahora_local = datetime.now(zona)


        # --- Actualizar horarios si es un nuevo día ---
        cache = horarios_cache[nombre]
        if cache["fecha"] != ahora_local.date():
            apertura, cierre = obtener_horario(parque["entity_id"], ahora_local.date().isoformat())
            cache.update({
                "fecha": ahora_local.date(),
                "apertura": apertura,
                "cierre": cierre,
                "last_collected": None
            })
            if apertura and cierre:
                print(f"🕓 {nombre}: abre a las {apertura.astimezone(zona).strftime('%H:%M')} y cierra a las {cierre.astimezone(zona).strftime('%H:%M')}")
            else:
                print(f"⚠️ No se pudo determinar horario de {nombre}")

        # --- Parques sin horarios válidos ---
        if not cache["apertura"] or not cache["cierre"]:
            continue

        # --- Antes de apertura ---
        if ahora_local < cache["apertura"]:
            print(f"⏳ {nombre} aún no ha abierto.")

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

        # --- Después de cierre ---
        if ahora_local >= cache["cierre"]:
            print(f"⏹️ {nombre} ya ha cerrado.")
            continue

        # --- Comprobar intervalo desde la última recogida ---
        ultima = cache["last_collected"]
        if ultima and (ahora_local - ultima).total_seconds() < INTERVALO_SEGUNDOS:
            continue

        # --- Evento activo ---
        evento_activo = detectar_evento(parque, ahora_local.date())

        # --- Recoger datos ---
        nuevos_registros, archivo = recoger_datos(parque, evento_activo, ahora_local)
        resumen_parques[nombre]["registros"] += nuevos_registros
        resumen_parques[nombre]["archivo"] = archivo

        # --- Actualizar última recogida ---
        cache["last_collected"] = ahora_local

    # --- Esperar 10 minutos antes de volver a revisar ---
    time.sleep(600)


    # Opcional: imprimir resumen temporal
    print("\n📊 Resumen temporal:")
    for nombre, info in resumen_parques.items():
        print(f"🎢 {nombre}: {info['registros']} registros → {info['archivo'] if info['archivo'] else 'Ninguno'}")
    print("-" * 40)

for nombre, info in resumen_parques.items():
    if info["registros"] > 0:
        print(f"🎢 {nombre}: {info['registros']} registros → {info['archivo']}")
    else:
        print(f"🎢 {nombre}: sin datos guardados")

print("=" * 40)
print("✅ Recolección completada con éxito.")
