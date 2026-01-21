import os
import time
from datetime import datetime
import pytz
from utils.logger import setup_logger


from utils.config_loader import cargar_parques
from utils.event_detector import detectar_evento

from collectors.schedule_client import obtener_horario
from collectors.live_client import get_live_data
from collectors.data_parser import parse_live_data
from collectors.csv_writer import save_to_csv

logger = setup_logger()
CONFIG_PATH = os.path.join("config", "parks.json")
INTERVALO_SEGUNDOS = 15 * 60

print("🚀 Iniciando colector de datos...\n")

# Cargar parques
parques = cargar_parques(CONFIG_PATH)

# Cache horarios
horarios_cache = {
    parque["name"]: {
        "fecha": None,
        "apertura": None,
        "cierre": None,
        "last_collected": None
    } for parque in parques
}

# Resumen
resumen_parques = {parque["name"]: {"registros": 0, "archivo": ""} for parque in parques}

# Bucle principal
while True:
    for parque in parques:
        nombre = parque["name"]
        zona = pytz.timezone(parque["timezone"])
        ahora_local = datetime.now(zona)

        cache = horarios_cache[nombre]

        # Actualizar horarios si cambia el día
        if cache["fecha"] != ahora_local.date():
            apertura, cierre = obtener_horario(parque["entity_id"], ahora_local.date().isoformat())
            cache.update({
                "fecha": ahora_local.date(),
                "apertura": apertura,
                "cierre": cierre,
                "last_collected": None
            })

        # Saltar si no hay horarios
        if not cache["apertura"] or not cache["cierre"]:
            continue

        # Antes de apertura
        if ahora_local < cache["apertura"]:
            continue

        # Después de cierre
        if ahora_local >= cache["cierre"]:
            continue

        # Control de intervalo
        ultima = cache["last_collected"]
        if ultima and (ahora_local - ultima).total_seconds() < INTERVALO_SEGUNDOS:
            continue

        # Detectar evento
        evento_activo = detectar_evento(parque, ahora_local.date())

        # Llamar API live
        raw_data = get_live_data(parque["entity_id"])

        # Parsear datos
        filas = parse_live_data(raw_data, evento_activo, ahora_local)

        # Guardar CSV
        continent = parque.get("continent")  
        country = parque.get("country")
        nuevos, archivo = save_to_csv(nombre, filas, ahora_local, continent, country)


        resumen_parques[nombre]["registros"] += nuevos
        resumen_parques[nombre]["archivo"] = archivo

        cache["last_collected"] = ahora_local

        logger.info(f"✅ {nombre}: {nuevos} registros")

    # Espera
    time.sleep(600)
