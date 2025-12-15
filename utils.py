import json
import os
import requests
from datetime import datetime
import pytz
import csv

BASE_URL = "https://api.themeparks.wiki/v1/entity"


# ---------------------------------------------------------------
# Cargar parques desde el JSON
# ---------------------------------------------------------------
def cargar_parques(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)


# ---------------------------------------------------------------
# Obtener horario del parque
# ---------------------------------------------------------------
def obtener_horario(entity_id, hoy):
    schedule_url = f"{BASE_URL}/{entity_id}/schedule"

    try:
        response = requests.get(schedule_url)
        response.raise_for_status()
        data = response.json()

        horarios = data.get("schedule", [])
        horario_hoy = next(
            (h for h in horarios if h["date"] == hoy and h.get("type") == "OPERATING"),
            None
        )

        if horario_hoy:
            apertura = horario_hoy.get("openingTime")
            cierre = horario_hoy.get("closingTime")

            if apertura and cierre:
                return (
                    datetime.fromisoformat(apertura),
                    datetime.fromisoformat(cierre)
                )
    except:
        pass

    return (None, None)


# ---------------------------------------------------------------
# Detectar si hay un evento activo en el parque
# ---------------------------------------------------------------
def detectar_evento(parque, fecha):
    for evento in parque.get("eventos", []):
        desde = datetime.fromisoformat(evento["desde"]).date()
        hasta = datetime.fromisoformat(evento["hasta"]).date()

        if desde <= fecha <= hasta:
            return evento["nombre"]

    return ""


# ---------------------------------------------------------------
# Recoger datos del endpoint /live
# ---------------------------------------------------------------
def recoger_datos(parque, evento_activo, ahora):
    nombre = parque["name"]
    entity_id = parque["entity_id"]

    live_url = f"{BASE_URL}/{entity_id}/live"

    # Archivo CSV
    nombre_archivo = f"{nombre.replace(' ', '')}_{ahora.date().isoformat()}.csv"
    ruta_archivo = os.path.join("data", nombre_archivo)
    os.makedirs("data", exist_ok=True)

    archivo_existe = os.path.isfile(ruta_archivo)

    try:
        response_live = requests.get(live_url)
        response_live.raise_for_status()
        data_live = response_live.json()

        atracciones = data_live.get("liveData", [])

        if not atracciones:
            print(f"⚠️  No se encontraron atracciones en {nombre}")
            return 0, ruta_archivo

        with open(ruta_archivo, mode="a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Encabezado
            if not archivo_existe:
                writer.writerow([
                    "timestamp", "weekday", "ride_id", "ride_name",
                    "status", "wait_time", "evento"
                ])

            total_atracciones = 0

            for ride in atracciones:
                if ride.get("entityType") != "ATTRACTION":
                    continue

                total_atracciones += 1

                ride_id = ride.get("id", "")
                ride_name = ride.get("name", "")
                status = ride.get("status", "")
                wait_time = ride.get("queue", {}).get("STANDBY", {}).get("waitTime", "")

                weekday = ahora.strftime("%A")

                writer.writerow([
                    ahora.isoformat(),
                    weekday,
                    ride_id,
                    ride_name,
                    status,
                    wait_time,
                    evento_activo
                ])

        print(f"  ✅ {nombre}: {total_atracciones} atracciones registradas")
        return total_atracciones, ruta_archivo

    except Exception as e:
        print(f"  ❌ Error al obtener datos de {nombre}: {e}")
        return 0, ruta_archivo
