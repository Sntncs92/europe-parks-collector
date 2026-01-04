import json
import os
import csv
import requests
from datetime import datetime
import pytz

BASE_URL = "https://api.themeparks.wiki/v1/entity"


# ---------------------------------------------------------------
# Cargar parques desde JSON
# ---------------------------------------------------------------
def cargar_parques(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        parques = json.load(file)
    return parques


# ---------------------------------------------------------------
# Obtener horario de apertura y cierre
# ---------------------------------------------------------------
def obtener_horario(entity_id, fecha_iso):
    """
    Devuelve (apertura, cierre) como datetime con timezone
    o (None, None) si no hay horario válido
    """
    schedule_url = f"{BASE_URL}/{entity_id}/schedule"

    try:
        response = requests.get(schedule_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        horarios = data.get("schedule", [])

        horario_dia = next(
            (
                h for h in horarios
                if h.get("date") == fecha_iso
                and h.get("type") == "OPERATING"
            ),
            None
        )

        if not horario_dia:
            return None, None

        apertura = horario_dia.get("openingTime")
        cierre = horario_dia.get("closingTime")

        if apertura and cierre:
            return (
                datetime.fromisoformat(apertura),
                datetime.fromisoformat(cierre)
            )

    except Exception as e:
        print(f"⚠️ Error obteniendo horario ({entity_id}): {e}")

    return None, None


# ---------------------------------------------------------------
# Detectar evento activo
# ---------------------------------------------------------------
def detectar_evento(parque, fecha):
    """
    Devuelve el nombre del evento activo o ""
    """
    for evento in parque.get("eventos", []):
        desde = datetime.fromisoformat(evento["desde"]).date()
        hasta = datetime.fromisoformat(evento["hasta"]).date()

        if desde <= fecha <= hasta:
            return evento["nombre"]

    return ""


# ---------------------------------------------------------------
# Recoger datos en vivo y guardarlos en CSV
# ---------------------------------------------------------------
def recoger_datos(parque, evento_activo, ahora_local):
    nombre = parque["name"]
    entity_id = parque["entity_id"]
    timezone = parque["timezone"]
    zona = pytz.timezone(timezone)

    live_url = f"{BASE_URL}/{entity_id}/live"

    # Nombre de archivo por parque y día
    nombre_archivo = f"{nombre.replace(' ', '_')}_{ahora_local.date().isoformat()}.csv"
    ruta_archivo = os.path.join("data", nombre_archivo)

    os.makedirs("data", exist_ok=True)
    archivo_existe = os.path.isfile(ruta_archivo)

    try:
        response = requests.get(live_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        atracciones = data.get("liveData", [])
        if not atracciones:
            print(f"⚠️  No se encontraron atracciones en {nombre}")
            return 0, ruta_archivo

        with open(ruta_archivo, mode="a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            if not archivo_existe:
                writer.writerow([
                    "timestamp",
                    "weekday",
                    "ride_id",
                    "ride_name",
                    "status",
                    "wait_time",
                    "evento"
                ])

            total = 0

            for ride in atracciones:
                if ride.get("entityType") != "ATTRACTION":
                    continue

                total += 1

                writer.writerow([
                    ahora_local.isoformat(),
                    ahora_local.strftime("%A"),
                    ride.get("id", ""),
                    ride.get("name", ""),
                    ride.get("status", ""),
                    ride.get("queue", {})
                        .get("STANDBY", {})
                        .get("waitTime", ""),
                    evento_activo
                ])

        print(f"  ✅ {nombre}: {total} atracciones registradas")
        return total, ruta_archivo

    except Exception as e:
        print(f"  ❌ Error al recoger datos de {nombre}: {e}")
        return 0, ruta_archivo
