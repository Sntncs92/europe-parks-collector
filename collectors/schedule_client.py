import requests
from datetime import datetime
from utils.logger import setup_logger
logger = setup_logger()

BASE_URL = "https://api.themeparks.wiki/v1/entity"

def obtener_horario(entity_id, fecha_iso):
    """
    Devuelve (apertura, cierre) como datetime con la zona horaria
    o (None, None) si no hay horario válido (normalmente el parque no abre)
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
        logger.info(f"⚠️ Error obteniendo horario ({entity_id}): {e}")

    return None, None