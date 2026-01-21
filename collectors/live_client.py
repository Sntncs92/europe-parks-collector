import requests
from utils.logger import setup_logger
logger = setup_logger()

BASE_URL = "https://api.themeparks.wiki/v1/entity"

def get_live_data(entity_id):
    live_url = f"{BASE_URL}/{entity_id}/live"

    try:
        response = requests.get(live_url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.info(f"  ❌ Error al llamar live API ({entity_id}): {e}")
        return None