"""
telegram_sender.py — Envío de reportes diarios por Telegram.

Soporta chat privado y canales (añadir '@' delante del username del canal).
Lee la configuración desde config/telegram.json.
"""

import json
import os
from pathlib import Path

import requests

CONFIG_PATH = Path(__file__).parent.parent / "config" / "telegram.json"
BASE_URL    = "https://api.telegram.org/bot{token}/{method}"


def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def send_report(image_path: str | Path, caption: str) -> bool:
    """
    Envía la imagen del reporte con el caption al chat configurado.

    Args:
        image_path : Ruta al PNG generado
        caption    : Texto del post (hasta 1024 caracteres en Telegram)

    Returns:
        True si el envío fue exitoso, False en caso contrario
    """
    config    = _load_config()
    token     = config["bot_token"]
    chat_id   = config["chat_id"]

    # Telegram limita los captions a 1024 caracteres
    if len(caption) > 1024:
        caption = caption[:1021] + "..."

    url = BASE_URL.format(token=token, method="sendPhoto")

    try:
        with open(image_path, "rb") as img:
            response = requests.post(
                url,
                data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                files={"photo": img},
                timeout=30,
            )

        if response.status_code == 200:
            return True

        # Log del error de la API de Telegram
        error = response.json().get("description", "Error desconocido")
        print(f"[Telegram] Error API: {error}")
        return False

    except Exception as e:
        print(f"[Telegram] Error enviando reporte: {e}")
        return False


def send_message(text: str) -> bool:
    """
    Envía un mensaje de texto simple. Útil para alertas del scheduler.
    """
    config  = _load_config()
    token   = config["bot_token"]
    chat_id = config["chat_id"]

    url = BASE_URL.format(token=token, method="sendMessage")

    try:
        response = requests.post(
            url,
            data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
        return response.status_code == 200
    except Exception as e:
        print(f"[Telegram] Error enviando mensaje: {e}")
        return False
