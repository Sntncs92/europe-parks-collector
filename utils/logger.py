import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "collector.log")

def setup_logger():
    # Crear carpeta logs si no existe
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("ParksCollector")
    logger.setLevel(logging.INFO)

    # Evitar duplicar handlers si se llama varias veces
    if logger.handlers:
        return logger

    # Formato de log
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler archivo con rotación (5 MB por archivo, guarda 3 backups)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    # Handler consola (para seguir viendo info en terminal)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
