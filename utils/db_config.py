"""
Configuración de la base de datos PostgreSQL
"""

import os
import json

# Ruta al archivo de configuración
DB_CONFIG_PATH = os.path.join("config", "database.json")


def get_db_config():
    """
    Carga la configuración de la base de datos
    
    Returns:
        dict con keys: host, port, database, user, password
    """
    # Intentar cargar desde archivo JSON
    if os.path.exists(DB_CONFIG_PATH):
        with open(DB_CONFIG_PATH, 'r') as f:
            config = json.load(f)
            return config
    
    # Si no existe el archivo, intentar variables de entorno
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'theme_parks'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    # Si no hay password, lanzar error
    if not config['password']:
        raise ValueError(
            "No se encontró configuración de base de datos. "
            "Crea el archivo config/database.json con las credenciales."
        )
    
    return config


def create_default_config():
    """
    Crea un archivo de configuración por defecto
    Ejecuta esta función una vez para crear el archivo database.json
    """
    default_config = {
        "host": "localhost",
        "port": 5432,
        "database": "theme_parks",
        "user": "postgres",
        "password": "1234Asdf"
    }
    
    # Crear carpeta config si no existe
    os.makedirs("config", exist_ok=True)
    
    # Guardar configuración
    with open(DB_CONFIG_PATH, 'w') as f:
        json.dump(default_config, f, indent=4)
    
    print(f"✓ Archivo de configuración creado: {DB_CONFIG_PATH}")
    print("⚠️  IMPORTANTE: Edita este archivo y pon tu contraseña de PostgreSQL")


if __name__ == "__main__":
    # Ejecutar este archivo directamente para crear el config por defecto
    create_default_config()