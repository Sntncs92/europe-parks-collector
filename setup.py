#!/usr/bin/env python3
"""
Script de setup para el colector PostgreSQL
Configura todo automáticamente
"""

import os
import sys
import json
from pathlib import Path

print("\n" + "="*70)
print("🎢 SETUP DEL COLECTOR POSTGRESQL")
print("="*70 + "\n")

# =============================================================================
# VERIFICACIONES PREVIAS
# =============================================================================

print("📋 Verificando estructura del proyecto...\n")

# Verificar carpetas
carpetas_requeridas = ['collectors', 'utils', 'config', 'logs']
carpetas_faltantes = []

for carpeta in carpetas_requeridas:
    if not os.path.exists(carpeta):
        carpetas_faltantes.append(carpeta)
        os.makedirs(carpeta, exist_ok=True)
        print(f"✓ Creada carpeta: {carpeta}/")
    else:
        print(f"✓ Carpeta existe: {carpeta}/")

# Verificar archivos __init__.py
for paquete in ['collectors', 'utils']:
    init_file = os.path.join(paquete, '__init__.py')
    if not os.path.exists(init_file):
        with open(init_file, 'w') as f:
            f.write(f"# {paquete.capitalize()} package\n")
        print(f"✓ Creado: {init_file}")

# =============================================================================
# VERIFICAR ARCHIVOS DEL LEGACY
# =============================================================================

print("\n" + "-"*70)
print("📦 Verificando archivos del proyecto legacy...\n")

archivos_legacy = {
    'collectors/schedule_client.py': False,
    'collectors/live_client.py': False,
    'collectors/data_parser.py': False,
    'utils/config_loader.py': False,
    'utils/event_detector.py': False,
    'utils/logger.py': False,
    'config/parks.json': False,
}

for archivo in archivos_legacy.keys():
    if os.path.exists(archivo):
        archivos_legacy[archivo] = True
        print(f"✓ {archivo}")
    else:
        print(f"✗ FALTA: {archivo}")

archivos_faltantes = [k for k, v in archivos_legacy.items() if not v]

if archivos_faltantes:
    print(f"\n⚠️  Faltan {len(archivos_faltantes)} archivos del legacy")
    print("\nDebes copiarlos desde tu proyecto legacy:")
    print("\nLEGACY_PATH='../TU_PROYECTO_LEGACY'")
    for archivo in archivos_faltantes:
        print(f"cp $LEGACY_PATH/{archivo} {archivo}")
    print("\n❌ Setup incompleto. Copia los archivos y ejecuta de nuevo.\n")
    sys.exit(1)
else:
    print("\n✅ Todos los archivos del legacy están presentes")

# =============================================================================
# VERIFICAR ARCHIVOS NUEVOS
# =============================================================================

print("\n" + "-"*70)
print("🆕 Verificando archivos nuevos...\n")

archivos_nuevos = {
    'collectors/db_writer.py': False,
    'utils/db_config.py': False,
    'main.py': False,
    'requirements.txt': False,
}

for archivo in archivos_nuevos.keys():
    if os.path.exists(archivo):
        archivos_nuevos[archivo] = True
        print(f"✓ {archivo}")
    else:
        print(f"✗ FALTA: {archivo}")

archivos_nuevos_faltantes = [k for k, v in archivos_nuevos.items() if not v]

if archivos_nuevos_faltantes:
    print(f"\n⚠️  Faltan {len(archivos_nuevos_faltantes)} archivos nuevos")
    print("\nAsegúrate de tener todos los archivos del proyecto nuevo.")
    print("\n❌ Setup incompleto.\n")
    sys.exit(1)
else:
    print("\n✅ Todos los archivos nuevos están presentes")

# =============================================================================
# CONFIGURAR DATABASE.JSON
# =============================================================================

print("\n" + "-"*70)
print("🔧 Configuración de base de datos...\n")

db_config_path = Path("config/database.json")

if db_config_path.exists():
    print(f"⚠️  {db_config_path} ya existe")
    respuesta = input("¿Deseas reconfigurar? (s/n): ")
    if respuesta.lower() != 's':
        print("Usando configuración existente...")
        with open(db_config_path, 'r') as f:
            db_config = json.load(f)
    else:
        db_config = None
else:
    db_config = None

if db_config is None:
    print("\nIngresa la configuración de PostgreSQL:")
    print("(Presiona Enter para usar el valor por defecto)\n")
    
    host = input("Host [localhost]: ").strip() or "localhost"
    port = input("Puerto [5432]: ").strip() or "5432"
    database = input("Base de datos [theme_parks]: ").strip() or "theme_parks"
    user = input("Usuario [postgres]: ").strip() or "postgres"
    
    password = ""
    while not password:
        password = input("Contraseña (requerida): ").strip()
        if not password:
            print("⚠️  La contraseña es obligatoria")
    
    db_config = {
        "host": host,
        "port": int(port),
        "database": database,
        "user": user,
        "password": password
    }
    
    with open(db_config_path, 'w') as f:
        json.dump(db_config, f, indent=4)
    
    print(f"\n✅ Configuración guardada en {db_config_path}")

# =============================================================================
# INSTALAR DEPENDENCIAS
# =============================================================================

print("\n" + "-"*70)
print("📦 Instalando dependencias...\n")

respuesta = input("¿Instalar dependencias de requirements.txt? (s/n): ")
if respuesta.lower() == 's':
    import subprocess
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
            check=True
        )
        print("\n✅ Dependencias instaladas")
    except subprocess.CalledProcessError:
        print("\n⚠️  Error instalando dependencias. Instálalas manualmente:")
        print("pip install -r requirements.txt")
else:
    print("\n⚠️  Recuerda instalar las dependencias más tarde:")
    print("pip install -r requirements.txt")

# =============================================================================
# TEST DE CONEXIÓN
# =============================================================================

print("\n" + "-"*70)
print("🔌 Testeando conexión a PostgreSQL...\n")

try:
    sys.path.insert(0, os.getcwd())
    from collectors.db_writer import test_connection
    
    if test_connection(db_config):
        print("✅ Conexión exitosa a PostgreSQL!\n")
    else:
        print("❌ No se pudo conectar")
        print("\nVerifica:")
        print("  1. PostgreSQL está corriendo")
        print(f"  2. La base de datos '{db_config['database']}' existe")
        print("  3. Las tablas están creadas (create_database_schema.sql)")
        print()

except ImportError as e:
    print(f"⚠️  No se pudo testear conexión: {e}")
    print("Verifica que las dependencias estén instaladas\n")
except Exception as e:
    print(f"❌ Error: {e}\n")

# =============================================================================
# RESUMEN FINAL
# =============================================================================

print("="*70)
print("✅ SETUP COMPLETADO")
print("="*70)

print("\n📋 Estado del proyecto:\n")
print(f"✓ Estructura de carpetas: OK")
print(f"✓ Archivos del legacy: {len(archivos_legacy)}/{len(archivos_legacy)}")
print(f"✓ Archivos nuevos: {len(archivos_nuevos)}/{len(archivos_nuevos)}")
print(f"✓ Configuración de BD: OK")

print("\n🚀 Próximos pasos:\n")
print("1. Verifica que PostgreSQL tenga las tablas creadas:")
print("   (Ejecuta create_database_schema.sql en pgAdmin si no lo has hecho)")
print()
print("2. Ejecuta el colector:")
print("   python main.py")
print()
print("3. Monitorea los logs:")
print("   tail -f logs/collector.log")
print()
print("="*70 + "\n")