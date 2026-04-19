"""
Colector de datos de parques de atracciones - PostgreSQL Version
Escribe directamente a PostgreSQL en lugar de CSVs
"""

import os
import time
from datetime import datetime
import pytz
from utils.logger import setup_logger
from utils.config_loader import cargar_parques
from utils.event_detector import detectar_evento
from utils.db_config import get_db_config

from collectors.schedule_client import obtener_horario
from collectors.live_client import get_live_data
from collectors.data_parser import parse_live_data
from collectors.db_writer import DatabaseWriter

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

CONFIG_PATH = os.path.join("config", "parks.json")
INTERVALO_SEGUNDOS = 15 * 60  # 15 minutos entre recolecciones

# Opcional: Habilitar backup en CSV (cambiar a True si quieres backup)
ENABLE_CSV_BACKUP = False

logger = setup_logger()

# =============================================================================
# INICIALIZACIÓN
# =============================================================================

print("\n" + "="*70)
print("🎢 COLECTOR DE DATOS DE PARQUES - POSTGRESQL VERSION")
print("="*70 + "\n")

# Cargar configuración de parques
try:
    parques = cargar_parques(CONFIG_PATH)
    logger.info(f"✓ Cargados {len(parques)} parques")
except Exception as e:
    logger.error(f"✗ Error cargando configuración de parques: {e}")
    exit(1)

# Cargar configuración de base de datos
try:
    db_config = get_db_config()
    logger.info("✓ Configuración de BD cargada")
except Exception as e:
    logger.error(f"✗ Error cargando configuración de BD: {e}")
    logger.error("Verifica que existe config/database.json")
    exit(1)

# Inicializar DatabaseWriter
try:
    db_writer = DatabaseWriter(db_config)
    logger.info("✓ DatabaseWriter inicializado")
except Exception as e:
    logger.error(f"✗ Error inicializando DatabaseWriter: {e}")
    logger.error("Verifica tu configuración en config/database.json")
    logger.error("Verifica que PostgreSQL está corriendo")
    logger.error("Verifica que las tablas están creadas")
    exit(1)

# Importar csv_writer si backup está habilitado
if ENABLE_CSV_BACKUP:
    try:
        from collectors.csv_writer import save_to_csv
        logger.info("✓ Backup CSV habilitado")
    except ImportError:
        logger.warning("⚠️  csv_writer no encontrado, backup CSV deshabilitado")
        ENABLE_CSV_BACKUP = False

print("="*70)
print("CONFIGURACIÓN:")
print(f"  • Parques monitorizados: {len(parques)}")
print(f"  • Intervalo: {INTERVALO_SEGUNDOS/60:.0f} minutos")
print(f"  • Backup CSV: {'Sí' if ENABLE_CSV_BACKUP else 'No'}")
print(f"  • Base de datos: {db_config['database']}")
print("="*70 + "\n")

# =============================================================================
# CACHE Y RESUMEN
# =============================================================================

# Cache de horarios para evitar consultas repetidas
horarios_cache = {
    parque["name"]: {
        "fecha": None,
        "apertura": None,
        "cierre": None,
        "last_collected": None
    } for parque in parques
}

# Resumen de recolección
resumen_parques = {
    parque["name"]: {
        "registros_db": 0,
        "registros_csv": 0,
        "ultimo_guardado": None,
        "ultimos_errores": []
    } for parque in parques
}

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def mostrar_resumen():
    """Muestra resumen de recolección"""
    logger.info("\n" + "="*70)
    logger.info("RESUMEN DE RECOLECCIÓN")
    logger.info("="*70)
    
    total_db = 0
    total_csv = 0
    parques_activos = 0
    
    for nombre, stats in resumen_parques.items():
        if stats["registros_db"] > 0:
            parques_activos += 1
            msg = f"{nombre}: {stats['registros_db']:,} registros en PostgreSQL"
            if ENABLE_CSV_BACKUP and stats["registros_csv"] > 0:
                msg += f", {stats['registros_csv']:,} en CSV"
            if stats["ultimo_guardado"]:
                msg += f" (último: {stats['ultimo_guardado']})"
            logger.info(msg)
            total_db += stats["registros_db"]
            total_csv += stats["registros_csv"]
    
    logger.info("-"*70)
    logger.info(f"Parques activos: {parques_activos}/{len(parques)}")
    logger.info(f"Total PostgreSQL: {total_db:,} registros")
    if ENABLE_CSV_BACKUP:
        logger.info(f"Total CSV: {total_csv:,} registros")
    logger.info("="*70 + "\n")

# =============================================================================
# BUCLE PRINCIPAL
# =============================================================================

logger.info("🚀 Iniciando bucle de recolección...\n")
ultimo_resumen = time.time()

try:
    while True:
        for parque in parques:
            nombre = parque["name"]
            zona = pytz.timezone(parque["timezone"])
            ahora_local = datetime.now(zona)

            cache = horarios_cache[nombre]

            # Actualizar horarios si cambia el día
            if cache["fecha"] != ahora_local.date():
                try:
                    apertura, cierre = obtener_horario(
                        parque["entity_id"], 
                        ahora_local.date().isoformat()
                    )
                    cache.update({
                        "fecha": ahora_local.date(),
                        "apertura": apertura,
                        "cierre": cierre,
                        "last_collected": None,
                        "report_queued": False
                    })
                    
                    if apertura and cierre:
                        logger.info(
                            f"📅 {nombre}: Horario actualizado "
                            f"({apertura.strftime('%H:%M')} - {cierre.strftime('%H:%M')})"
                        )
                        db_writer.save_schedule(
                          nombre, ahora_local.date(), apertura, cierre
                        )
                except Exception as e:
                    logger.error(f"Error obteniendo horario de {nombre}: {e}")
                    continue

            # Saltar si no hay horarios
            if not cache["apertura"] or not cache["cierre"]:
                continue

            # Antes de apertura
            if ahora_local < cache["apertura"]:
                continue

            # Después de cierre
            if ahora_local >= cache["cierre"]:
              if not cache.get("report_queued"):
                db_writer.enqueue_report(
                    nombre, ahora_local.date(), cache["cierre"]
                )
                cache["report_queued"] = True 
                logger.info(f"📋 {nombre}: Reporte encolado para las "
                    f"{cache['cierre'].strftime('%H:%M %Z')}")  
                continue

            # Control de intervalo
            ultima = cache["last_collected"]
            if ultima and (ahora_local - ultima).total_seconds() < INTERVALO_SEGUNDOS:
                continue

            # =================================================================
            # RECOLECCIÓN DE DATOS
            # =================================================================
            
            try:
                # Detectar evento
                evento_activo = detectar_evento(parque, ahora_local.date())

                # Llamar API live
                raw_data = get_live_data(parque["entity_id"])

                # Parsear datos
                filas = parse_live_data(raw_data, evento_activo, ahora_local)

                if not filas:
                    logger.warning(f"⚠️  {nombre}: No hay datos para guardar")
                    continue

                # =============================================================
                # GUARDAR EN POSTGRESQL
                # =============================================================
                
                continent = parque.get("continent")
                country = parque.get("country")
                
                try:
                    nuevos_db, descripcion = db_writer.save_data(
                        nombre, 
                        filas, 
                        country, 
                        continent
                    )
                    
                    resumen_parques[nombre]["registros_db"] += nuevos_db
                    resumen_parques[nombre]["ultimo_guardado"] = ahora_local.strftime("%H:%M")
                    
                    logger.info(f"✅ {nombre}: {nuevos_db} registros → PostgreSQL")
                    
                except Exception as e:
                    logger.error(f"✗ Error guardando {nombre} en PostgreSQL: {e}")
                    resumen_parques[nombre]["ultimos_errores"].append(str(e)[:100])
                    
                    # Intentar reconectar
                    try:
                        logger.info("Intentando reconectar a PostgreSQL...")
                        db_writer._reconnect()
                        logger.info("✓ Reconexión exitosa")
                    except Exception as reconnect_error:
                        logger.error(f"✗ No se pudo reconectar: {reconnect_error}")

                # =============================================================
                # OPCIONAL: BACKUP EN CSV
                # =============================================================
                
                if ENABLE_CSV_BACKUP:
                    try:
                        nuevos_csv, archivo = save_to_csv(
                            nombre, 
                            filas, 
                            ahora_local, 
                            continent, 
                            country
                        )
                        resumen_parques[nombre]["registros_csv"] += nuevos_csv
                        logger.info(f"💾 {nombre}: Backup CSV ({nuevos_csv} registros)")
                    except Exception as e:
                        logger.error(f"✗ Error guardando backup CSV para {nombre}: {e}")

                cache["last_collected"] = ahora_local

            except Exception as e:
                logger.error(f"✗ Error procesando {nombre}: {e}")
                resumen_parques[nombre]["ultimos_errores"].append(str(e)[:100])

        # Mostrar resumen cada hora
        if time.time() - ultimo_resumen > 3600:
            mostrar_resumen()
            ultimo_resumen = time.time()

        # Espera antes de siguiente iteración
        time.sleep(600)  # 10 minutos de espera entre ciclos

except KeyboardInterrupt:
    logger.info("\n\n⚠️  Deteniendo colector (Ctrl+C detectado)...")
    
except Exception as e:
    logger.error(f"\n\n✗ Error fatal en el colector: {e}")
    import traceback
    logger.error(traceback.format_exc())
    
finally:
    # Cerrar conexión a la base de datos
    logger.info("\nCerrando conexión a PostgreSQL...")
    db_writer.close()
    
    # Mostrar resumen final
    logger.info("\n" + "="*70)
    logger.info("RESUMEN FINAL")
    logger.info("="*70)
    
    total_db = 0
    total_csv = 0
    
    for nombre, stats in resumen_parques.items():
        if stats["registros_db"] > 0:
            logger.info(f"\n{nombre}:")
            logger.info(f"  PostgreSQL: {stats['registros_db']:,} registros")
            if ENABLE_CSV_BACKUP and stats["registros_csv"] > 0:
                logger.info(f"  CSV Backup: {stats['registros_csv']:,} registros")
            
            if stats["ultimos_errores"]:
                logger.info(f"  Errores: {len(stats['ultimos_errores'])}")
                for i, error in enumerate(stats["ultimos_errores"][-3:], 1):
                    logger.info(f"    {i}. {error}")
            
            total_db += stats["registros_db"]
            total_csv += stats["registros_csv"]
    
    logger.info("\n" + "-"*70)
    logger.info(f"TOTAL PostgreSQL: {total_db:,} registros")
    if ENABLE_CSV_BACKUP:
        logger.info(f"TOTAL CSV Backup: {total_csv:,} registros")
    logger.info("="*70)
    logger.info("\n✅ Colector finalizado correctamente\n")