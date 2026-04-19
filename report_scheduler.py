"""
report_scheduler.py — Scheduler de reportes diarios de parques temáticos.

Proceso independiente que corre 24/7 como servicio systemd.
Cada 5 minutos revisa report_queue y genera los reportes pendientes
una vez transcurrida la ventana de gracia tras el cierre del parque.

Uso:
    python report_scheduler.py

Variables de entorno necesarias (heredadas del .env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    REPORT_OUTPUT_DIR  (opcional, default: /opt/theme_parks_collector/reports)
    GRACE_MINUTES      (opcional, default: 30)
    MIN_COLLECTIONS    (opcional, default: 6)
"""

import os
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from utils.db_config import get_db_config

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

DB_CONFIG = get_db_config()

OUTPUT_DIR      = Path(os.getenv("REPORT_OUTPUT_DIR",
                                  "/opt/theme_parks_collector/reports"))
GRACE_MINUTES   = int(os.getenv("GRACE_MINUTES", 30))
MIN_COLLECTIONS = int(os.getenv("MIN_COLLECTIONS", 6))
POLL_SECONDS    = 300   # cada 5 minutos

# =============================================================================
# LOGGING
# =============================================================================

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scheduler")

# =============================================================================
# BASE DE DATOS
# =============================================================================

def get_conn():
    """Abre una conexión nueva a PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


def fetch_pending_reports(conn) -> list[dict]:
    """
    Devuelve los reportes pendientes cuya ventana de gracia ya ha pasado.
    Una entrada es elegible cuando:
        closing_time + GRACE_MINUTES <= ahora (UTC)
    """
    sql = """
        SELECT
            rq.id,
            rq.park_id,
            p.park_name,
            rq.report_date,
            rq.closing_time
        FROM report_queue rq
        JOIN parks p ON p.park_id = rq.park_id
        WHERE rq.status = 'pending'
          AND rq.closing_time + %s * INTERVAL '1 minute' <= NOW()
        ORDER BY rq.closing_time ASC;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (GRACE_MINUTES,))
        return cur.fetchall()


def mark_status(conn, report_id: int, status: str, error_msg: str = None):
    """Actualiza el status de una entrada en report_queue."""
    sql = """
        UPDATE report_queue
        SET status       = %s,
            triggered_at = CASE WHEN %s = 'processing' THEN NOW() ELSE triggered_at END,
            completed_at = CASE WHEN %s IN ('done', 'skipped', 'error') THEN NOW() ELSE NULL END,
            error_msg    = %s
        WHERE id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (status, status, status, error_msg, report_id))
    conn.commit()


def get_park_stats(conn, park_id: int, report_date, closing_time) -> dict:
    """
    Calcula estadísticas básicas del parque para el día del reporte.
    Usa closing_time como límite superior para no incluir datos del día siguiente.
    También recupera la opening_time de park_schedules para usarla como límite inferior.
    """
    sql = """
        SELECT
            COUNT(*)                                                    AS total_measurements,
            COUNT(DISTINCT r.ride_id)                                   AS num_rides,
            COUNT(*) FILTER (WHERE wt.status = 'OPERATING')            AS operating_count,
            AVG(wt.wait_time) FILTER (WHERE wt.status = 'OPERATING')   AS avg_wait,
            MAX(wt.wait_time)                                           AS max_wait,
            (
                SELECT opening_time
                FROM park_schedules
                WHERE park_id = %s AND date = %s
            )                                                           AS opening_time
        FROM wait_times wt
        JOIN rides r ON r.ride_id = wt.ride_id
        WHERE r.park_id = %s
          AND wt.timestamp >= (
              SELECT opening_time FROM park_schedules
              WHERE park_id = %s AND date = %s
          )
          AND wt.timestamp <= %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (
            park_id, report_date,   # subquery opening_time en SELECT
            park_id,                # WHERE r.park_id
            park_id, report_date,   # subquery opening_time en WHERE
            closing_time,           # límite superior
        ))
        return dict(cur.fetchone())


def has_enough_data(stats: dict, min_collections: int = MIN_COLLECTIONS) -> bool:
    """
    Verifica si hay suficientes datos para generar un reporte útil.
    Umbral: al menos min_collections mediciones por atracción.
    """
    num_rides = stats.get("num_rides") or 0
    total     = stats.get("total_measurements") or 0

    if num_rides == 0:
        return False

    return total >= (num_rides * min_collections)

# =============================================================================
# GENERACIÓN DEL REPORTE
# =============================================================================

def generate_report(park_name: str, report_date, closing_time,
                    output_dir: Path, park_id: int, conn):
    from report_generator import generate_park_report
    from utils.telegram_sender import send_report

    png_path, caption_es, caption_en = generate_park_report(
        park_name   = park_name,
        report_date = report_date,
        park_id     = park_id,
        conn        = conn,
    )

    ok_es = send_report(png_path, caption_es)
    if not ok_es:
        log.warning(f"  ⚠️  Telegram ES falló para {park_name}")

    ok_en = send_report(png_path, caption_en)
    if not ok_en:
        log.warning(f"  ⚠️  Telegram EN falló para {park_name}")

    return png_path

# =============================================================================
# BUCLE PRINCIPAL
# =============================================================================

def run():
    log.info("=" * 60)
    log.info("SCHEDULER DE REPORTES — Theme Parks Analytics")
    log.info(f"  Ventana de gracia : {GRACE_MINUTES} min")
    log.info(f"  Min colecciones   : {MIN_COLLECTIONS} por atracción")
    log.info(f"  Directorio salida : {OUTPUT_DIR}")
    log.info(f"  Intervalo polling : {POLL_SECONDS // 60} min")
    log.info("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            conn = get_conn()
            try:
                pending = fetch_pending_reports(conn)

                if pending:
                    log.info(f"{len(pending)} reporte(s) listo(s) para procesar")

                for row in pending:
                    report_id  = row["id"]
                    park_name  = row["park_name"]
                    park_id    = row["park_id"]
                    report_date = row["report_date"]
                    closing_time = row["closing_time"]

                    log.info(f"▶ Procesando: {park_name} ({report_date})")
                    mark_status(conn, report_id, "processing")

                    try:
                        # Verificar datos suficientes
                        stats = get_park_stats(
                            conn, park_id, report_date, closing_time
                        )

                        if not has_enough_data(stats):
                            rides = stats.get("num_rides", 0)
                            total = stats.get("total_measurements", 0)
                            reason = (
                                f"Datos insuficientes: {total} mediciones "
                                f"para {rides} atracciones "
                                f"(mínimo {rides * MIN_COLLECTIONS})"
                            )
                            log.warning(f"  ⏭ Skipped — {reason}")
                            mark_status(conn, report_id, "skipped", reason)
                            continue

                        log.info(
                            f"  ✓ Datos OK: {stats['total_measurements']} mediciones, "
                            f"{stats['num_rides']} atracciones, "
                            f"espera media {stats['avg_wait']:.1f} min"
                        )

                        # Generar reporte
                        out_path = generate_report(
                          park_name, report_date, closing_time, OUTPUT_DIR, park_id, conn
                        )
                        log.info(f"  ✅ Reporte generado: {out_path}")
                        mark_status(conn, report_id, "done")

                    except Exception as e:
                        err = str(e)[:400]
                        log.error(f"  ✗ Error procesando {park_name}: {err}")
                        mark_status(conn, report_id, "error", err)

            finally:
                conn.close()

        except Exception as e:
            log.error(f"Error de conexión en el scheduler: {e}")
            log.error(traceback.format_exc())

        log.info(f"Siguiente revisión en {POLL_SECONDS // 60} minutos...")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run()
