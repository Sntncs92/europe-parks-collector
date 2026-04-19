"""
Theme Parks Analytics - National Holidays Loader
Crea la tabla `holidays` en PostgreSQL y la puebla con festivos nacionales
para todos los países donde hay parques monitorizados.
"""

import holidays
import psycopg2
from psycopg2.extras import execute_values
from datetime import date

# ─── Configuración de conexión ────────────────────────────────────────────────
DB_CONFIG = {
    "host": "89.167.56.172",
    "port": 5432,
    "database": "theme_parks",
    "user": "postgres",
    "password": input("🔑 PostgreSQL password: "),
}

# ─── Países de los parques + código ISO ───────────────────────────────────────
# Formato: (country_name tal como está en parks, iso_code, subdivisions_override)
COUNTRIES = [
    ("United States",  "US", None),
    ("France",         "FR", None),
    ("Spain",          "ES", None),
    ("Germany",        "DE", None),
    ("Netherlands",    "NL", None),
    ("United Kingdom", "GB", None),
    ("Italy",          "IT", None),
    ("Sweden",         "SE", None),
    ("Japan",          "JP", None),
    ("China",          "CN", None),
    ("Hong Kong",      "HK", None),
    ("Australia",      "AU", None),
    ("Mexico",         "MX", None),
]

# Rango de años: histórico + predicción
YEARS = range(2025, 2028)   # 2025, 2026, 2027


# ─── DDL de la tabla ──────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS national_holidays (
    holiday_id      SERIAL PRIMARY KEY,
    country_code    CHAR(2)      NOT NULL,
    country_name    VARCHAR(100) NOT NULL,
    holiday_date    DATE         NOT NULL,
    holiday_name    VARCHAR(255) NOT NULL,
    created_at      TIMESTAMP    DEFAULT NOW(),
    UNIQUE (country_code, holiday_date, holiday_name)
);

CREATE INDEX IF NOT EXISTS idx_holidays_date
    ON national_holidays (holiday_date);

CREATE INDEX IF NOT EXISTS idx_holidays_country_date
    ON national_holidays (country_code, holiday_date);
"""

# ─── Vista útil para joins con wait_times ────────────────────────────────────
CREATE_VIEW_SQL = """
CREATE OR REPLACE VIEW wait_times_with_holidays AS
SELECT
    wt.measurement_id,
    p.park_name,
    p.country,
    p.continent,
    r.ride_name,
    wt.timestamp,
    wt.weekday,
    wt.status,
    wt.wait_time,
    wt.evento,
    CASE WHEN nh.holiday_date IS NOT NULL THEN 1 ELSE 0 END AS is_holiday,
    nh.holiday_name
FROM wait_times wt
JOIN rides r       ON wt.ride_id  = r.ride_id
JOIN parks p       ON r.park_id   = p.park_id
LEFT JOIN national_holidays nh
    ON  nh.holiday_date  = wt.timestamp::date
    AND nh.country_name  = p.country;
"""

# ─── Función principal ────────────────────────────────────────────────────────

def generate_holidays():
    """Genera festivos NACIONALES (PUBLIC) filtrando días de la semana ordinarios.

    Algunas legislaciones (ej. Suecia) incluyen todos los domingos en la
    categoría PUBLIC. Los excluimos porque day_of_week ya es un feature
    del modelo y añadirían ruido, no señal.
    """
    # Nombres que son simplemente días de la semana, no festivos reales
    WEEKDAY_NAMES = {"Söndag", "Sunday", "Sonntag", "Dimanche", "Domingo"}

    rows = []
    for country_name, iso, _ in COUNTRIES:
        for year in YEARS:
            try:
                h = holidays.country_holidays(
                    iso,
                    years=year,
                    categories=(holidays.PUBLIC,),   # solo festivos nacionales
                )
                for hdate, hname in h.items():
                    # Un festivo puede tener nombre compuesto "Påskdagen; Söndag"
                    parts = {p.strip() for p in hname.split(";")}
                    real_parts = parts - WEEKDAY_NAMES
                    if not real_parts:
                        continue   # era solo un día de la semana ordinario
                    clean_name = "; ".join(sorted(real_parts))
                    rows.append((iso, country_name, hdate, clean_name))
            except Exception as e:
                print(f"  ⚠️  {country_name} {year}: {e}")
    return rows


def main():
    print("\n🎢  Theme Parks Analytics — Holiday Loader")
    print("=" * 50)

    # Generar festivos
    print(f"\n📅 Generando festivos para {len(COUNTRIES)} países × {len(list(YEARS))} años…")
    rows = generate_holidays()
    print(f"   → {len(rows):,} festivos generados")

    # Conectar a PostgreSQL
    print("\n🐘 Conectando a PostgreSQL…")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    print("   → Conexión OK")

    try:
        # Crear tabla e índices
        print("\n🏗️  Creando tabla national_holidays e índices…")
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("   → Tabla creada / ya existía ✓")

        # Insertar festivos (ON CONFLICT DO NOTHING para idempotencia)
        print(f"\n📥 Insertando {len(rows):,} festivos (upsert)…")
        execute_values(
            cur,
            """
            INSERT INTO national_holidays
                (country_code, country_name, holiday_date, holiday_name)
            VALUES %s
            ON CONFLICT (country_code, holiday_date, holiday_name) DO NOTHING
            """,
            rows,
            page_size=1000,
        )
        inserted = cur.rowcount
        conn.commit()
        print(f"   → {inserted:,} filas insertadas (duplicados ignorados) ✓")

        # Crear vista
        print("\n🔭 Creando vista wait_times_with_holidays…")
        cur.execute(CREATE_VIEW_SQL)
        conn.commit()
        print("   → Vista creada ✓")

        # Resumen por país
        print("\n📊 Resumen por país:")
        cur.execute("""
            SELECT country_name, country_code,
                   COUNT(*) as total,
                   MIN(holiday_date) as desde,
                   MAX(holiday_date) as hasta
            FROM national_holidays
            GROUP BY country_name, country_code
            ORDER BY country_name;
        """)
        rows_summary = cur.fetchall()
        print(f"  {'País':<22} {'ISO':<5} {'Festivos':>8}  {'Desde':<12} {'Hasta'}")
        print("  " + "-" * 60)
        for r in rows_summary:
            print(f"  {r[0]:<22} {r[1]:<5} {r[2]:>8}  {r[3]}  {r[4]}")

        # Ejemplo de uso para ML
        print("\n💡 Ejemplo de consulta ML-ready:")
        cur.execute("""
            SELECT holiday_date, holiday_name
            FROM national_holidays
            WHERE country_code = 'US'
              AND holiday_date BETWEEN '2026-01-01' AND '2026-12-31'
            ORDER BY holiday_date
            LIMIT 5;
        """)
        examples = cur.fetchall()
        for d, name in examples:
            print(f"   {d}  →  {name}")

        print("\n✅ ¡Todo listo!")
        print("\n📋 Queries útiles para el modelo ML:")
        print("""
  -- Añadir is_holiday como feature:
  SELECT wt.*, 
         COALESCE(nh.holiday_date IS NOT NULL, false)::int AS is_holiday,
         nh.holiday_name
  FROM wait_times wt
  JOIN rides r ON wt.ride_id = r.ride_id
  JOIN parks p ON r.park_id  = p.park_id
  LEFT JOIN national_holidays nh
      ON nh.holiday_date = wt.timestamp::date
     AND nh.country_name = p.country
  LIMIT 5;

  -- O directamente con la vista:
  SELECT * FROM wait_times_with_holidays LIMIT 5;
        """)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
