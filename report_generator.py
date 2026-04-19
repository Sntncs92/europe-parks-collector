"""
report_generator.py — Generador de imágenes PNG para reportes diarios.

Produce una imagen 1080x1350 con:
  - Header con logo del parque y colores del operador
  - KPI strip (hora pico, espera pico, mejor hora, % operativas)
  - Top 3 por tier en tres columnas
  - Gráfico horario de la atracción estrella (Tier 1 con más espera)

Uso desde report_scheduler.py:
    from report_generator import generate_park_report
    png_path, caption = generate_park_report(park_name, report_date, conn)
"""

from __future__ import annotations

import os
import textwrap
from datetime import date, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import matplotlib.image as mpimg
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import json

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

LOGOS_DIR  = Path(__file__).parent / "assets" / "logos"
OUTPUT_DIR = Path(os.getenv("REPORT_OUTPUT_DIR",
                             "/opt/theme_parks_collector/reports"))

IMG_W, IMG_H = 1080, 1350
DPI          = 150

# Colores por operador
OPERATOR_THEMES = {
    "disney":     {"bg1": "#0d2b6b", "bg2": "#2160c4", "accent": "#c8a951", "kpi_bg": "#0f2060"},
    "universal":  {"bg1": "#001a3d", "bg2": "#0044b8", "accent": "#f9a825", "kpi_bg": "#001535"},
    "six_flags":  {"bg1": "#7a0000", "bg2": "#d62828", "accent": "#f7b731", "kpi_bg": "#5a0000"},
    "merlin":     {"bg1": "#1a1a2e", "bg2": "#3a2060", "accent": "#f4a261", "kpi_bg": "#12122a"},
    "europa":     {"bg1": "#1a2a1a", "bg2": "#3a6b3a", "accent": "#f4a261", "kpi_bg": "#121e12"},
    "default":    {"bg1": "#1a1a2e", "bg2": "#2d2d5a", "accent": "#e94560", "kpi_bg": "#12122a"},
}

TIER_COLORS = {
    1: {"bar": "#c8a951", "text": "#c8a951", "dot": "#c8a951", "label": "Tier 1 · Estrella"},
    2: {"bar": "#4a90d9", "text": "#4a90d9", "dot": "#4a90d9", "label": "Tier 2 · Popular"},
    3: {"bar": "#5aab6a", "text": "#5aab6a", "dot": "#5aab6a", "label": "Tier 3 · Familiar"},
}

MESES_ES = ["enero","febrero","marzo","abril","mayo","junio",
            "julio","agosto","septiembre","octubre","noviembre","diciembre"]
DIAS_ES  = ["lunes","martes","miércoles","jueves","viernes","sábado","domingo"]

def _fecha_es(d: date) -> str:
    dia  = DIAS_ES[d.weekday()]
    mes  = MESES_ES[d.month - 1]
    return f"{dia}, {d.day} de {mes} de {d.year}"
    
def _get_park_timezone(park_name: str) -> str:
    """Lee el timezone del parque desde parks.json."""
    config_path = Path(__file__).parent / "config" / "parks.json"
    with open(config_path, encoding="utf-8") as f:
        parks = json.load(f)
    for park in parks:
        if park["name"] == park_name:
            return park.get("timezone", "UTC")
    return "UTC"

# Mapeo park_name → operador
def _get_operator(park_name: str) -> str:
    n = park_name.lower()
    if any(x in n for x in ["disney", "epcot", "animal kingdom", "hollywood studios",
                              "magic kingdom", "disneyland", "tokyo disney", "shanghai",
                              "hong kong disney"]):
        return "disney"
    if any(x in n for x in ["universal", "epic universe", "islands of adventure"]):
        return "universal"
    if "six flags" in n:
        return "six_flags"
    if any(x in n for x in ["alton towers", "gardaland", "thorpe", "chessington",
                              "legoland", "madame tussauds"]):
        return "merlin"
    if any(x in n for x in ["europa", "efteling", "phantasialand", "portaventura",
                              "parc asterix", "liseberg", "ferrari", "warner madrid",
                              "walibi", "wallibi", "parque warner"]):
        return "europa"
    return "default"

# Mapeo park_name → nombre de archivo de logo
def _logo_path(park_name: str) -> Path | None:
    mapping = {
        "Magic Kingdom":                    "Magic_Kingdom.png",
        "EPCOT":                            "EPCOT.png",
        "Disney's Hollywood Studios":       "Disneys_Hollywood_Studios.png",
        "Animal Kingdom":                   "Animal_Kingdom.png",
        "Disneyland Park":                  "Disneyland_Park.png",
        "Disney California Adventure Park": "Disney_California_Adventure_Park.png",
        "Disneyland Paris":                 "Disneyland_Paris.png",
        "Disney Adventure World":           "Disney_Adventure_World.png",
        "Tokyo DisneyLand":                 "Tokyo_DisneyLand.png",
        "Tokyo Disney Sea":                 "Tokyo_Disney_Sea.png",
        "Shanghai Disneyland":              "Shanghai_Disneyland.png",
        "Hong Kong Disneyland Park":        "Hong_Kong_Disneyland_Park.png",
        "Universal Studios Florida":        "Universal_Studios_Florida.png",
        "Universal Islands of Adventure":   "Universal_Islands_of_Adventure.png",
        "Universal's Epic Universe":        "Universals_Epic_Universe.png",
        "Universal Studios":                "Universal_Studios.png",
        "Six Flags Magic Mountain":         "Six_Flags_Magic_Mountain.png",
        "Six Flags Great Adventure":        "Six_Flags_Great_Adventure.png",
        "Six Flags Over Texas":             "Six_Flags_Over_Texas.png",
        "Six Flags Mexico":                 "Six_Flags_Mexico.png",
        "Alton Towers":                     "Alton_Towers.png",
        "Efteling":                         "Efteling.png",
        "Europa Park":                      "Europa_Park.png",
        "Gardaland":                        "Gardaland.png",
        "Liseberg":                         "Liseberg.png",
        "Parc Asterix":                     "Parc_Asterix.png",
        "Parque Warner Madrid":             "Parque_Warner_Madrid.png",
        "PortAventura":                     "PortAventura.png",
        "Ferarri Land España":              "Ferrari_Land_Espana.png",
        "Phantasialand":                    "Phantasialand.png",
        "Walibi Belgium":                   "Walibi_Belgium.png",
        "Wallibi Holland":                  "Wallibi_Holland.png",
        "Dollywood":                        "Dollywood.png",
        "Hersheypark":                      "Hersheypark.png",
        "Knott's Berry Farm":               "Knotts_Berry_Farm.png",
        "SeaWorld Orlando":                 "SeaWorld_Orlando.png",
        "Busch Gardens Tampa":              "Busch_Gardens_Tampa.png",
        "Warner Bros Movie World":          "Warner_Bros_Movie_World.png",
        "Six Flags Qiddiya City":           "Six_Flags_Qiddiya_City.png",
        "Cedar Point":                      "Cedar_Point.png",
    }
    filename = mapping.get(park_name)
    if not filename:
        return None
    p = LOGOS_DIR / filename
    return p if p.exists() else None


# =============================================================================
# QUERIES
# =============================================================================

def _fetch_top_by_tier(conn, park_id: int, opening_time, closing_time,
                        n: int = 3) -> dict[int, pd.DataFrame]:
    """Top N rides por tier para el día del reporte."""
    sql = """
        SELECT
            r.ride_name,
            r.tier,
            ROUND(AVG(wt.wait_time)::numeric, 1)::float AS avg_wait
        FROM wait_times wt
        JOIN rides r ON r.ride_id = wt.ride_id
        WHERE r.park_id    = %s
          AND r.tier       IS NOT NULL
          AND wt.status    = 'OPERATING'
          AND wt.wait_time IS NOT NULL
          AND wt.timestamp BETWEEN %s AND %s
        GROUP BY r.ride_name, r.tier
        HAVING COUNT(*) >= 3
        ORDER BY r.tier, avg_wait DESC;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (park_id, opening_time, closing_time))
        rows = cur.fetchall()

    result = {}
    for tier in [1, 2, 3]:
        tier_rows = [r for r in rows if r["tier"] == tier][:n]
        result[tier] = pd.DataFrame(tier_rows) if tier_rows else pd.DataFrame()
    return result


def _fetch_hourly(conn, park_id: int, star_ride: str,
                   opening_time, closing_time, timezone: str) -> pd.DataFrame:
    """Evolución horaria de la atracción estrella."""
    sql = """
        SELECT
            EXTRACT(HOUR FROM wt.timestamp AT TIME ZONE %s)::int AS hour,
            ROUND(AVG(wt.wait_time)::numeric, 1)::float AS avg_wait
        FROM wait_times wt
        JOIN rides r ON r.ride_id = wt.ride_id
        WHERE r.park_id    = %s
          AND r.ride_name  = %s
          AND wt.status    = 'OPERATING'
          AND wt.wait_time IS NOT NULL
          AND wt.timestamp BETWEEN %s AND %s
        GROUP BY EXTRACT(HOUR FROM wt.timestamp AT TIME ZONE %s)
        ORDER BY hour;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (timezone, park_id, star_ride, opening_time, closing_time, timezone))
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _fetch_kpis(conn, park_id: int, opening_time, closing_time, timezone: str) -> dict:
    """KPIs del día: hora pico, espera pico, mejor hora, % operativas."""
    sql = """
        SELECT
            EXTRACT(HOUR FROM wt.timestamp AT TIME ZONE %s)::int AS hour,
            ROUND(AVG(wt.wait_time) FILTER (WHERE wt.status = 'OPERATING'
                                      AND wt.wait_time IS NOT NULL)::numeric, 1)::float AS avg_wait,
            COUNT(DISTINCT r.ride_id)                      AS total_rides,
            COUNT(DISTINCT r.ride_id) FILTER (WHERE wt.status = 'OPERATING') AS op_rides
        FROM wait_times wt
        JOIN rides r ON r.ride_id = wt.ride_id
        WHERE r.park_id = %s
          AND wt.timestamp BETWEEN %s AND %s
        GROUP BY EXTRACT(HOUR FROM wt.timestamp AT TIME ZONE %s)
        ORDER BY avg_wait DESC NULLS LAST;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (timezone, park_id, opening_time, closing_time, timezone))
        rows = cur.fetchall()

    if not rows:
        return {}

    df       = pd.DataFrame(rows)
    peak_row = df.dropna(subset=["avg_wait"]).iloc[0]
    best_row = df.dropna(subset=["avg_wait"]).iloc[-1]

    return {
        "peak_hour":     int(peak_row["hour"]),
        "peak_wait":     round(float(peak_row["avg_wait"])),
        "best_hour":     int(best_row["hour"]),
        "best_wait":     round(float(best_row["avg_wait"])),
        "pct_operating": round(
            100 * df["op_rides"].max() / df["total_rides"].max()
        ),
    }


def _fetch_opening_closing(conn, park_id: int, report_date: date):
    """Recupera opening_time y closing_time de park_schedules."""
    sql = """
        SELECT opening_time, closing_time
        FROM park_schedules
        WHERE park_id = %s AND date = %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (park_id, report_date))
        row = cur.fetchone()
    return (row["opening_time"], row["closing_time"]) if row else (None, None)


# =============================================================================
# CAPTION
# =============================================================================

def _build_caption(park_name: str, report_date: date,
                    tiers: dict, kpis: dict, lang: str = "es") -> str:
    lines_es, lines_en = [], []

    date_es = _fecha_es(report_date)
    date_en = report_date.strftime("%A, %B %d %Y")

    def tier_block(lang):
        block = []
        labels_es = {1: "Estrella", 2: "Popular", 3: "Familiar"}
        labels_en = {1: "Star rides", 2: "Popular", 3: "Family"}
        for t in [1, 2, 3]:
            df = tiers.get(t)
            if df is None or df.empty:
                continue
            lbl = labels_es[t] if lang == "es" else labels_en[t]
            block.append(f"\n{lbl}:")
            for _, row in df.iterrows():
                block.append(f"  · {row['ride_name'][:32]} — {row['avg_wait']:.0f} min")
        return "\n".join(block)

    caption_es = (
        f"📊 INFORME DIARIO — {park_name}\n"
        f"📅 {date_es}\n"
        f"\n🎢 TOP ATRACCIONES\n{tier_block('es')}\n"
        f"\n⏰ Hora pico: {kpis.get('peak_hour','?')}:00h · {kpis.get('peak_wait','?')} min\n"
        f"🌅 Mejor hora: {kpis.get('best_hour','?')}:00h · {kpis.get('best_wait','?')} min\n"
        f"✅ Atracciones operativas: {kpis.get('pct_operating','?')}%\n"
        f"\n📈 themepark-analytics.io · Datos cada 15 min\n"
        f"#ThemeParks #ParquesTemáticos #WaitTimes"
    )

    caption_en = (
        f"📊 DAILY REPORT — {park_name}\n"
        f"📅 {date_en}\n"
        f"\n🎢 TOP RIDES\n{tier_block('en')}\n"
        f"\n⏰ Peak hour: {kpis.get('peak_hour','?')}:00 · {kpis.get('peak_wait','?')} min\n"
        f"🌅 Best time: {kpis.get('best_hour','?')}:00 · {kpis.get('best_wait','?')} min\n"
        f"✅ Rides operating: {kpis.get('pct_operating','?')}%\n"
        f"\n📈 themepark-analytics.io · Updated every 15 min\n"
        f"#ThemeParks #WaitTimes #ThemeParkData"
    )

    return caption_es, caption_en


# =============================================================================
# RENDER PNG
# =============================================================================

def _render_png(park_name: str, report_date: date,
                tiers: dict, df_hourly: pd.DataFrame,
                kpis: dict, star_ride: str,
                theme: dict, logo_path: Path | None,
                output_path: Path) -> Path:

    fig = plt.figure(figsize=(IMG_W / DPI, IMG_H / DPI),
                     facecolor=theme["bg1"], dpi=DPI)

    gs = gridspec.GridSpec(
        4, 1, figure=fig,
        height_ratios=[0.14, 0.08, 0.38, 0.40],
        hspace=0.0,
        left=0.0, right=1.0, top=1.0, bottom=0.0,
    )

    ax_header = fig.add_subplot(gs[0])
    ax_kpi    = fig.add_subplot(gs[1])
    ax_tiers  = fig.add_subplot(gs[2])
    ax_chart  = fig.add_subplot(gs[3])

    for ax in [ax_header, ax_kpi, ax_tiers, ax_chart]:
        ax.set_xticks([])
        ax.set_yticks([])
        for sp in ax.spines.values():
            sp.set_visible(False)

    # ── HEADER ────────────────────────────────────────────────────────────────
    ax_header.set_facecolor(theme["bg1"])
    _draw_gradient_bg(ax_header, theme["bg1"], theme["bg2"])

    operator_label = _operator_label(park_name)
    date_str = _fecha_es(report_date)

    ax_header.text(0.05, 0.72, operator_label.upper(),
                   transform=ax_header.transAxes,
                   color=theme["accent"], fontsize=9, alpha=0.9,
                   fontweight="bold", va="center")
    fontsize_name = 22 if len(park_name) <= 24 else 16
    ax_header.text(0.05, 0.38, park_name,
                   transform=ax_header.transAxes,
                   color="white", fontsize=fontsize_name, fontweight="bold", va="center")
    ax_header.text(0.05, 0.10, date_str,
                   transform=ax_header.transAxes,
                   color="white", fontsize=10, alpha=0.6, va="center")

    # Logo
    if logo_path:
        try:
            logo_img = mpimg.imread(str(logo_path))
            logo_ax  = fig.add_axes([0.80, 0.87, 0.14, 0.10])
            logo_ax.imshow(logo_img)
            logo_ax.axis("off")
        except Exception:
            pass

    # ── KPI STRIP ─────────────────────────────────────────────────────────────
    ax_kpi.set_facecolor(theme["kpi_bg"])
    kpi_items = [
        (f"{kpis.get('peak_hour','?')}:00h", "Hora pico"),
        (f"{kpis.get('peak_wait','?')} min", "Media en hora pico"),
        (f"{kpis.get('best_hour','?')}:00h", "Mejor hora"),
        (f"{kpis.get('pct_operating','?')}%", "Operativas"),
    ]
    n_kpi = len(kpi_items)
    for i, (val, lbl) in enumerate(kpi_items):
        x = (i + 0.5) / n_kpi
        ax_kpi.text(x, 0.65, val, transform=ax_kpi.transAxes,
                    color=theme["accent"], fontsize=14, fontweight="bold",
                    ha="center", va="center")
        ax_kpi.text(x, 0.18, lbl, transform=ax_kpi.transAxes,
                    color="white", fontsize=8, alpha=0.5,
                    ha="center", va="center")
        if i < n_kpi - 1:
            ax_kpi.axvline((i + 1) / n_kpi, color="white", alpha=0.08, lw=0.5)

    # ── TIERS ─────────────────────────────────────────────────────────────────
    ax_tiers.set_facecolor("#0d1117")

    ax_tiers.text(0.04, 0.95, "TOP ATRACCIONES POR CATEGORÍA",
                  transform=ax_tiers.transAxes,
                  color="white", fontsize=8, alpha=0.4, fontweight="bold",
                  va="top")

    col_w = 1 / 3
    for col_i, tier in enumerate([1, 2, 3]):
        df    = tiers.get(tier, pd.DataFrame())
        tc    = TIER_COLORS[tier]
        x_off = col_i * col_w
        center_x = x_off + col_w / 2

        # Tier label
        ax_tiers.text(center_x, 0.84, tc["label"],
                      transform=ax_tiers.transAxes,
                      color=tc["text"], fontsize=9, fontweight="bold",
                      ha="center", va="center", alpha=0.9)

        # Divider dot
        ax_tiers.plot([x_off + 0.03, x_off + col_w - 0.03], [0.79, 0.79],
                      transform=ax_tiers.transAxes,
                      color=tc["bar"], lw=0.5, alpha=0.3)

        if df.empty:
            ax_tiers.text(center_x, 0.55, "Sin datos",
                          transform=ax_tiers.transAxes,
                          color="white", fontsize=9, alpha=0.3,
                          ha="center", va="center")
            continue

        max_wait = float(df["avg_wait"].max())
        row_positions = [0.68, 0.50, 0.32]

        for row_i, (_, row) in enumerate(df.iterrows()):
            if row_i >= 3:
                break
            y_center = row_positions[row_i]
            bar_pct  = row["avg_wait"] / max_wait if max_wait > 0 else 0

            # Ride name
            name_short = textwrap.shorten(row["ride_name"], width=22, placeholder="…")
            ax_tiers.text(x_off + 0.03, y_center + 0.06, name_short,
                          transform=ax_tiers.transAxes,
                          color="white", fontsize=9, alpha=0.9, va="center")

            # Wait time
            ax_tiers.text(x_off + col_w - 0.03,
                          y_center + 0.06,
                          f"{row['avg_wait']:.0f}m",
                          transform=ax_tiers.transAxes,
                          color=tc["text"], fontsize=9, fontweight="bold",
                          ha="right", va="center")

            # Bar background
            bar_x     = x_off + 0.03
            bar_pct   = float(row["avg_wait"]) / float(max_wait) if max_wait > 0 else 0
            bar_width = (col_w - 0.06) * bar_pct
            bar_y     = y_center - 0.03

            bg_rect = FancyBboxPatch((bar_x, bar_y - 0.01),
                                      col_w - 0.06, 0.025,
                                      boxstyle="round,pad=0",
                                      transform=ax_tiers.transAxes,
                                      facecolor="white", alpha=0.07,
                                      linewidth=0)
            ax_tiers.add_patch(bg_rect)

            # Bar fill
            if bar_width > 0:
                fill_rect = FancyBboxPatch((bar_x, bar_y - 0.01),
                                            bar_width, 0.025,
                                            boxstyle="round,pad=0",
                                            transform=ax_tiers.transAxes,
                                            facecolor=tc["bar"], alpha=0.85,
                                            linewidth=0)
                ax_tiers.add_patch(fill_rect)

        # Vertical divider between columns
        if col_i < 2:
            ax_tiers.axvline(x_off + col_w, color="white", alpha=0.06, lw=0.5)

    # ── HOURLY CHART ──────────────────────────────────────────────────────────
    ax_chart.set_facecolor("#0d1117")

    if not df_hourly.empty:
        hours = df_hourly["hour"].values
        waits = df_hourly["avg_wait"].values

        # Inset axes for the actual chart
        chart_inset = fig.add_axes(
            [0.07, 0.06, 0.88, 0.28],
            facecolor="#0d1117"
        )

        chart_inset.fill_between(hours, waits,
                                  color=theme["accent"], alpha=0.12)
        chart_inset.plot(hours, waits,
                         color=theme["accent"], lw=2, zorder=3)
        chart_inset.scatter(hours, waits,
                             color=theme["accent"], s=18, zorder=4)

        # Peak annotation
        peak_idx = int(np.argmax(waits))
        chart_inset.annotate(
            f"{waits[peak_idx]:.0f} min",
            xy=(hours[peak_idx], waits[peak_idx]),
            xytext=(hours[peak_idx], waits[peak_idx] + waits.max() * 0.12),
            color=theme["accent"], fontsize=8, fontweight="bold", ha="center",
            arrowprops=dict(arrowstyle="-", color=theme["accent"], lw=1)
        )

        # Styling
        chart_inset.set_facecolor("#0d1117")
        chart_inset.tick_params(colors="white", labelsize=8)
        for sp in chart_inset.spines.values():
            sp.set_color("#ffffff18")
        chart_inset.grid(axis="y", color="#ffffff0a", lw=0.5)
        chart_inset.set_xticks(hours)
        chart_inset.set_xticklabels([f"{h}h" for h in hours],
                                     color="white", alpha=0.5, fontsize=7)
        chart_inset.tick_params(axis="y", colors="white", labelsize=7)
        chart_inset.set_ylim(bottom=0)

        # Star ride label
        ax_chart.text(0.5, 0.94,
                       textwrap.shorten(star_ride, width=45, placeholder="…"),
                       transform=ax_chart.transAxes,
                       color="white", fontsize=9, alpha=0.7,
                       ha="center", va="center")

    # Footer
    ax_chart.text(0.5, 0.02,
                   "themepark-analytics.io  ·  Datos actualizados cada 15 min",
                   transform=ax_chart.transAxes,
                   color="white", fontsize=7, alpha=0.25,
                   ha="center", va="center")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=DPI, bbox_inches="tight",
                facecolor=theme["bg1"])
    plt.close(fig)
    return output_path


# =============================================================================
# HELPERS
# =============================================================================

def _draw_gradient_bg(ax, color1: str, color2: str):
    """Simula un degradado horizontal con un rectángulo coloreado."""
    ax.set_facecolor(color1)
    for i in range(100):
        alpha = i / 100
        r1, g1, b1 = _hex_to_rgb(color1)
        r2, g2, b2 = _hex_to_rgb(color2)
        r = r1 + (r2 - r1) * alpha
        g = g1 + (g2 - g1) * alpha
        b = b1 + (b2 - b1) * alpha
        ax.axvspan(i / 100, (i + 1) / 100, facecolor=(r, g, b), alpha=0.6)


def _hex_to_rgb(hex_color: str) -> tuple:
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i+2], 16) / 255 for i in (0, 2, 4))


def _operator_label(park_name: str) -> str:
    op = _get_operator(park_name)
    labels = {
        "disney":    "Disney Parks",
        "universal": "Universal Parks",
        "six_flags": "Six Flags",
        "merlin":    "Merlin Entertainments",
        "europa":    "Parques Europeos",
        "default":   "Theme Parks",
    }
    return labels.get(op, "Theme Parks")


# =============================================================================
# ENTRY POINT
# =============================================================================

def generate_park_report(park_name: str, report_date: date,
                          park_id: int, conn) -> tuple[Path, Path, str, str]:
    """
    Genera el PNG + captions ES/EN para un parque y fecha dados.

    Returns:
        (png_path, png_path, caption_es, caption_en)
        El PNG es el mismo para ambos idiomas; los captions difieren.
    """
    opening_time, closing_time = _fetch_opening_closing(conn, park_id, report_date)
    if not opening_time:
        raise ValueError(f"No hay horario para {park_name} en {report_date}")
        
    
    timezone = _get_park_timezone(park_name)
    tiers      = _fetch_top_by_tier(conn, park_id, opening_time, closing_time)
    kpis       = _fetch_kpis(conn, park_id, opening_time, closing_time, timezone)

    # Atracción estrella = Tier 1 con más espera
    star_ride = "—"
    if not tiers[1].empty:
        star_ride = tiers[1].iloc[0]["ride_name"]
    elif not tiers[2].empty:
        star_ride = tiers[2].iloc[0]["ride_name"]

    df_hourly  = _fetch_hourly(conn, park_id, star_ride, opening_time, closing_time, timezone)
    theme      = OPERATOR_THEMES[_get_operator(park_name)]
    logo_path  = _logo_path(park_name)

    slug       = park_name.replace(" ", "_").replace("'", "").replace("é", "e").replace("ñ", "n")
    date_str   = report_date.isoformat()
    out_dir    = OUTPUT_DIR / date_str
    png_path   = out_dir / f"{slug}_{date_str}.png"

    _render_png(park_name, report_date, tiers, df_hourly,
                kpis, star_ride, theme, logo_path, png_path)

    caption_es, caption_en = _build_caption(park_name, report_date, tiers, kpis)

    return png_path, caption_es, caption_en
