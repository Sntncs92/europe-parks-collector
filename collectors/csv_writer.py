import os
import csv

def save_to_csv(nombre_parque, filas, ahora_local, continent, country):
    """
    Lo guardamos con la ruta data/raw/continent/country/parque.csv
    """
    ruta_carpeta = os.path.join("data", "raw", continent, country, nombre_parque.replace(" ", "_"))
    os.makedirs(ruta_carpeta, exist_ok=True)

    nombre_archivo = f"{nombre_parque.replace(' ', '_')}_{ahora_local.date().isoformat()}.csv"
    ruta_archivo = os.path.join(ruta_carpeta, nombre_archivo)

    archivo_existe = os.path.isfile(ruta_archivo)

    with open(ruta_archivo, mode="a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        if not archivo_existe:
            # Escribir cabecera
            writer.writerow([
                "timestamp",
                "weekday",
                "ride_id",
                "ride_name",
                "status",
                "wait_time",
                "evento"
            ])

        # Escribir filas
        for fila in filas:
            writer.writerow(fila)

    return len(filas), ruta_archivo
