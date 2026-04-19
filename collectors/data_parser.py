def parse_live_data(data, evento_activo, ahora_local):
    """
    Recoge los datos JSON que envia la API y lo convierte a filas para el CSV
    """
    if not data:
        return []

    atracciones = data.get("liveData", [])
    filas = []

    for ride in atracciones:
        if ride.get("entityType") != "ATTRACTION":
            continue

        filas.append([
            ahora_local.isoformat(),
            ahora_local.strftime("%A"),
            ride.get("id", ""),
            ride.get("name", ""),
            ride.get("status", ""),
            ride.get("queue", {})
                .get("STANDBY", {})
                .get("waitTime", ""),
            evento_activo
        ])

    return filas