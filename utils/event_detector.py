from datetime import datetime

def detectar_evento(parque, fecha):
    """
    Devuelve el nombre del evento activo o ""
    """
    for evento in parque.get("eventos", []):
        desde = datetime.fromisoformat(evento["desde"]).date()
        hasta = datetime.fromisoformat(evento["hasta"]).date()

        if desde <= fecha <= hasta:
            return evento["nombre"]

    return ""