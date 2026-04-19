import json


def cargar_parques(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        parques = json.load(file)
    return parques