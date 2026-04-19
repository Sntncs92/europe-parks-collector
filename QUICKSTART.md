# 🚀 INICIO RÁPIDO

## ⏱️ 5 minutos para arrancar el colector

### Paso 1: Copiar archivos del legacy (1 min)

**Opción A - Script automático:**
```bash
./copy_from_legacy.sh /ruta/a/tu/proyecto/legacy
```

**Opción B - Manual:**
```bash
LEGACY="../tu_proyecto_legacy"

cp $LEGACY/collectors/schedule_client.py collectors/
cp $LEGACY/collectors/live_client.py collectors/
cp $LEGACY/collectors/data_parser.py collectors/
cp $LEGACY/utils/config_loader.py utils/
cp $LEGACY/utils/event_detector.py utils/
cp $LEGACY/utils/logger.py utils/
cp $LEGACY/config/parks.json config/
```

---

### Paso 2: Configurar (2 min)

```bash
# Ejecutar setup automático
python setup.py
```

Te preguntará:
- Host: `localhost` (Enter)
- Puerto: `5432` (Enter)
- Base de datos: `theme_parks` (Enter)
- Usuario: `postgres` (Enter)
- Contraseña: **TU_PASSWORD** (escribe tu password)

---

### Paso 3: Ejecutar (1 min)

```bash
python main.py
```

**¡Listo!** El colector está funcionando.

---

## 📊 Ver que funciona

**Logs:**
```bash
tail -f logs/collector.log
```

**PostgreSQL (pgAdmin):**
```sql
SELECT * FROM wait_times_complete
ORDER BY timestamp DESC
LIMIT 10;
```

Deberías ver datos nuevos cada 15 minutos.

---

## ⚙️ Configuración Opcional

**Cambiar intervalo de recolección:**

Edita `main.py` línea 17:
```python
INTERVALO_SEGUNDOS = 10 * 60  # 10 minutos
```

**Habilitar backup CSV:**

Edita `main.py` línea 15:
```python
ENABLE_CSV_BACKUP = True
```

---

## 🆘 Problemas Comunes

**"No module named 'psycopg2'"**
```bash
pip install -r requirements.txt
```

**"password authentication failed"**
- Verifica `config/database.json`

**"No hay datos para guardar"**
- Espera 15 minutos (intervalo configurado)
- Verifica que algún parque esté abierto

---

## 📖 Documentación Completa

Ver `README.md` para instrucciones detalladas.

---

**¡Eso es todo! 🎉**