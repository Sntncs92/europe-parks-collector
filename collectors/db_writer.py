"""
Módulo para escribir datos de parques a PostgreSQL
Compatible con SQLAlchemy 2.0+
"""

import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text
from datetime import datetime
from utils.logger import setup_logger

logger = setup_logger()


class DatabaseWriter:
    """Maneja la escritura de datos de parques a PostgreSQL"""

    def __init__(self, db_config):
        """
        Inicializa la conexión a PostgreSQL

        Args:
            db_config: dict con keys: host, port, database, user, password
        """
        self.config = db_config
        self.conn = None
        self.cursor = None
        self.engine = None
        self._connect()

        # Cache de park_ids para evitar queries repetidas
        self.park_cache = {}

    def _connect(self):
        """Establece conexión a PostgreSQL"""
        try:
            # Conexión con psycopg2 para operaciones transaccionales
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            self.cursor = self.conn.cursor()

            # Engine de SQLAlchemy para pandas.to_sql
            connection_string = (
                f"postgresql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            self.engine = create_engine(connection_string)

            logger.info("✓ Conectado a PostgreSQL")
        except Exception as e:
            logger.error(f"✗ Error conectando a PostgreSQL: {e}")
            raise

    def _reconnect(self):
        """Reconecta si la conexión se perdió"""
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        self._connect()
        # Limpiar cache para forzar re-fetch de park_ids
        self.park_cache = {}

    def _ensure_connected(self):
        """Comprueba si la conexión está viva y reconecta si es necesario"""
        try:
            # Ping ligero a PostgreSQL
            self.cursor.execute("SELECT 1")
        except Exception:
            logger.warning("⚠️ Conexión perdida, reconectando...")
            self._reconnect()
            logger.info("✓ Reconexión exitosa")

    def get_or_create_park(self, park_name, country, continent):
        """
        Obtiene el park_id o crea un nuevo parque si no existe
        Usa cache para evitar queries repetidas

        Args:
            park_name: Nombre del parque
            country: País
            continent: Continente

        Returns:
            park_id (int)
        """
        # Verificar cache primero
        if park_name in self.park_cache:
            return self.park_cache[park_name]

        self._ensure_connected()

        try:
            # Intentar obtener park existente
            self.cursor.execute(
                "SELECT park_id FROM parks WHERE park_name = %s",
                (park_name,)
            )
            result = self.cursor.fetchone()

            if result:
                park_id = result[0]
                self.park_cache[park_name] = park_id
                return park_id

            # Crear nuevo parque
            self.cursor.execute(
                """
                INSERT INTO parks (park_name, country, continent)
                VALUES (%s, %s, %s)
                RETURNING park_id
                """,
                (park_name, country, continent)
            )
            self.conn.commit()
            park_id = self.cursor.fetchone()[0]
            self.park_cache[park_name] = park_id
            logger.info(f"✓ Nuevo parque registrado: {park_name} (ID: {park_id})")
            return park_id

        except Exception as e:
            logger.error(f"Error obteniendo/creando parque {park_name}: {e}")
            self.conn.rollback()
            raise

    def ensure_rides_exist(self, rides_data, park_id):
        """
        Asegura que todas las atracciones existen en la BD

        Args:
            rides_data: Lista de listas [timestamp, weekday, ride_id, ride_name, status, wait_time, evento]
            park_id: ID del parque
        """
        try:
            self._ensure_connected()

            # Preparar datos únicos de rides
            # rides_data es una lista de listas: [timestamp, weekday, ride_id, ride_name, status, wait_time, evento]
            unique_rides = {}
            for ride in rides_data:
                ride_id = ride[2]  # Índice 2 es ride_id
                ride_name = ride[3]  # Índice 3 es ride_name
                if ride_id not in unique_rides:
                    unique_rides[ride_id] = ride_name

            for ride_id, ride_name in unique_rides.items():
              self.cursor.execute("""
                INSERT INTO rides (ride_id, park_id, ride_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (park_id, ride_id) DO NOTHING
              """, (str(ride_id), park_id, str(ride_name)))

            self.conn.commit()

        except Exception as e:
            logger.error(f"Error asegurando rides: {e}")
            self.conn.rollback()
            raise

    def insert_wait_times(self, wait_times_data):
        """
        Inserta mediciones de tiempos de espera en batch

        Args:
            wait_times_data: Lista de listas [timestamp, weekday, ride_id, ride_name, status, wait_time, evento]

        Returns:
            Número de registros insertados
        """
        if not wait_times_data:
            return 0

        try:
            self._ensure_connected()

            import pandas as pd

            # Convertir lista de listas a DataFrame
            # Estructura: [timestamp, weekday, ride_id, ride_name, status, wait_time, evento]
            df = pd.DataFrame(wait_times_data, columns=[
                'timestamp', 'weekday', 'ride_id', 'ride_name', 'status', 'wait_time', 'evento'
            ])

            # LIMPIEZA: Convertir strings vacíos a None en wait_time
            # PostgreSQL no acepta '' en columnas INTEGER
            df['wait_time'] = df['wait_time'].replace('', None)

            # Seleccionar solo las columnas necesarias para wait_times
            columns = ['ride_id', 'timestamp', 'weekday', 'status', 'wait_time', 'evento']
            df = df[columns]

            # Insertar usando pandas.to_sql (más eficiente)
            df.to_sql(
                'wait_times',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

            return len(df)

        except Exception as e:
            logger.error(f"Error insertando wait_times: {e}")
            self._reconnect()
            raise

    def save_data(self, park_name, rides_data, country, continent):
        """
        Guarda datos completos de un parque
        Equivalente a save_to_csv() del módulo csv_writer

        Args:
            park_name: Nombre del parque
            rides_data: Lista de listas [timestamp, weekday, ride_id, ride_name, status, wait_time, evento]
            country: País
            continent: Continente

        Returns:
            Tupla (num_registros, descripcion_guardado)
        """
        if not rides_data:
            return 0, f"No hay datos para {park_name}"

        try:
            # 1. Obtener o crear park_id
            park_id = self.get_or_create_park(park_name, country, continent)

            # 2. Asegurar que todas las rides existen
            self.ensure_rides_exist(rides_data, park_id)

            # 3. Insertar wait_times
            num_insertados = self.insert_wait_times(rides_data)

            return num_insertados, f"PostgreSQL: {park_name}"

        except Exception as e:
            logger.error(f"Error guardando datos de {park_name}: {e}")
            return 0, f"ERROR: {park_name}"
            
            
        
    def save_schedule(self, park_name, date, opening_time, closing_time):
      """
      Persiste el horario del día en park_schedules.
      ON CONFLICT DO NOTHING: si ya existe para ese día, lo ignoramos.
      """
      self._ensure_connected()
      try:
          self.cursor.execute("""
              INSERT INTO park_schedules (park_id, date, opening_time, closing_time)
              SELECT park_id, %s, %s, %s
              FROM parks
              WHERE park_name = %s
              ON CONFLICT (park_id, date) DO NOTHING
          """, (date, opening_time, closing_time, park_name))
          self.conn.commit()
      except Exception as e:
          logger.error(f"Error guardando horario de {park_name}: {e}")
          self.conn.rollback()
          raise

    def enqueue_report(self, park_name, report_date, closing_time):
      """
      Encola el reporte diario del parque si no existe ya para ese día.
      ON CONFLICT DO NOTHING garantiza idempotencia ante reinicios del colector.
      """
      self._ensure_connected()
      try:
          self.cursor.execute("""
              INSERT INTO report_queue (park_id, report_date, closing_time)
              SELECT park_id, %s, %s
              FROM parks
              WHERE park_name = %s
              ON CONFLICT (park_id, report_date) DO NOTHING
          """, (report_date, closing_time, park_name))
          self.conn.commit()
      except Exception as e:
          logger.error(f"Error encolando reporte de {park_name}: {e}")
          self.conn.rollback()
          raise
          

    def close(self):
        """Cierra la conexión"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            if self.engine:
                self.engine.dispose()
            logger.info("✓ Conexión a PostgreSQL cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def test_connection(db_config):
    """
    Testea la conexión a PostgreSQL

    Args:
        db_config: dict con configuración de BD

    Returns:
        True si la conexión es exitosa, False en caso contrario
    """
    try:
        writer = DatabaseWriter(db_config)
        writer.close()
        return True
    except Exception as e:
        logger.error(f"Test de conexión falló: {e}")
