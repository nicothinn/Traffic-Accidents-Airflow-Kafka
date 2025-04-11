from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Credenciales para ambas bases de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME_SOURCE = "CrashTraffic"
DB_NAME_DIM = "CrashTraffic_Dimensional"

# Crear URLs de conexión para SQLAlchemy
SOURCE_DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_SOURCE}"
DIM_DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_DIM}"

# Funciones para cada tarea
def setup_tables():
    engine = None
    conn = None
    try:
        engine = create_engine(DIM_DB_URL)
        conn = engine.connect()
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_fecha (
            Fecha_ID SERIAL PRIMARY KEY,
            Día INTEGER,
            Mes INTEGER,
            Año INTEGER,
            Día_Semana VARCHAR(20),
            Hora TIME
        );
        CREATE TABLE IF NOT EXISTS dim_ubicacion (
            Ubicación_ID SERIAL PRIMARY KEY,
            Latitud DECIMAL(10,6),
            Longitud DECIMAL(10,6),
            Intersección VARCHAR(3)
        );
        CREATE TABLE IF NOT EXISTS dim_clima (
            Clima_ID SERIAL PRIMARY KEY,
            Condición_Climática VARCHAR(50)
        );
        CREATE TABLE IF NOT EXISTS dim_iluminacion (
            Iluminación_ID SERIAL PRIMARY KEY,
            Condición_Iluminación VARCHAR(50)
        );
        CREATE TABLE IF NOT EXISTS dim_condicion_camino (
            Condición_Camino_ID SERIAL PRIMARY KEY,
            Superficie_Carretera VARCHAR(50),
            Defecto_Carretera VARCHAR(50)
        );
        CREATE TABLE IF NOT EXISTS dim_tipo_accidente (
            Tipo_Accidente_ID SERIAL PRIMARY KEY,
            Tipo_Primer_Choque VARCHAR(50),
            Tipo_Vía VARCHAR(50),
            Alineación VARCHAR(50),
            Nivel_Lesión VARCHAR(50)
        );
        CREATE TABLE IF NOT EXISTS dim_contribuyente_principal (
            Contribuyente_Principal_ID SERIAL PRIMARY KEY,
            Causa_Principal VARCHAR(100)
        );
        CREATE TABLE IF NOT EXISTS hechos_accidentes (
            ID_Hecho INTEGER PRIMARY KEY,
            Fecha_ID INTEGER,
            Ubicación_ID INTEGER,
            Clima_ID INTEGER,
            Iluminación_ID INTEGER,
            Condición_Camino_ID INTEGER,
            Tipo_Accidente_ID INTEGER,
            Contribuyente_Principal_ID INTEGER,
            Unidades_Involucradas INTEGER,
            Total_Lesiones INTEGER,
            Fatalidades INTEGER,
            Incapacitantes INTEGER,
            No_Incapacitantes INTEGER,
            Reportadas_No_Evidentes INTEGER,
            Sin_Indicación INTEGER,
            FOREIGN KEY (Fecha_ID) REFERENCES dim_fecha(Fecha_ID),
            FOREIGN KEY (Ubicación_ID) REFERENCES dim_ubicacion(Ubicación_ID),
            FOREIGN KEY (Clima_ID) REFERENCES dim_clima(Clima_ID),
            FOREIGN KEY (Iluminación_ID) REFERENCES dim_iluminacion(Iluminación_ID),
            FOREIGN KEY (Condición_Camino_ID) REFERENCES dim_condicion_camino(Condición_Camino_ID),
            FOREIGN KEY (Tipo_Accidente_ID) REFERENCES dim_tipo_accidente(Tipo_Accidente_ID),
            FOREIGN KEY (Contribuyente_Principal_ID) REFERENCES dim_contribuyente_principal(Contribuyente_Principal_ID)
        );
        """))
        log.info("Tablas creadas exitosamente.")
    except Exception as e:
        log.error(f"Error en setup_tables: {str(e)}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
        if engine:
            engine.dispose()
        log.info("Conexión cerrada en setup_tables.")

def extract():
    source_engine = None
    dim_engine = None
    src_conn = None
    dim_conn = None
    try:
        source_engine = create_engine(SOURCE_DB_URL)
        dim_engine = create_engine(DIM_DB_URL)
        src_conn = source_engine.connect()
        dim_conn = dim_engine.connect()

        # dim_fecha
        result = src_conn.execute(text("""
        SELECT DISTINCT
            EXTRACT(DAY FROM crash_date) AS Día,
            EXTRACT(MONTH FROM crash_date) AS Mes,
            EXTRACT(YEAR FROM crash_date) AS Año,
            TO_CHAR(crash_date, 'Dy') AS Día_Semana,
            CAST(crash_date AS TIME) AS Hora
        FROM public.accidentes
        """)).fetchall()
        for row in result:
            dim_conn.execute(
                text("""
                INSERT INTO dim_fecha (Día, Mes, Año, Día_Semana, Hora)
                VALUES (:dia, :mes, :ano, :dia_semana, :hora)
                ON CONFLICT DO NOTHING
                """),
                {"dia": row[0], "mes": row[1], "ano": row[2], "dia_semana": row[3], "hora": row[4]}
            )

        # dim_ubicacion
        result = src_conn.execute(text("""
        SELECT DISTINCT
            Start_Lat::DECIMAL(10,6) AS Latitud,
            Start_Lng::DECIMAL(10,6) AS Longitud,
            intersection_related AS Intersección
        FROM public.accidentes
        """)).fetchall()
        for row in result:
            dim_conn.execute(
                text("""
                INSERT INTO dim_ubicacion (Latitud, Longitud, Intersección)
                VALUES (:latitud, :longitud, :interseccion)
                ON CONFLICT DO NOTHING
                """),
                {"latitud": row[0], "longitud": row[1], "interseccion": row[2]}
            )

        # dim_clima
        result = src_conn.execute(text("""
        SELECT DISTINCT weather_condition AS Condición_Climática
        FROM public.accidentes
        """)).fetchall()
        for row in result:
            dim_conn.execute(
                text("""
                INSERT INTO dim_clima (Condición_Climática)
                VALUES (:condicion)
                ON CONFLICT DO NOTHING
                """),
                {"condicion": row[0]}
            )

        # dim_iluminacion
        result = src_conn.execute(text("""
        SELECT DISTINCT lighting_condition AS Condición_Iluminación
        FROM public.accidentes
        """)).fetchall()
        for row in result:
            dim_conn.execute(
                text("""
                INSERT INTO dim_iluminacion (Condición_Iluminación)
                VALUES (:condicion)
                ON CONFLICT DO NOTHING
                """),
                {"condicion": row[0]}
            )

        # dim_condicion_camino
        result = src_conn.execute(text("""
        SELECT DISTINCT
            roadway_surface_cond AS Superficie_Carretera,
            road_defect AS Defecto_Carretera
        FROM public.accidentes
        """)).fetchall()
        for row in result:
            dim_conn.execute(
                text("""
                INSERT INTO dim_condicion_camino (Superficie_Carretera, Defecto_Carretera)
                VALUES (:superficie, :defecto)
                ON CONFLICT DO NOTHING
                """),
                {"superficie": row[0], "defecto": row[1]}
            )

        # dim_tipo_accidente
        result = src_conn.execute(text("""
        SELECT DISTINCT
            first_crash_type AS Tipo_Primer_Choque,
            trafficway_type AS Tipo_Vía,
            alignment AS Alineación,
            most_severe_injury AS Nivel_Lesión
        FROM public.accidentes
        """)).fetchall()
        for row in result:
            dim_conn.execute(
                text("""
                INSERT INTO dim_tipo_accidente (Tipo_Primer_Choque, Tipo_Vía, Alineación, Nivel_Lesión)
                VALUES (:tipo_choque, :tipo_via, :alineacion, :nivel_lesion)
                ON CONFLICT DO NOTHING
                """),
                {"tipo_choque": row[0], "tipo_via": row[1], "alineacion": row[2], "nivel_lesion": row[3]}
            )

        # dim_contribuyente_principal
        result = src_conn.execute(text("""
        SELECT DISTINCT prim_contributory_cause AS Causa_Principal
        FROM public.accidentes
        """)).fetchall()
        for row in result:
            dim_conn.execute(
                text("""
                INSERT INTO dim_contribuyente_principal (Causa_Principal)
                VALUES (:causa)
                ON CONFLICT DO NOTHING
                """),
                {"causa": row[0]}
            )

        log.info("Extracción completada exitosamente.")
    except Exception as e:
        log.error(f"Error en extract: {str(e)}", exc_info=True)
        raise
    finally:
        if src_conn:
            src_conn.close()
        if dim_conn:
            dim_conn.close()
        if source_engine:
            source_engine.dispose()
        if dim_engine:
            dim_engine.dispose()
        log.info("Conexiones cerradas en extract.")

def transform():
    source_engine = None
    dim_engine = None
    src_conn = None
    dim_conn = None
    try:
        source_engine = create_engine(SOURCE_DB_URL)
        dim_engine = create_engine(DIM_DB_URL)
        src_conn = source_engine.connect()
        dim_conn = dim_engine.connect()

        # 1. Cargar tablas dimensionales en memoria como diccionarios
        fecha_rows = dim_conn.execute(text("SELECT Fecha_ID, Día, Mes, Año, Día_Semana, Hora FROM dim_fecha")).fetchall()
        fecha_dict = {(row[1], row[2], row[3], row[4], row[5]): row[0] for row in fecha_rows}

        ubicacion_rows = dim_conn.execute(text("SELECT Ubicación_ID, Latitud, Longitud, Intersección FROM dim_ubicacion")).fetchall()
        ubicacion_dict = {(row[1], row[2], row[3]): row[0] for row in ubicacion_rows}

        clima_rows = dim_conn.execute(text("SELECT Clima_ID, Condición_Climática FROM dim_clima")).fetchall()
        clima_dict = {row[1]: row[0] for row in clima_rows}

        iluminacion_rows = dim_conn.execute(text("SELECT Iluminación_ID, Condición_Iluminación FROM dim_iluminacion")).fetchall()
        iluminacion_dict = {row[1]: row[0] for row in iluminacion_rows}

        condicion_rows = dim_conn.execute(text("SELECT Condición_Camino_ID, Superficie_Carretera, Defecto_Carretera FROM dim_condicion_camino")).fetchall()
        condicion_dict = {(row[1], row[2]): row[0] for row in condicion_rows}

        tipo_rows = dim_conn.execute(text("SELECT Tipo_Accidente_ID, Tipo_Primer_Choque, Tipo_Vía, Alineación, Nivel_Lesión FROM dim_tipo_accidente")).fetchall()
        tipo_dict = {(row[1], row[2], row[3], row[4]): row[0] for row in tipo_rows}

        contribuyente_rows = dim_conn.execute(text("SELECT Contribuyente_Principal_ID, Causa_Principal FROM dim_contribuyente_principal")).fetchall()
        contribuyente_dict = {row[1]: row[0] for row in contribuyente_rows}

        # 2. Extraer datos de la fuente y transformarlos
        result = src_conn.execute(text("""
        SELECT
            a.id,
            a.crash_date,
            a.Start_Lat::DECIMAL(10,6) AS latitud,
            a.Start_Lng::DECIMAL(10,6) AS longitud,
            a.intersection_related,
            a.weather_condition,
            a.lighting_condition,
            a.roadway_surface_cond,
            a.road_defect,
            a.first_crash_type,
            a.trafficway_type,
            a.alignment,
            a.most_severe_injury,
            a.prim_contributory_cause,
            a.num_units,
            a.injuries_total::INTEGER,
            a.injuries_fatal::INTEGER,
            a.injuries_incapacitating::INTEGER,
            a.injuries_non_incapacitating::INTEGER,
            a.injuries_reported_not_evident::INTEGER,
            a.injuries_no_indication::INTEGER
        FROM public.accidentes a
        """)).fetchall()

        # 3. Transformar datos en lotes y acumularlos en una lista
        batch_size = 1000
        hechos_batches = []
        hechos_batch = []
        
        for row in result:
            fecha_key = (int(row[1].day), int(row[1].month), int(row[1].year), row[1].strftime('%a'), row[1].time())
            fecha_id = fecha_dict.get(fecha_key)

            ubicacion_key = (row[2], row[3], row[4])
            ubicacion_id = ubicacion_dict.get(ubicacion_key)

            clima_id = clima_dict.get(row[5])
            iluminacion_id = iluminacion_dict.get(row[6])
            condicion_key = (row[7], row[8])
            condicion_camino_id = condicion_dict.get(condicion_key)
            tipo_key = (row[9], row[10], row[11], row[12])
            tipo_accidente_id = tipo_dict.get(tipo_key)
            contribuyente_id = contribuyente_dict.get(row[13])

            hechos_row = {
                "id": row[0],
                "fecha_id": fecha_id,
                "ubicacion_id": ubicacion_id,
                "clima_id": clima_id,
                "iluminacion_id": iluminacion_id,
                "condicion_camino_id": condicion_camino_id,
                "tipo_accidente_id": tipo_accidente_id,
                "contribuyente_id": contribuyente_id,
                "unidades": row[14],
                "total_lesiones": row[15],
                "fatalidades": row[16],
                "incapacitantes": row[17],
                "no_incapacitantes": row[18],
                "reportadas_no_evidentes": row[19],
                "sin_indicacion": row[20]
            }
            hechos_batch.append(hechos_row)

            if len(hechos_batch) >= batch_size:
                hechos_batches.append(hechos_batch)
                hechos_batch = []

        if hechos_batch:  # Agregar cualquier resto
            hechos_batches.append(hechos_batch)

        log.info("Transformación completada exitosamente.")
        return hechos_batches
    except Exception as e:
        log.error(f"Error en transform: {str(e)}", exc_info=True)
        raise
    finally:
        if src_conn:
            src_conn.close()
        if dim_conn:
            dim_conn.close()
        if source_engine:
            source_engine.dispose()
        if dim_engine:
            dim_engine.dispose()
        log.info("Conexiones cerradas en transform.")

def load(**context):
    dim_engine = None
    dim_conn = None
    try:
        log.info("Contexto recibido en load: %s", context)
        if not context:
            raise ValueError("El contexto no se pasó correctamente a la función load.")

        dim_engine = create_engine(DIM_DB_URL)
        dim_conn = dim_engine.connect()
        task_instance = context.get('task_instance')
        if not task_instance:
            raise ValueError("No se encontró 'task_instance' en el contexto.")

        hechos_batches = task_instance.xcom_pull(task_ids='transform')
        if not hechos_batches:
            raise ValueError("No se encontraron datos en XCom para la tarea 'transform'.")

        for batch in hechos_batches:
            dim_conn.execute(
                text("""
                INSERT INTO hechos_accidentes (
                    ID_Hecho, Fecha_ID, Ubicación_ID, Clima_ID, Iluminación_ID, Condición_Camino_ID,
                    Tipo_Accidente_ID, Contribuyente_Principal_ID, Unidades_Involucradas, Total_Lesiones,
                    Fatalidades, Incapacitantes, No_Incapacitantes, Reportadas_No_Evidentes, Sin_Indicación
                )
                VALUES (:id, :fecha_id, :ubicacion_id, :clima_id, :iluminacion_id, :condicion_camino_id,
                        :tipo_accidente_id, :contribuyente_id, :unidades, :total_lesiones, :fatalidades,
                        :incapacitantes, :no_incapacitantes, :reportadas_no_evidentes, :sin_indicacion)
                ON CONFLICT (ID_Hecho) DO NOTHING
                """),
                batch
            )

        log.info("Carga completada exitosamente.")
    except Exception as e:
        log.error(f"Error en load: {str(e)}", exc_info=True)
        raise
    finally:
        if dim_conn:
            dim_conn.close()
        if dim_engine:
            dim_engine.dispose()
        log.info("Conexión cerrada en load.")

# Definir argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'max_active_tasks': 4,  # Limitar el número de tareas simultáneas
}

# Definir el DAG
with DAG(
    'etl_crash_traffic_sqlalchemy',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,  # Limitar a una ejecución activa a la vez
    catchup=False
) as dag:
    # Tareas usando PythonOperator
    task_setup_tables = PythonOperator(
        task_id='setup_tables',
        python_callable=setup_tables
    )

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True  # Asegurar que el contexto se pase
    )

    # Definir dependencias
    task_setup_tables >> task_extract >> task_transform >> task_load