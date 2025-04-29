from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import os
import glob
import pandas as pd
import ast
from sqlalchemy import create_engine, text
import overpy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
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
DB_NAME_SOURCE =os.getenv("DB_NAME_SOURCE", "CrashTraffic")
DB_NAME_DIM = os.getenv("DB_NAME_DIM", "CrashTrafficDim")

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
            Intersección VARCHAR(3),
            Approx_City VARCHAR(100),
            Approx_County VARCHAR(100),
            Approx_State VARCHAR(50),
            Approx_Postcode VARCHAR(10)
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

        CREATE TABLE IF NOT EXISTS dim_infraestructura (
            BBox_Label VARCHAR(50) PRIMARY KEY,
            Category_Hospital INTEGER,
            Category_School INTEGER,
            Crossing_Combinations INTEGER,
            Crossing_Marked INTEGER,
            Crossing_Uncontrolled INTEGER,
            Crossing_Unknown INTEGER,
            Crossing_Unmarked INTEGER,
            Crossing_Zebra INTEGER,
            Traffic_Signals_Bridge INTEGER,
            Traffic_Signals_Emergency INTEGER,
            Traffic_Signals_Level_Crossing INTEGER,
            Traffic_Signals_Pedestrian_Crossing INTEGER,
            Traffic_Signals_Ramp_Meter INTEGER,
            Traffic_Signals_Signal INTEGER,
            Traffic_Signals_Traffic_Lights INTEGER,
            Traffic_Signals_Unknown INTEGER
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


def extract_bbox_osm():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    raw_folder = "/opt/airflow/data/raw/"
    bbox_df = pd.DataFrame([
        [35.0, -81.0], [32.5, -97.0], [34.0, -118.5], [29.5, -95.5],
        [34.5, -82.5], [37.5, -122.5], [35.5, -79.0], [33.5, -118.5],
        [40.5, -74.0], [36.0, -87.0], [41.5, -88.0], [33.5, -84.5],
        [42.0, -71.5], [25.5, -80.5], [33.5, -118.0], [34.0, -118.0],
        [40.5, -74.5], [30.0, -98.0], [26.0, -80.5], [30.0, -82.0],
        [29.5, -96.0], [40.0, -75.5], [28.5, -81.5], [41.5, -88.5],
        [30.0, -91.5], [47.5, -122.5], [37.0, -122.0], [38.5, -77.5],
        [34.5, -83.0], [34.5, -87.0], [41.0, -82.0], [42.0, -83.5],
        [34.0, -117.5], [39.5, -84.5], [27.5, -83.0], [38.5, -90.5]
    ], columns=["lat_bin", "lng_bin"])

    api = overpy.Overpass()

    for i, row in bbox_df.iterrows():
        minLat = row['lat_bin']
        minLon = row['lng_bin']
        maxLat = minLat + 0.5
        maxLon = minLon + 0.5
        label = f"{minLat}_{minLon}"

        log.info(f"Consultando bbox {label} → ({minLat}, {minLon}, {maxLat}, {maxLon})")

        query = f"""
        [out:json][timeout:25];
        (
          node["amenity"="school"]({minLat},{minLon},{maxLat},{maxLon});
          node["amenity"="hospital"]({minLat},{minLon},{maxLat},{maxLon});
          node["highway"="traffic_signals"]({minLat},{minLon},{maxLat},{maxLon});
          node["highway"="crossing"]({minLat},{minLon},{maxLat},{maxLon});
        );
        out body;
        """

        try:
            result = api.query(query)
            data = []
            for node in tqdm(result.nodes, desc=f"Procesando bbox {label}", leave=False):
                data.append({
                    "bbox_label": label,
                    "category": node.tags.get("amenity", node.tags.get("highway", "N/A")),
                    "latitude": node.lat,
                    "longitude": node.lon,
                    "tags": str(node.tags)
                })

            df_resultado = pd.DataFrame(data)
            output_file = os.path.join(raw_folder, f"bbox_{label}_osm.csv")
            df_resultado.to_csv(output_file, index=False)
            log.info(f"Guardado {output_file}")
            time.sleep(5)

        except Exception as e:
            log.error(f"Error en bbox {label}: {e}", exc_info=True)



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

def transform_bbox_data():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    raw_folder = "/opt/airflow/data/raw/"
    processed_folder = "/opt/airflow/data/processed/"
    files = glob.glob(os.path.join(raw_folder, "bbox*_osm.csv"))

    geolocator = Nominatim(user_agent="bbox_locator")
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    def map_traffic_signal(val):
        ts_values = ["traffic_lights", "signal", "pedestrian_crossing", "ramp_meter", "level_crossing", "emergency", "bridge"]
        v = str(val).strip().lower()
        return v if v in ts_values else "unknown"

    def map_crossing(val):
        crossing_values = ["uncontrolled", "marked", "unmarked", "zebra", "pelican", "puffin", "toucan"]
        v = str(val).strip().lower()
        if ";" in v:
            return "combinations"
        return v if v in crossing_values else "unknown"

    def parse_approx_city(address):
        if address == "Not found" or pd.isna(address):
            return {"city": None, "county": None, "state": None, "postcode": None}
        parts = [p.strip() for p in address.split(",")]
        city, county, state, postcode = None, None, None, None
        if len(parts) >= 5:
            city = parts[1]
            county = parts[2]
            state = parts[3]
            if parts[4].isdigit() or len(parts[4]) == 5:
                postcode = parts[4]
        elif len(parts) == 4:
            city = parts[0]
            county = parts[1]
            state = parts[2]
        elif len(parts) == 3:
            city = parts[0]
            county = parts[1]
            state = parts[2]
        else:
            city = parts[0]
        return {"city": city, "county": county, "state": state, "postcode": postcode}

    summary_list = []
    bbox_city_map = {}

    for file in files:
        bbox_label = os.path.basename(file).replace("_osm.csv", "")
        try:
            lat, lng = map(float, bbox_label.replace("bbox_", "").split("_"))
        except ValueError:
            lat, lng = None, None

        if lat is not None and lng is not None:
            try:
                location = geocode(f"{lat}, {lng}", exactly_one=True)
                if location:
                    bbox_city_map[bbox_label] = location.address
                else:
                    bbox_city_map[bbox_label] = "Not found"
            except Exception as e:
                log.error(f"Error geocoding bbox {bbox_label}: {e}")
                bbox_city_map[bbox_label] = "Error geocoding"

        df = pd.read_csv(file)
        df["tags"] = df["tags"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {})
        tags_expanded = df["tags"].apply(pd.Series)
        df_cleaned = pd.concat([df.drop(columns=["tags"]), tags_expanded], axis=1)
        df_cleaned.fillna("unknown", inplace=True)
        df_filtered = df_cleaned[df_cleaned["category"].isin(["school", "hospital", "traffic_signals", "crossing"])].copy()

        group_school_hospital = df_filtered[df_filtered["category"].isin(["school", "hospital"])] \
            .groupby("category").size().reset_index(name="count")
        group_school_hospital.rename(columns={"category": "value"}, inplace=True)
        group_school_hospital["group"] = "category"

        df_ts = df_filtered[df_filtered["category"] == "traffic_signals"].copy()
        if not df_ts.empty and "traffic_signals" in df_ts.columns:
            df_ts["mapped"] = df_ts["traffic_signals"].apply(map_traffic_signal)
            group_ts = df_ts.groupby("mapped").size().reset_index(name="count")
            group_ts.rename(columns={"mapped": "value"}, inplace=True)
            group_ts["group"] = "traffic_signals"
        else:
            group_ts = pd.DataFrame(columns=["value", "count", "group"])

        df_cross = df_filtered[df_filtered["category"] == "crossing"].copy()
        if not df_cross.empty and "crossing" in df_cross.columns:
            df_cross["mapped"] = df_cross["crossing"].apply(map_crossing)
            group_cross = df_cross.groupby("mapped").size().reset_index(name="count")
            group_cross.rename(columns={"mapped": "value"}, inplace=True)
            group_cross["group"] = "crossing"
        else:
            group_cross = pd.DataFrame(columns=["value", "count", "group"])

        summary_bbox = pd.concat([group_school_hospital, group_ts, group_cross], ignore_index=True)
        summary_bbox["bbox_label"] = bbox_label
        summary_list.append(summary_bbox)

    df_all = pd.concat(summary_list, ignore_index=True)
    pivot_df = df_all.pivot_table(index="bbox_label", columns=["group", "value"], values="count", fill_value=0)
    pivot_df.columns = [f"{grp}_{val}" for grp, val in pivot_df.columns]
    pivot_df.reset_index(inplace=True)

    pivot_df["approx_city"] = pivot_df["bbox_label"].map(bbox_city_map)

    parsed_data = pivot_df["approx_city"].apply(parse_approx_city).apply(pd.Series)
    pivot_df = pd.concat([pivot_df, parsed_data], axis=1)

    os.makedirs(processed_folder, exist_ok=True)
    output_path = os.path.join(processed_folder, "combined_bbox_summary_final.csv")
    pivot_df.to_csv(output_path, index=False)
    log.info(f"Archivo final guardado en '{output_path}'")

def merge_accidents_with_api():
    try:
        engine = create_engine(SOURCE_DB_URL)
        conn = engine.connect()

        log.info("Cargando datos de accidentes desde la base de datos...")
        df_accidents = pd.read_sql("SELECT * FROM accidentes", conn)

        df_accidents['lat_bin'] = (df_accidents['start_lat'] // 0.5) * 0.5
        df_accidents['lng_bin'] = (df_accidents['start_lng'] // 0.5) * 0.5
        df_accidents['bbox_label'] = df_accidents['lat_bin'].astype(str) + "_" + df_accidents['lng_bin'].astype(str)

        log.info("Leyendo datos de infraestructura desde CSV de la API...")
        api_df = pd.read_csv("data/processed/combined_bbox_summary_with_city.csv")

        columnas_utiles = [
            'bbox_label',
            'category_hospital', 'category_school',
            'crossing_combinations', 'crossing_marked', 'crossing_uncontrolled',
            'crossing_unknown', 'crossing_unmarked', 'crossing_zebra',
            'traffic_signals_bridge', 'traffic_signals_emergency',
            'traffic_signals_level_crossing', 'traffic_signals_pedestrian_crossing',
            'traffic_signals_ramp_meter', 'traffic_signals_signal',
            'traffic_signals_traffic_lights', 'traffic_signals_unknown',
            'city', 'county', 'state', 'postcode'
        ]

        api_df = api_df[columnas_utiles]

        # Renombrar campos de ciudad para evitar colisión de nombres
        api_df = api_df.rename(columns={
            "city": "aprox_city",
            "county": "aprox_county",
            "state": "aprox_state",
            "postcode": "aprox_postcode"
        })

        log.info("Haciendo el merge entre accidentes y datos de la API por bbox_label...")
        merged_df = pd.merge(df_accidents, api_df, on="bbox_label", how="inner")

        log.info("Guardando el resultado como nueva tabla 'accidentes_final'...")
        merged_df.to_sql("accidentes_final", conn, if_exists="replace", index=False)

        log.info("Tabla 'accidentes_final' creada con éxito.")

    except Exception as e:
        log.error(f"Error en merge_accidents_with_api: {str(e)}", exc_info=True)
        raise

    finally:
        if conn:
            conn.close()
        if engine:
            engine.dispose()
        log.info("Conexión cerrada en merge_accidents_with_api.")

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
        infraestructura_batches = task_instance.xcom_pull(task_ids='api_transform_bbox_data')

        if not hechos_batches:
            raise ValueError("No se encontraron datos en XCom para la tarea 'transform'.")

        if not infraestructura_batches:
            raise ValueError("No se encontraron datos en XCom para la tarea 'api_transform_bbox_data'.")

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

        for infra_batch in infraestructura_batches:
            dim_conn.execute(
                text("""
                INSERT INTO dim_infraestructura (
                    BBox_Label, Category_Hospital, Category_School, Crossing_Combinations, Crossing_Marked,
                    Crossing_Uncontrolled, Crossing_Unknown, Crossing_Unmarked, Crossing_Zebra,
                    Traffic_Signals_Bridge, Traffic_Signals_Emergency, Traffic_Signals_Level_Crossing,
                    Traffic_Signals_Pedestrian_Crossing, Traffic_Signals_Ramp_Meter, Traffic_Signals_Signal,
                    Traffic_Signals_Traffic_Lights, Traffic_Signals_Unknown
                )
                VALUES (
                    :bbox_label, :category_hospital, :category_school, :crossing_combinations,
                    :crossing_marked, :crossing_uncontrolled, :crossing_unknown, :crossing_unmarked,
                    :crossing_zebra, :traffic_signals_bridge, :traffic_signals_emergency,
                    :traffic_signals_level_crossing, :traffic_signals_pedestrian_crossing,
                    :traffic_signals_ramp_meter, :traffic_signals_signal, :traffic_signals_traffic_lights,
                    :traffic_signals_unknown
                )
                ON CONFLICT (BBox_Label) DO NOTHING
                """),
                infra_batch
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


# Configuración por defecto del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'max_active_tasks': 4,
}

with DAG(
    'etl_crash_traffic_sqlalchemy',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False
) as dag:

    task_setup_tables = PythonOperator(
        task_id='setup_tables',
        python_callable=setup_tables
    )

    # ETL para dataset de accidentes (base de datos)
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    # ETL para datos de infraestructura (API)
    task_api_extract = PythonOperator(
        task_id='api_extract_bbox_osm',
        python_callable=extract_bbox_osm
    )

    task_api_transform = PythonOperator(
        task_id='api_transform_bbox_data',
        python_callable=transform_bbox_data
    )

    # Carga final en el modelo dimensional
    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )

    # Definir dependencias
    task_setup_tables >> [task_extract, task_api_extract]

    task_extract >> task_transform
    task_api_extract >> task_api_transform

    [task_transform, task_api_transform] >> task_load