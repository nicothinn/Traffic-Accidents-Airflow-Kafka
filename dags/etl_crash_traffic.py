from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import os
import logging
import glob
import time
import ast

import pandas as pd
import numpy as np
from tqdm import tqdm
import overpy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from sqlalchemy import create_engine, text
import psycopg2


# Configurar logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path("/opt/airflow/.env"))
            
# Credenciales para ambas bases de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME_SOURCE = "postgres"
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
            Infraestructura_ID SERIAL PRIMARY KEY,
            BBox_Label VARCHAR(50) UNIQUE,
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
            Infraestructura_ID INTEGER,
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
            FOREIGN KEY (Contribuyente_Principal_ID) REFERENCES dim_contribuyente_principal(Contribuyente_Principal_ID),
            FOREIGN KEY (Infraestructura_ID) REFERENCES dim_infraestructura(Infraestructura_ID)
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





def extract_bbox_osm():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    raw_folder = "/opt/airflow/data/raw/"
    os.makedirs(raw_folder, exist_ok=True)  

    check_file = os.path.join(raw_folder, "bbox_35.0_-81.0_osm.csv")
    if os.path.exists(check_file):
        log.info(f"El archivo {check_file} ya existe. Omitiendo extract_bbox_osm().")
        return
    
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

import os
import glob
import logging
import pandas as pd
import ast
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


def transform_bbox_data():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    raw_folder = "/opt/airflow/data/raw/"
    processed_folder = "/opt/airflow/data/processed/"
    final_path = os.path.join(processed_folder, "combined_bbox_summary_final.csv")
    if os.path.exists(final_path):
        log.info(f"Archivo {final_path} ya existe. Omitiendo transform_bbox_data().")
        return

    files = glob.glob(os.path.join(raw_folder, "bbox*_osm.csv"))

    # Geocoder con detalles estructurados
    geolocator = Nominatim(user_agent="bbox_locator")
    geocode = RateLimiter(
        lambda coords: geolocator.reverse(coords, exactly_one=True, addressdetails=True),
        min_delay_seconds=1
    )

    def map_traffic_signal(val):
        ts_values = ["traffic_lights", "signal", "pedestrian_crossing", "ramp_meter", 
                     "level_crossing", "emergency", "bridge"]
        v = str(val).strip().lower()
        return v if v in ts_values else "unknown"

    def map_crossing(val):
        crossing_values = ["uncontrolled", "marked", "unmarked", "zebra", 
                           "pelican", "puffin", "toucan"]
        v = str(val).strip().lower()
        if ";" in v:
            return "combinations"
        return v if v in crossing_values else "unknown"

    summary_list = []
    bbox_city_map = {}

    for file in files:
        bbox_label = os.path.basename(file).replace("_osm.csv", "")
        try:
            lat, lng = map(float, bbox_label.replace("bbox_", "").split("_"))
        except ValueError:
            lat, lng = None, None

        # Geocodificación estructurada
        if lat is not None and lng is not None:
            try:
                loc = geocode(f"{lat}, {lng}")
                if loc and "address" in loc.raw:
                    addr = loc.raw["address"]
                    bbox_city_map[bbox_label] = {
                        "city": addr.get("city") or addr.get("town") \
                                  or addr.get("village") or addr.get("hamlet"),
                        "county": addr.get("county"),
                        "state": addr.get("state"),
                        "postcode": addr.get("postcode")
                    }
                else:
                    bbox_city_map[bbox_label] = {"city": "unknown", "county": "unknown", "state": "unknown", "postcode": None}
            except Exception as e:
                log.error(f"Error geocoding bbox {bbox_label}: {e}")
                bbox_city_map[bbox_label] = {"city": "unknown", "county": "unknown", "state": "unknown", "postcode": None}

        df = pd.read_csv(file)
        df["tags"] = df["tags"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {})
        tags_expanded = df["tags"].apply(pd.Series)
        df_cleaned = pd.concat([df.drop(columns=["tags"]), tags_expanded], axis=1)
        df_cleaned.fillna("unknown", inplace=True)
        df_filtered = df_cleaned[df_cleaned["category"].isin(["school", "hospital", "traffic_signals", "crossing"])].copy()

        # Conteo de schools y hospitals
        group_sh = df_filtered[df_filtered["category"].isin(["school", "hospital"])].groupby("category").size().reset_index(name="count")
        group_sh.rename(columns={"category": "value"}, inplace=True)
        group_sh["group"] = "category"

        # Conteo de traffic_signals
        df_ts = df_filtered[df_filtered["category"] == "traffic_signals"].copy()
        if not df_ts.empty and "traffic_signals" in df_ts.columns:
            df_ts["mapped"] = df_ts["traffic_signals"].apply(map_traffic_signal)
            group_ts = df_ts.groupby("mapped").size().reset_index(name="count")
            group_ts.rename(columns={"mapped": "value"}, inplace=True)
            group_ts["group"] = "traffic_signals"
        else:
            group_ts = pd.DataFrame(columns=["value", "count", "group"])

        # Conteo de crossings
        df_cr = df_filtered[df_filtered["category"] == "crossing"].copy()
        if not df_cr.empty and "crossing" in df_cr.columns:
            df_cr["mapped"] = df_cr["crossing"].apply(map_crossing)
            group_cr = df_cr.groupby("mapped").size().reset_index(name="count")
            group_cr.rename(columns={"mapped": "value"}, inplace=True)
            group_cr["group"] = "crossing"
        else:
            group_cr = pd.DataFrame(columns=["value", "count", "group"])

        summary_bbox = pd.concat([group_sh, group_ts, group_cr], ignore_index=True)
        summary_bbox["bbox_label"] = bbox_label
        summary_list.append(summary_bbox)

    # Consolidar y pivotar
    df_all = pd.concat(summary_list, ignore_index=True)
    pivot_df = df_all.pivot_table(index="bbox_label", columns=["group", "value"], values="count", fill_value=0)
    pivot_df.columns = [f"{grp}_{val}" for grp, val in pivot_df.columns]
    pivot_df.reset_index(inplace=True)

    # Agregar columnas de ubicación estructurada
    addr_df = pivot_df["bbox_label"].map(bbox_city_map).apply(pd.Series)
    addr_df["city"].fillna("unknown", inplace=True)
    addr_df["county"].fillna("unknown", inplace=True)
    addr_df["state"].fillna("unknown", inplace=True)
    addr_df["postcode"].fillna("", inplace=True)
    pivot_df[["city", "county", "state", "postcode"]] = addr_df[["city", "county", "state", "postcode"]]

    # Reordenar columnas según esquema deseado
    cols_desired = [
        "bbox_label",
        "category_hospital", "category_school",
        "crossing_combinations", "crossing_marked", "crossing_uncontrolled",
        "crossing_unknown", "crossing_unmarked", "crossing_zebra",
        "traffic_signals_bridge", "traffic_signals_emergency",
        "traffic_signals_level_crossing", "traffic_signals_pedestrian_crossing",
        "traffic_signals_ramp_meter", "traffic_signals_signal",
        "traffic_signals_traffic_lights", "traffic_signals_unknown",
        "city", "county", "state", "postcode"
    ]
    # Algunos pivots podrían faltar si no hay datos; aseguramos existencia
    existing_cols = [c for c in cols_desired if c in pivot_df.columns]
    pivot_df = pivot_df[existing_cols]

    # Guardar resultado final
    os.makedirs(processed_folder, exist_ok=True)
    pivot_df.to_csv(final_path, index=False)
    log.info(f"Archivo final guardado en '{final_path}'")


def merge_accidents_with_api():
    log = logging.getLogger(__name__)
    source_engine = create_engine(SOURCE_DB_URL)
    processed_folder = "/opt/airflow/data/processed/"
    try:
        # --- Crear tabla de destino si no existe ---
        with source_engine.begin() as conn:
            log.info("Creando tabla 'accidentes_final' si no existe...")
            create_sql = """
            CREATE TABLE IF NOT EXISTS accidentes_final (
                id INTEGER PRIMARY KEY,
                crash_date TIMESTAMP,
                traffic_control_device TEXT,
                weather_condition TEXT,
                lighting_condition TEXT,
                first_crash_type TEXT,
                trafficway_type TEXT,
                alignment TEXT,
                roadway_surface_cond TEXT,
                road_defect TEXT,
                crash_type TEXT,
                intersection_related CHAR(1),
                damage TEXT,
                prim_contributory_cause TEXT,
                num_units INT,
                most_severe_injury TEXT,
                injuries_total FLOAT,
                injuries_fatal FLOAT,
                injuries_incapacitating FLOAT,
                injuries_non_incapacitating FLOAT,
                injuries_reported_not_evident FLOAT,
                injuries_no_indication FLOAT,
                crash_hour INT,
                crash_day_of_week INT,
                crash_month INT,
                start_lat FLOAT,
                start_lng FLOAT,
                lat_bin FLOAT,
                lng_bin FLOAT,
                bbox_label TEXT,
                category_hospital INT,
                category_school INT,
                crossing_combinations INT,
                crossing_marked INT,
                crossing_uncontrolled INT,
                crossing_unknown INT,
                crossing_unmarked INT,
                crossing_zebra INT,
                traffic_signals_bridge INT,
                traffic_signals_emergency INT,
                traffic_signals_level_crossing INT,
                traffic_signals_pedestrian_crossing INT,
                traffic_signals_ramp_meter INT,
                traffic_signals_signal INT,
                traffic_signals_traffic_lights INT,
                traffic_signals_unknown INT,
                aprox_city TEXT,
                aprox_county TEXT,
                aprox_state TEXT,
                aprox_postcode TEXT
            );
            """
            conn.execute(text(create_sql))

        # --- Cargar datos de accidentes ---
        with source_engine.connect() as conn:
            log.info("Leyendo 'accidentes' desde base de datos...")
            result = conn.execute(text("SELECT * FROM accidentes"))
            rows = result.fetchall()
            cols = result.keys()
            df_acc = pd.DataFrame(rows, columns=cols)

            # --- Leer CSV procesado ---
            log.info("Leyendo CSV de API: combined_bbox_summary_final.csv...")
            api_df = pd.read_csv(
                os.path.join(processed_folder, 'combined_bbox_summary_final.csv'),
                dtype={
                    'category_hospital': 'Int64',
                    'category_school': 'Int64'
                }
            )

            # Rebautizar columnas de ubicación para coincidir
            api_df = api_df.rename(columns={
                'city': 'aprox_city',
                'county': 'aprox_county',
                'state': 'aprox_state',
                'postcode': 'aprox_postcode'
            })

            # Convertir recuentos a enteros (si vienen como floats)
            count_cols = [
                'category_hospital','category_school',
                'crossing_combinations','crossing_marked','crossing_uncontrolled',
                'crossing_unknown','crossing_unmarked','crossing_zebra',
                'traffic_signals_bridge','traffic_signals_emergency',
                'traffic_signals_level_crossing','traffic_signals_pedestrian_crossing',
                'traffic_signals_ramp_meter','traffic_signals_signal',
                'traffic_signals_traffic_lights','traffic_signals_unknown'
            ]
            for c in count_cols:
                if c in api_df.columns:
                    api_df[c] = api_df[c].fillna(0).astype(int)

            # Reemplazar cadenas vacías por None
            api_df[['aprox_city','aprox_county','aprox_state','aprox_postcode']] = \
                api_df[['aprox_city','aprox_county','aprox_state','aprox_postcode']].replace({'': None})

            # --- Preprocesamiento de coordenadas en df_acc ---
            df_acc['start_lat'] = df_acc['start_lat'].astype(float)
            df_acc['start_lng'] = df_acc['start_lng'].astype(float)
            df_acc['lat_bin'] = (df_acc['start_lat'] // 0.5) * 0.5
            df_acc['lng_bin'] = (df_acc['start_lng'] // 0.5) * 0.5
            df_acc['bbox_label'] = 'bbox_' + df_acc['lat_bin'].astype(str) + '_' + df_acc['lng_bin'].astype(str)

            # --- Merge interno ---
            log.info("Realizando INNER JOIN sobre 'bbox_label'...")
            merged = pd.merge(df_acc, api_df, on='bbox_label', how='inner')
            log.info(f"Registros tras merge: {len(merged)}")

            # --- Filtrar nuevos registros ---
            existing = conn.execute(text("SELECT id FROM accidentes_final")).fetchall()
            existing_ids = set(r[0] for r in existing)
            new_df = merged[~merged['id'].isin(existing_ids)].copy()
            log.info(f"Nuevos registros a insertar: {len(new_df)}")
            if new_df.empty:
                return

            # --- Definir orden de columnas ---
            cols_final = [
                'id','crash_date','traffic_control_device','weather_condition','lighting_condition',
                'first_crash_type','trafficway_type','alignment','roadway_surface_cond','road_defect',
                'crash_type','intersection_related','damage','prim_contributory_cause','num_units',
                'most_severe_injury','injuries_total','injuries_fatal','injuries_incapacitating',
                'injuries_non_incapacitating','injuries_reported_not_evident','injuries_no_indication',
                'crash_hour','crash_day_of_week','crash_month','start_lat','start_lng',
                'lat_bin','lng_bin','bbox_label'
            ] + count_cols + [
                'aprox_city','aprox_county','aprox_state','aprox_postcode'
            ]
            new_df = new_df[cols_final]

            # --- Insertar registros ---
            insert_sql = f"INSERT INTO accidentes_final ({', '.join(cols_final)}) " \
                         + "VALUES ({}) ON CONFLICT (id) DO NOTHING".format(
                ', '.join(':'+col for col in cols_final)
            )

            inserted = 0
            for _, row in new_df.iterrows():
                record = row.where(pd.notnull(row), None).to_dict()
                try:
                    conn.execute(text(insert_sql), record)
                    inserted += 1
                except Exception as e:
                    log.warning(f"Error insertando ID {record.get('id')}: {e}")
            log.info(f"Total insertados: {inserted} de {len(new_df)}")

    except Exception as e:
        log.error(f"Error en merge_accidents_with_api: {e}", exc_info=True)
        raise

    finally:
        source_engine.dispose()
        log.info("Conexión finalizada.")



def load_from_accidentes_final():
    log = logging.getLogger(__name__)
    source_engine = dim_engine = None
    source_conn = dim_conn = None

    try:
        source_engine = create_engine(SOURCE_DB_URL)
        dim_engine = create_engine(DIM_DB_URL)
        source_conn = source_engine.connect()
        dim_conn = dim_engine.connect()

        log.info("Verificando si hay datos en accidentes_final...")
        result = source_conn.execute(text("SELECT COUNT(*) FROM accidentes_final"))
        count = result.scalar()
        if count == 0:
            log.info("No hay datos en accidentes_final. Finalizando función.")
            return

        result = dim_conn.execute(text("SELECT COUNT(*) FROM dim_fecha"))
        dim_fecha_count = result.scalar()

        log.info("Consultando registros de accidentes_final...")
        rows = source_conn.execute(text("SELECT * FROM accidentes_final")).fetchall()

        if dim_fecha_count == 0:
            log.info("Insertando dimensiones en lote...")

            dim_conn.execute(text("""
                INSERT INTO dim_fecha (Día, Mes, Año, Día_Semana, Hora)
                VALUES (:dia, :mes, :ano, :dia_semana, :hora)
                ON CONFLICT DO NOTHING
            """), [{
                "dia": row.crash_date.day,
                "mes": row.crash_date.month,
                "ano": row.crash_date.year,
                "dia_semana": row.crash_date.strftime("%A"),
                "hora": row.crash_date.time()
            } for row in rows])

            dim_conn.execute(text("""
                INSERT INTO dim_ubicacion (Latitud, Longitud, Intersección, Approx_City, Approx_County, Approx_State, Approx_Postcode)
                VALUES (:lat, :lng, :inter, :city, :county, :state, :postcode)
                ON CONFLICT DO NOTHING
            """), [{
                "lat": row.start_lat,
                "lng": row.start_lng,
                "inter": row.intersection_related,
                "city": row.aprox_city,
                "county": row.aprox_county,
                "state": row.aprox_state,
                "postcode": row.aprox_postcode
            } for row in rows])

            dim_conn.execute(text("""
                INSERT INTO dim_clima (Condición_Climática)
                VALUES (:cond)
                ON CONFLICT DO NOTHING
            """), [{"cond": row.weather_condition} for row in rows])

            dim_conn.execute(text("""
                INSERT INTO dim_iluminacion (Condición_Iluminación)
                VALUES (:cond)
                ON CONFLICT DO NOTHING
            """), [{"cond": row.lighting_condition} for row in rows])

            dim_conn.execute(text("""
                INSERT INTO dim_contribuyente_principal (Causa_Principal)
                VALUES (:causa)
                ON CONFLICT DO NOTHING
            """), [{"causa": row.prim_contributory_cause} for row in rows])

            dim_conn.execute(text("""
                INSERT INTO dim_condicion_camino (Superficie_Carretera, Defecto_Carretera)
                VALUES (:superf, :defec)
                ON CONFLICT DO NOTHING
            """), [{
                "superf": row.roadway_surface_cond,
                "defec": row.road_defect
            } for row in rows])

            dim_conn.execute(text("""
                INSERT INTO dim_tipo_accidente (Tipo_Primer_Choque, Tipo_Vía, Alineación, Nivel_Lesión)
                VALUES (:choque, :via, :alinea, :lesion)
                ON CONFLICT DO NOTHING
            """), [{
                "choque": row.first_crash_type,
                "via": row.trafficway_type,
                "alinea": row.alignment,
                "lesion": row.most_severe_injury
            } for row in rows])

            dim_conn.execute(text("""
                INSERT INTO dim_infraestructura (
                    BBox_Label, Category_Hospital, Category_School, Crossing_Combinations,
                    Crossing_Marked, Crossing_Uncontrolled, Crossing_Unknown, Crossing_Unmarked,
                    Crossing_Zebra, Traffic_Signals_Bridge, Traffic_Signals_Emergency,
                    Traffic_Signals_Level_Crossing, Traffic_Signals_Pedestrian_Crossing,
                    Traffic_Signals_Ramp_Meter, Traffic_Signals_Signal, Traffic_Signals_Traffic_Lights,
                    Traffic_Signals_Unknown
                ) VALUES (
                    :bbox, :hosp, :school, :comb, :marked, :uncontrolled, :unknown, :unmarked,
                    :zebra, :bridge, :emerg, :level, :pedes, :ramp, :signal, :traffic, :ts_unknown
                ) ON CONFLICT DO NOTHING
            """), [{
                "bbox": row.bbox_label,
                "hosp": row.category_hospital,
                "school": row.category_school,
                "comb": row.crossing_combinations,
                "marked": row.crossing_marked,
                "uncontrolled": row.crossing_uncontrolled,
                "unknown": row.crossing_unknown,
                "unmarked": row.crossing_unmarked,
                "zebra": row.crossing_zebra,
                "bridge": row.traffic_signals_bridge,
                "emerg": row.traffic_signals_emergency,
                "level": row.traffic_signals_level_crossing,
                "pedes": row.traffic_signals_pedestrian_crossing,
                "ramp": row.traffic_signals_ramp_meter,
                "signal": row.traffic_signals_signal,
                "traffic": row.traffic_signals_traffic_lights,
                "ts_unknown": row.traffic_signals_unknown
            } for row in rows])

        log.info("Dimensiones cargadas correctamente desde accidentes_final.")

    except Exception as e:
        log.error(f"Error en load_from_accidentes_final: {str(e)}", exc_info=True)
        raise

    finally:
        if source_conn:
            source_conn.close()
        if dim_conn:
            dim_conn.close()
        if source_engine:
            source_engine.dispose()
        if dim_engine:
            dim_engine.dispose()
        log.info("Conexiones cerradas.")


def insert_hechos_accidentes_optimizado(rows, dim_conn):
    log = logging.getLogger(__name__)
    log.info("Cargando IDs de dimensiones en memoria...")

    def build_dict(query, key_cols, value_col):
        result = dim_conn.execute(text(query)).mappings().all()
        return {
            tuple(row[col] for col in key_cols): row[value_col] for row in result
        }

    fecha_dict = build_dict(
        "SELECT fecha_id, día, mes, año, día_semana, hora FROM dim_fecha",
        ["día", "mes", "año", "día_semana", "hora"], "fecha_id")

    ubicacion_dict = build_dict(
        """SELECT ubicación_id, latitud, longitud, intersección,
                  approx_city, approx_county, approx_state, approx_postcode
           FROM dim_ubicacion""",
        ["latitud", "longitud", "intersección", "approx_city", "approx_county", "approx_state", "approx_postcode"],
        "ubicación_id")

    clima_dict = build_dict(
        "SELECT clima_id, condición_climática FROM dim_clima",
        ["condición_climática"], "clima_id")

    iluminacion_dict = build_dict(
        "SELECT iluminación_id, condición_iluminación FROM dim_iluminacion",
        ["condición_iluminación"], "iluminación_id")

    camino_dict = build_dict(
        "SELECT condición_camino_id, superficie_carretera, defecto_carretera FROM dim_condicion_camino",
        ["superficie_carretera", "defecto_carretera"], "condición_camino_id")

    tipo_dict = build_dict(
        "SELECT tipo_accidente_id, tipo_primer_choque, tipo_vía, alineación, nivel_lesión FROM dim_tipo_accidente",
        ["tipo_primer_choque", "tipo_vía", "alineación", "nivel_lesión"], "tipo_accidente_id")

    causa_dict = build_dict(
        "SELECT contribuyente_principal_id, causa_principal FROM dim_contribuyente_principal",
        ["causa_principal"], "contribuyente_principal_id")

    infraestructura_dict = build_dict(
        "SELECT infraestructura_id, bbox_label FROM dim_infraestructura",
        ["bbox_label"], "infraestructura_id")

    log.info("Construyendo registros para hechos_accidentes...")
    hechos_batch = []

    for row in rows:
        hechos_batch.append({
            "id": row.id,
            "fecha_id": fecha_dict.get((
                row.crash_date.day,
                row.crash_date.month,
                row.crash_date.year,
                row.crash_date.strftime("%A"),
                row.crash_date.time()
            )),
            "ubicacion_id": ubicacion_dict.get((
                row.start_lat, row.start_lng, row.intersection_related,
                row.aprox_city, row.aprox_county, row.aprox_state, row.aprox_postcode
            )),
            "clima_id": clima_dict.get((row.weather_condition,)),
            "iluminacion_id": iluminacion_dict.get((row.lighting_condition,)),
            "condicion_camino_id": camino_dict.get((row.roadway_surface_cond, row.road_defect)),
            "tipo_accidente_id": tipo_dict.get((
                row.first_crash_type, row.trafficway_type,
                row.alignment, row.most_severe_injury
            )),
            "contribuyente_id": causa_dict.get((row.prim_contributory_cause,)),
            "infraestructura_id": infraestructura_dict.get((row.bbox_label,)),
            "unidades": row.num_units,
            "total_lesiones": row.injuries_total,
            "fatalidades": row.injuries_fatal,
            "incapacitantes": row.injuries_incapacitating,
            "no_incapacitantes": row.injuries_non_incapacitating,
            "reportadas_no_evidentes": row.injuries_reported_not_evident,
            "sin_indicacion": row.injuries_no_indication
        })

    log.info(f"Insertando {len(hechos_batch)} registros en hechos_accidentes...")
    dim_conn.execute(text("""
        INSERT INTO hechos_accidentes (
            id_hecho, fecha_id, ubicación_id, clima_id, iluminación_id,
            condición_camino_id, tipo_accidente_id, contribuyente_principal_id,
            infraestructura_id,
            unidades_involucradas, total_lesiones, fatalidades, incapacitantes,
            no_incapacitantes, reportadas_no_evidentes, sin_indicación
        ) VALUES (
            :id, :fecha_id, :ubicacion_id, :clima_id, :iluminacion_id,
            :condicion_camino_id, :tipo_accidente_id, :contribuyente_id,
            :infraestructura_id,
            :unidades, :total_lesiones, :fatalidades, :incapacitantes,
            :no_incapacitantes, :reportadas_no_evidentes, :sin_indicacion
        )
        ON CONFLICT (id_hecho) DO NOTHING
    """), hechos_batch)

    log.info("Carga optimizada de hechos_accidentes finalizada.")


def task_insert_hechos_accidentes_optimizado():
    log = logging.getLogger(__name__)
    source_engine = dim_engine = None
    source_conn = dim_conn = None
    try:
        # Crear conexiones
        source_engine = create_engine(SOURCE_DB_URL)
        dim_engine = create_engine(DIM_DB_URL)
        source_conn = source_engine.connect()
        dim_conn = dim_engine.connect()

        # Obtener datos
        log.info("Extrayendo registros de accidentes_final...")
        rows = source_conn.execute(text("SELECT * FROM accidentes_final")).fetchall()
        if not rows:
            log.info("No hay registros para insertar.")
            return

        # Llamar a la función optimizada
        insert_hechos_accidentes_optimizado(rows, dim_conn)

    except Exception as e:
        log.error(f"Error en task_insert_hechos_accidentes_optimizado: {str(e)}", exc_info=True)
        raise
    finally:
        if source_conn:
            source_conn.close()
        if dim_conn:
            dim_conn.close()
        if source_engine:
            source_engine.dispose()
        if dim_engine:
            dim_engine.dispose()
        log.info("Conexiones cerradas en insert_hechos.")

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

    task_transform_accidents = PythonOperator(
        task_id='transform_accidents',
        python_callable=transform 
    )

    task_api_extract = PythonOperator(
        task_id='api_extractation',
        python_callable=extract_bbox_osm
    )

    task_api_transform = PythonOperator(
        task_id='api_transform',
        python_callable=transform_bbox_data
    )

    task_merge = PythonOperator(
        task_id='merge',
        python_callable=merge_accidents_with_api,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS 
    )

    task_load = PythonOperator(
        task_id='load_dimensiones',
        python_callable=load_from_accidentes_final,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  
    )

    task_insert_hechos = PythonOperator(
        task_id='load_hechos',
        python_callable=task_insert_hechos_accidentes_optimizado,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  
    )


    # Flujo del DAG
    task_setup_tables >> [task_transform_accidents, task_api_extract]
    task_api_extract >> task_api_transform
    [task_transform_accidents, task_api_transform] >> task_merge
    task_merge >> task_load >> task_insert_hechos