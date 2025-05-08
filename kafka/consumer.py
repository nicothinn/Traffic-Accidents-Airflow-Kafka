from kafka import KafkaConsumer
import json
import pandas as pd

# Temas que escuchar
topics = [
    'transit_signals_by_state',
    'signals_vs_lesions',
    'weather_light_surface',
    'accidents_by_time',
    'lesions_by_county',
    'hospitals_schools_vs_lesions',
    'crossings_vs_lesions'
]

# Inicializar el consumidor
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers='localhost:9092',  # Cambia si usas otro host
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='dashboard-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Diccionario donde guardar los resultados por topic
data_by_topic = {topic: [] for topic in topics}

# Recibir un mensaje por cada topic
for message in consumer:
    topic = message.topic
    data = message.value
    data_by_topic[topic] = data  # Se espera que cada mensaje ya sea una lista de dicts

    if all(data_by_topic[t] for t in topics):
        break  # Una vez recibido todo, salir del ciclo

# Convertir a DataFrames
dataframes = {t: pd.DataFrame(data_by_topic[t]) for t in topics}

# Ejemplo: mostrar uno
print("Ejemplo de dataframe para 'signals_vs_lesions':")
print(dataframes['signals_vs_lesions'].head())
