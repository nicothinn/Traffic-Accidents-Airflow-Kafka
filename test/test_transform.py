# tests/test_etl_transform.py
import pytest
from dags.etl_crash_traffic import transform

def test_transform_empty(mocker, fake_src_engine):
    # Si la query de accidentes devuelve [], debe dar lista vacía
    fake_src_engine.connect().execute = lambda q: SimpleNamespace(fetchall=lambda: [])
    result = transform()
    assert result == []

def test_transform_single_row(mocker, fake_src_engine, sample_accident_rows):
    # Prepara también los selects de las dim_*
    conn = fake_src_engine.connect()
    conn.execute = pytest.helpers.side_effect = [
        [(100,1,1,2025,"Wed","00:00:00")],  # dim_fecha
        [(200,1.0,2.0,True)],               # dim_ubicacion
        [(10,"Sunny")],                     # dim_clima
        [(20,"Daylight")],                  # dim_iluminacion
        [(30,"Dry","None")],                # dim_condicion_camino
        [(40,"Front","Highway","Straight","Minor")],  # dim_tipo_accidente
        [(50,"Rain")],                      # dim_contribuyente_principal
        sample_accident_rows                # la consulta a accidentes
    ]
    batches = transform()
    assert len(batches) == 1
    row = batches[0][0]
    assert row["fecha_id"] == 100
    assert row["ubicacion_id"] == 200
