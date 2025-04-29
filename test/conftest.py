# tests/conftest.py
import pytest
import pandas as pd
from types import SimpleNamespace

@pytest.fixture
def sample_accident_rows():
    from datetime import datetime
    return [
        (
            1,
            datetime(2025,1,1,0,0),
            1.0, 2.0, True,
            "Sunny", "Daylight",
            "Dry","None",
            "Front","Highway","Straight","Minor",
            "Rain", 2, 3, 0,1,0,0,0
        )
    ]

@pytest.fixture
def fake_src_engine(mocker, sample_accident_rows):
    # Crea un engine cuyo conn.execute().fetchall() devuelva sample_accident_rows
    fake_conn = SimpleNamespace()
    fake_conn.execute = lambda q: SimpleNamespace(fetchall=lambda: sample_accident_rows)
    fake_conn.close = lambda: None
    fake_engine = SimpleNamespace(connect=lambda: fake_conn, dispose=lambda: None)
    mocker.patch("dags.etl_crash_traffic.create_engine", return_value=fake_engine)
    return fake_engine

@pytest.fixture
def sample_tags_df(tmp_path):
    df = pd.DataFrame({
        "tags": ['{"category":"school"}', '{"category":"hospital"}'],
        "category": ["school", "hospital"]
    })
    path = tmp_path / "bbox_test.csv"
    df.to_csv(path, index=False)
    return path
