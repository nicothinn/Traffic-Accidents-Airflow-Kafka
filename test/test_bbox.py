# tests/test_etl_bbox.py
import pandas as pd
import pytest
from dags.etl_crash_traffic import map_traffic_signal, map_crossing, parse_approx_city, transform_bbox_data

def test_map_traffic_signal_known():
    assert map_traffic_signal("signal") == "signal"
    assert map_traffic_signal(" TRAFFIC_lights ") == "traffic_lights"

def test_map_crossing():
    assert map_crossing("marked;unmarked") == "combinations"
    assert map_crossing("Zebra") == "zebra"

def test_parse_approx_city_full():
    addr = "123 Main St, Smalltown, Smallcounty, Somestate, 54321"
    parsed = parse_approx_city(addr)
    assert parsed["city"] == "Smalltown"
    assert parsed["postcode"] == "54321"

def test_transform_bbox_data(tmp_path, mocker, sample_tags_df):
    # Redirige las carpetas
    mocker.patch("dags.etl_crash_traffic.raw_folder", str(tmp_path))
    mocker.patch("dags.etl_crash_traffic.processed_folder", str(tmp_path))
    # Crea el archivo CSV
    # Parchea glob, pandas.read_csv y el geocoder
    mocker.patch("glob.glob", lambda p: [str(sample_tags_df)])
    mocker.patch("pandas.read_csv", lambda f: pd.read_csv(str(sample_tags_df)))
    fake_loc = SimpleNamespace(address="C1,C2,C3,12345")
    mocker.patch("dags.etl_crash_traffic.RateLimiter", lambda fn, **kw: lambda *args, **k: fake_loc)
    # Corre y comprueba salida
    transform_bbox_data()
    out = tmp_path / "combined_bbox_summary_final.csv"
    assert out.exists()
    df_out = pd.read_csv(out)
    assert "category_school" in df_out.columns
