# tests/test_etl_merge.py
import pandas as pd
from dags.etl_crash_traffic import merge_accidents_with_api

def test_merge_calls(mocker):
    df_acc = pd.DataFrame({"start_lat":[1.0],"start_lng":[2.0],"foo":[0]})
    mocker.patch("dags.etl_crash_traffic.pd.read_sql", return_value=df_acc)
    df_api = pd.DataFrame({
        "bbox_label":["1.0_2.0"],
        "category_school":[1],"city":["X"],"county":["Y"],"state":["Z"],"postcode":["000"]
    })
    mocker.patch("dags.etl_crash_traffic.pd.read_csv", return_value=df_api)
    fake_conn = mocker.Mock()
    fake_engine = mocker.Mock(connect=lambda: fake_conn, dispose=lambda: None)
    mocker.patch("dags.etl_crash_traffic.create_engine", return_value=fake_engine)

    merge_accidents_with_api()
    fake_conn.to_sql.assert_called_with("accidentes_final", fake_conn, if_exists="replace", index=False)
