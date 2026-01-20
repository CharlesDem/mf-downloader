from collections.abc import Iterable
import re
import pandas as pd
from bufr_decoder import deco_bufr
from radar_models import RadarData, RadarPointDoppler, RadarPointReflectivity


def radar_points_reflectivity_to_df(points: Iterable[RadarPointReflectivity]):
    return pd.DataFrame(p.to_dict() for p in points)


def radar_points_doppler_to_df(points: Iterable[RadarPointDoppler]):
    return pd.DataFrame(p.to_dict() for p in points)


def extract_data_from_bufr(file: str):

    m  = re.search(r"PAGB(\d+)", file)

    if m is None:
        raise ValueError(f"Numéro de station introuvable dans le nom de fichier: {file}")

    num_station: int = int(m.group(1))

    reflectivity: RadarData
    doppler: RadarData

    reflectivity, doppler = deco_bufr(file)

    reflectivity_points = reflectivity.radar_data_to_reflectivity_points()
    doppler_points = doppler.radar_data_to_doppler_points()

    date = reflectivity.timestamp.strftime("%Y-%m-%d-%H-%M-%S")

    df_ref = radar_points_reflectivity_to_df(reflectivity_points)
    df_dop = radar_points_doppler_to_df(doppler_points)


    res_ref =df_ref["reflectivity_dbz"].value_counts().sort_index()
    res_dop = df_dop["radial_velocity_ms"].value_counts().sort_index()
    print("ref après filtre, ", res_ref.to_string())
    print("doppler après filtre", res_dop.to_string())

    df_ref["id_station"] = num_station
    df_ref["timestamp"] = pd.to_datetime(df_ref["timestamp"], utc=True)
    df_ref = df_ref.astype({ #type to keep parquet storage efficient
        "x_m": "float32",
        "y_m": "float32",
        "z_m": "float32",
        "lat": "float32",
        "long": "float32",
        "altitude_m": "float32",
        "reflectivity_dbz": "float32",
        "id_station": "int16",
    })
    df_ref.to_parquet(
        f"test/reflectivity_{date}_station_{num_station}.parquet",
        engine="pyarrow",
        compression="zstd",
        index=False,
    )

    df_dop["id_station"] = num_station
    df_dop["timestamp"] = pd.to_datetime(df_dop["timestamp"], utc=True)
    df_dop = df_dop.astype({
        "x_m": "float32",
        "y_m": "float32",
        "z_m": "float32",
        "lat": "float32",
        "long": "float32",
        "altitude_m": "float32",
        "radial_velocity_ms": "float32",
        "id_station": "int16",
    })
    df_ref.to_parquet(
        f"test/doppler_{date}_station_{num_station}.parquet",
        engine="pyarrow",
        compression="zstd",
        index=False,
    )

