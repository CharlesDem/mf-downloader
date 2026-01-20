from datetime import datetime
from bufr_parser.radar_models import RadarData

EPS = 1e-9


def test_radar_quadrant_orientation():
    radar = RadarData(
        timestamp=datetime(2026, 1, 1),
        latitude_deg=0.0,
        longitude_deg=0.0,
        altitude_m=0.0,
        elevation_deg=0.0,
        azimuth_start_deg=0.0,
        azimuth_step_deg=45.0,
        n_azimuths=8,
        n_ranges=2,
        range_gate_m=1000.0,
        pixel_indices=[
            0, 1,  # az = 0
            0, 1,  # 45
            0, 1,  # 90
            0, 1,  # 135
            0, 1,  # 180
            0, 1,  # 225
            0, 1,  # 270
            0, 1,  # 315
        ],
        value_lut=[0.0, 1.0],
        doppler_vmin=0.0,
        doppler_step=0.0,
        product="reflectivity",
    )

    points = [
        p for p in radar.radar_data_to_reflectivity_points()
        if p.x_m != 0 or p.y_m != 0
    ]

    # keep quadrants
    quadrant_points = {
        "NE": points[1],   # 45°
        "SE": points[3],   # 135°
        "SW": points[5],   # 225°
        "NW": points[7],   # 315°
    }

    ne = quadrant_points["NE"]
    se = quadrant_points["SE"]
    sw = quadrant_points["SW"]
    nw = quadrant_points["NW"]

    assert ne.x_m > 0 and ne.y_m > 0
    assert se.x_m > 0 and se.y_m < 0
    assert sw.x_m < 0 and sw.y_m < 0
    assert nw.x_m < 0 and nw.y_m > 0
