from dataclasses import dataclass
from datetime import datetime
import math
from typing import Any
from _collections_abc import Generator


@dataclass(slots=True)
class RadarPointReflectivity:
    """ A class to store a point with reflectivity value and convert it to dict"""

    timestamp: datetime
    x_m: float  # coordonnée x du point centrée sur le radar(0.0,0.0) généralement de -40000 m à +40000m
    y_m: float  # coordonnée y
    z_m: float  # élévation faisceau
    lat: float  # latitude du point (approximation pas projetée sur la courbure terrestre encore, fonctionne pour les platistes)
    long: float # longitude approximative, idem
    altitude_m: float # altitude niveau de la mer, élévation + altitude du radar
    reflectivity_dbz: float # indice de reflectivité radar (non normalisé, chaque radar a sa propre échelle)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "x_m": self.x_m,
            "y_m": self.y_m,
            "z_m": self.z_m,
            "lat": self.lat,
            "long": self.long,
            "altitude_m": self.altitude_m,
            "reflectivity_dbz": self.reflectivity_dbz,
        }


@dataclass(slots=True)
class RadarPointDoppler:
    """ A class to store a point with doppler radial velocity value and convert it to dict"""

    timestamp: datetime
    x_m: float  # coordonnée x du point centrée sur le radar(0.0,0.0) généralement de -40000 m à +40000m
    y_m: float  # coordonnée y
    z_m: float  # élévation faisceau
    lat: float  # latitude du point (approximation pas projetée sur la courbure terrestre encore, fonctionne pour les platistes)
    long: float # longitude approximative, idem
    altitude_m: float # altitude niveau de la mer, élévation + altitude du radar
    radial_velocity_ms: float # vélocité radiale doppler éloignement par rapport au radar

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "x_m": self.x_m,
            "y_m": self.y_m,
            "z_m": self.z_m,
            "lat": self.lat,
            "long": self.long,
            "altitude_m": self.altitude_m,
            "radial_velocity_ms": self.radial_velocity_ms,
        }


@dataclass(slots=True)
class RadarData:
    """ A class to store global BUFR data and convert it as a list of doppler points or a list of reflectivity points"""
    timestamp: datetime
    latitude_deg: float
    longitude_deg: float
    altitude_m: float

    elevation_deg: float
    azimuth_start_deg: float
    azimuth_step_deg: float

    n_azimuths: int
    n_ranges: int
    range_gate_m: float

    pixel_indices: list[int]
    value_lut: list[float]

    doppler_vmin: float # min speed for doppler
    doppler_step: float

    product: str  # "reflectivity" | "doppler"

    def radar_data_to_doppler_points(self) -> Generator[RadarPointDoppler, None, None]:

        elev_rad = math.radians(self.elevation_deg)

        for az_i in range(self.n_azimuths):
            az_rad = math.radians(az_i * self.azimuth_step_deg)

            for r_i in range(self.n_ranges):
                idx = az_i * self.n_ranges + r_i
                if idx >= len(self.pixel_indices):
                    continue

                pix = self.pixel_indices[idx]
                if pix <= 0 or pix >= len(self.value_lut):
                    continue

                value = self.value_lut[pix]

                if value < self.doppler_vmin - self.doppler_step / 2 or value > abs(self.doppler_vmin) + self.doppler_step /2:  # filter invalid values vi​=vmin​+i⋅Δv
                    continue

                r = r_i * self.range_gate_m

                x = r * math.cos(elev_rad) * math.sin(az_rad)
                y = r * math.cos(elev_rad) * math.cos(az_rad)
                z = r * math.sin(elev_rad)
                altitude = self.altitude_m + z

                lat, lon = enu_to_latlon(
                    x, y, self.latitude_deg, self.longitude_deg
                )

                yield RadarPointDoppler(
                    self.timestamp,
                    x, y, z,
                    lat, lon,
                    altitude,
                    value,
                )


    def radar_data_to_reflectivity_points(self) -> Generator[RadarPointReflectivity, None, None]:

        elev_rad = math.radians(self.elevation_deg)

        for az_i in range(self.n_azimuths):
            az_rad = math.radians(az_i * self.azimuth_step_deg)

            for r_i in range(self.n_ranges):
                idx = az_i * self.n_ranges + r_i
                if idx >= len(self.pixel_indices):
                    continue

                pix = self.pixel_indices[idx]
                if pix <= 0 or pix >= len(self.value_lut):
                    continue

                value = self.value_lut[pix]
                r = r_i * self.range_gate_m

                x = r * math.cos(elev_rad) * math.sin(az_rad)
                y = r * math.cos(elev_rad) * math.cos(az_rad)
                z = r * math.sin(elev_rad)
                altitude = self.altitude_m + z

                lat, lon = enu_to_latlon(
                    x, y, self.latitude_deg, self.longitude_deg
                )

                yield RadarPointReflectivity(
                    timestamp=self.timestamp,
                    x_m=x,
                    y_m=y,
                    z_m=z,
                    lat=lat,
                    long=lon,
                    altitude_m=altitude,
                    reflectivity_dbz=value,
                )






R = 6371000.0  # rayon moyen Terre en mètres

def enu_to_latlon(x: float, y: float, lat0_deg: float, lon0_deg: float):
    lat0 = math.radians(lat0_deg)
    lon0 = math.radians(lon0_deg)

    dlat = y / R
    dlon = x / (R * math.cos(lat0))

    lat = lat0 + dlat
    lon = lon0 + dlon

    return math.degrees(lat), math.degrees(lon)
