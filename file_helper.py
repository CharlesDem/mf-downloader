import os
from pathlib import Path
import shutil
import time
from dotenv import load_dotenv
from minio import S3Error
import requests
import structlog
from discord_alerter import discord_error
from minio_config import minio_client, ensure_bucket, bufr_bucket
import tarfile

load_dotenv()
SCRIPT_DIR = Path(__file__).resolve().parent


stations_ids = ['36','37','38','40','41','42','43','44','45','47','49','50','51','52','53','54','55','56','57','59','61','62','63','65','66','67','68','69']

URL = "https://public-api.meteofrance.fr/public/DPPaquetRadar/v1/station/paquet?id_station="
apikey = os.getenv("MF_APIKEY", "")

log = structlog.get_logger()

headers = {
    "accept": "application/octet-stream+gzip",
    "apikey": apikey
}

MAX_RETRIES = 5
RETRY_DELAY = 5


def dl_file_to_s3(file: str) -> None:

    ensure_bucket(minio_client, bufr_bucket)

    minio_client.fput_object(
        bucket_name=bufr_bucket,
        object_name=file,
        file_path=str(file),
    )

    log.info(f"File {file} uploaded to {bufr_bucket}")

def dl_pagb_all(temp: str):

    for station_id in stations_ids:
        try:
            files_to_upload = dl_file_to_local_temp(URL, temp, station_id)

            for file in files_to_upload:
                dl_file_to_s3(file)

        except Exception as e:
            log.error(f"Unmanaged error {e}")

        finally:
            try:
                del_temp_files(temp)
                log.info(f"Directory and files removed: {temp}")
            except Exception as cleanup_error:
                log.error(f"Cleanup failed: {cleanup_error}")


def dl_file_to_local_temp(url: str, path: str, station_id: str) -> list[str]:

    url_with_station = f"{url}{station_id}"

    target_dir = SCRIPT_DIR / path
    target_dir.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:

            with requests.get(url_with_station, stream=True, timeout=10, headers=headers) as r:
                r.raise_for_status()

                file = f"{path}/archive.gz"
                with open(file, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

                    log.info(f"Package radar downloaded for station {station_id}, file {file} created")

                    extract(file, path)
                    log.info(f"File {file} unzipped")

                    files_to_save = find_files(f"T_PAGB", path)
                    for file_to_save in files_to_save:
                        log.info(f"/File to save in storage S3 : {file_to_save}")

                    return files_to_save

        except (requests.RequestException, S3Error) as e:

            log.error("Download_failed", attempt=attempt, error=str(e))

            if attempt == MAX_RETRIES:
                log.error("Giving_up", attempts=MAX_RETRIES)
                discord_error(f"Error while dowloading or sending to minio for data station {station_id}, retry depleted")
                return []

            time.sleep(RETRY_DELAY)
    return []

def find_files(pattern: str, dir: str) -> list[str]:
    return [f"{dir}/{file}" for file in os.listdir(dir) if pattern in file]

    
def del_temp_files(target_dir: str) -> None:
    path = Path(target_dir)
    if path.exists():
        shutil.rmtree(path)

def extract(archive_file: str, dir: str):
    with tarfile.open(archive_file, "r:gz") as tar:
        tar.extractall(f"./{dir}")
