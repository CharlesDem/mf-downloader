from datetime import datetime
import os
import time
from typing import BinaryIO, cast
from dotenv import load_dotenv
from minio import S3Error
import requests
import structlog
from minio_config import minio_client, ensure_bucket, bufr_bucket


load_dotenv()

URL = "https://public-api.meteofrance.fr/public/DPRadar/v1/mosaiques/METROPOLE/observations/REFLECTIVITE/produit?maille=1000"
apikey = os.getenv("MF_APIKEY", "")

log = structlog.get_logger()

headers = {
    "accept": "application/octet-stream+gzip",
    "apikey": apikey
}

def dl_file(url: str):
    dest_file = dest_path("compressed")

    r = requests.get(url, stream=True, timeout=25, headers=headers)
    r.raise_for_status()
    with open(dest_file, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
        log.info(f"File gz downloaded from mf api: {dest_file}")
        

MAX_RETRIES = 5
RETRY_DELAY = 20


def dl_file_to_s3(url: str) -> None:

    ensure_bucket(minio_client, bufr_bucket)

    for attempt in range(1, MAX_RETRIES + 1):
        try:

            r = requests.get(url, stream=True, timeout=20, headers=headers)
            r.raise_for_status()

            data = cast(BinaryIO, r.raw)
            dest_name = dest_path("")

            minio_client.put_object(
                bucket_name=bufr_bucket,
                object_name=dest_name,
                data=data,
                length=-1,
                part_size=10 * 1024 * 1024,
            )

            log.info("file_uploaded_to_minio", object_name=dest_name)
            return

        except (requests.RequestException, S3Error) as e:
            log.error("upload_failed", attempt=attempt, error=str(e))

            if attempt == MAX_RETRIES:
                log.error("giving_up", attempts=MAX_RETRIES)
                return

            time.sleep(RETRY_DELAY)

def dest_path(target_dir: str) -> str:
    return f"{target_dir}/radar-moz-metr-{datetime.now().strftime("%Y-%m-%d--%H-%M-%S")}.gz"