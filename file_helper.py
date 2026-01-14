from datetime import datetime
import os
from dotenv import load_dotenv
import requests
import structlog

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
        

def dest_path(target_dir: str) -> str:
    return f"{target_dir}/radar-moz-metr-{datetime.now().strftime("%Y-%m-%d--%H-%M-%S")}.gz"