from dataclasses import dataclass
from dotenv import load_dotenv
from minio import Minio
import os

import urllib3

load_dotenv()

@dataclass
class MinioConfig:
    bufr_bucket: str = "raw-bufr"

    def __post_init__(self):
        self.minio_client = Minio(
            endpoint="mf-minio:9000",
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            secure=False,
            # http_client=self._get_http_client(),
        )

    # def _get_http_client(self) -> urllib3.PoolManager:
    #     return urllib3.PoolManager(
    #         cert_reqs="CERT_REQUIRED",
    #         ca_certs="/etc/ssl/certs/ca-certificates.crt",
    #         assert_hostname=False,
    #     )

    def ensure_bucket(self, bucket: str) -> None:
        if not self.minio_client.bucket_exists(bucket):
            self.minio_client.make_bucket(bucket)


@dataclass
class DiscordConfig():
    web_hook = os.environ["WEBHOOK_DISCORD"]

@dataclass
class DownloaderConfig():
    stations_ids = ['36','37','38','40','41','42','43','44','45','47','49','50','51','52','53','54','55','56','57','59','61','62','63','65','66','67','68','69']
    url = "https://public-api.meteofrance.fr/public/DPPaquetRadar/v1/station/paquet?id_station="
    apikey = os.getenv("MF_APIKEY", "")
    headers = {
        "accept": "application/octet-stream+gzip",
        "apikey": apikey
    }
    max_retries = 5
    retry_delay = 5

minio_config = MinioConfig()
discord_config = DiscordConfig()
dl_config = DownloaderConfig()

