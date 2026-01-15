from dotenv import load_dotenv
from minio import Minio
import os

import urllib3

load_dotenv()

http_client = urllib3.PoolManager(
    cert_reqs="CERT_REQUIRED",
    ca_certs="/etc/ssl/certs/ca-certificates.crt",
    assert_hostname=False,
)

minio_client = Minio(
    endpoint="mf-minio:9000",
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=True,
    http_client=http_client,
)

bufr_bucket = "raw-bufr"

def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)