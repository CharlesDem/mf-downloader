from minio import Minio
import os

minio_client = Minio(
    endpoint="minio:9000",
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False,
)

bufr_bucket = "raw-bufr"

def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)