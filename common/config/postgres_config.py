from dataclasses import dataclass
import os

@dataclass
class PostgresConfig:
    user: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    host: str = os.getenv("POSTGRES_CONTAINER_NAME", "mf-db")
    port: str = os.getenv("POSTGRES_CONTAINER_NAME", 5432)
    dbname: str = os.getenv("POSTGRES_DB", "silver_db")
    database_url: str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

postgres_conf = PostgresConfig()