import atexit
from contextlib import AbstractContextManager
from psycopg_pool import ConnectionPool
from psycopg import Connection
from common.config.postgres_config import postgres_conf


class PostgresPool:

    pool: ConnectionPool | None = None

    @staticmethod
    def __init_pool():
        if PostgresPool.pool is None:
            db_url: str = postgres_conf.database_url
            PostgresPool.pool = ConnectionPool(db_url, max_size=5, min_size=1)
            atexit.register(lambda: PostgresPool.pool.close())
            
    @staticmethod
    def get_conn() -> AbstractContextManager[Connection]:
        if PostgresPool.pool is None:
            PostgresPool.__init_pool()
        assert PostgresPool.pool is not None
        return PostgresPool.pool.connection()

    @staticmethod
    def close() -> None:
        """Ferme le pool si ouvert"""
        if PostgresPool.pool is not None:
            PostgresPool.pool.close()
            PostgresPool.pool = None