import traceback
from typing import Any, Callable
import psycopg
from common.db.postgres_pool import PostgresPool

def db_execute(fn: Callable[..., Any], *args: Any)-> None :
    try:
        with PostgresPool.get_conn() as conn:
            with conn.cursor() as cursor:
                return fn(cursor, *args)
    except psycopg.errors.SyntaxError as e:
        print("error sql", e)
    except psycopg.errors.UniqueViolation as e:
        print("Violation unique error", e)
    except psycopg.errors.OperationalError as e:
        print("Connection issue", e)
    except Exception as e:
        print("other error", e)
        traceback.print_exc()

def db(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any):
        return db_execute(func, *args, **kwargs)
    return wrapper