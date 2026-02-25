from datetime import datetime
from psycopg import Cursor

from common.db.postgres_lib import db

def fetch_as_scalar(cursor: Cursor):
    value = cursor.fetchone()
    return value


def fetch_as_dicts(cursor: Cursor):
    columns = [desc.name for desc in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]

CREATE_FILE_META_QUERY = """
                        INSERT INTO bufr_file_index(path, parsing_nb, station_id, created_at)
                        VALUEs (%s, %s, %s, %s)
                        RETURNING bufr_file_index_id
                    """

DELETE_FILE_META_QUERY = """
                        DELETE FROM bufr_file_index WHERE bufr_file_index_id = %s
                    """

@db
def create_file_metadata(cursor: Cursor, path: str, station_id: int, timestamp = datetime.now()) -> str:
    cursor.execute(CREATE_FILE_META_QUERY, (path, 0, station_id , timestamp))

@db
def delete_file_metadata(cursor: Cursor, id: str):
    cursor.execute(DELETE_FILE_META_QUERY, (id,))