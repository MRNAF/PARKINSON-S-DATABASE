import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'bio.db'


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    conn.execute('PRAGMA busy_timeout = 30000')
    return conn


def init_database():
    from models.metadata import CREATE_TABLE_SQL as METADATA_SQL
    from models.variants import CREATE_TABLE_SQL as VARIANTS_SQL, CREATE_INDEX_SQL as VARIANTS_INDEX_SQL
    from models.genotypes import CREATE_TABLE_SQL as GENOTYPES_SQL, CREATE_INDEXES_SQL as GENOTYPES_INDEXES_SQL

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(METADATA_SQL)
        cur.execute(VARIANTS_SQL)
        cur.execute(VARIANTS_INDEX_SQL)
        cur.execute(GENOTYPES_SQL)
        for sql in GENOTYPES_INDEXES_SQL:
            cur.execute(sql)
