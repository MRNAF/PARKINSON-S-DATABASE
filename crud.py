import sqlite3
import csv

from models.metadata import (
    TABLE_NAME as METADATA_TABLE,
    COLUMNS as METADATA_COLUMNS,
    CREATE_TABLE_SQL as METADATA_CREATE_SQL
)
from models.variants import (
    TABLE_NAME as VARIANTS_TABLE,
    COLUMNS as VARIANTS_COLUMNS,
    CREATE_TABLE_SQL as VARIANTS_CREATE_SQL,
    CREATE_INDEX_SQL as VARIANTS_CREATE_INDEX_SQL
)
from models.genotypes import (
    TABLE_NAME as GENOTYPES_TABLE,
    CREATE_TABLE_SQL as GENOTYPES_CREATE_SQL,
    CREATE_INDEXES_SQL as GENOTYPES_CREATE_INDEXES_SQL
)

from schemas.metadata import (
    validate_metadata_form,
    validate_metadata_csv_columns,
    normalize_metadata_row
)
from schemas.variants import (
    validate_variants_csv_columns,
    normalize_variant_row,
    ALLOWED_SORT_COLUMNS as VARIANTS_ALLOWED_SORT_COLUMNS
)
from schemas.genotypes import (
    validate_genotypes_csv_columns,
    normalize_genotype_row
)

DB_PATH = 'bio_db_project/bio.db'

METADATA_ALLOWED_SORT_COLUMNS = ['iid', 'phenotype', 'sex', 'age']


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_metadata_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(METADATA_CREATE_SQL)
    conn.commit()
    conn.close()


def ensure_variants_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(VARIANTS_CREATE_SQL)
    cur.execute(VARIANTS_CREATE_INDEX_SQL)
    conn.commit()
    conn.close()


def ensure_genotypes_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(GENOTYPES_CREATE_SQL)
    for sql in GENOTYPES_CREATE_INDEXES_SQL:
        cur.execute(sql)
    conn.commit()
    conn.close()


def get_existing_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
    """)
    tables = {row[0] for row in cur.fetchall()}

    conn.close()
    return tables


def get_table_columns(table_name):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cur.fetchall()]

    conn.close()
    return columns


def create_metadata(data_dict):
    ensure_metadata_table()

    data, error = validate_metadata_form(data_dict)
    if error:
        return False, error

    values = tuple(data[column] for column in METADATA_COLUMNS)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"INSERT INTO {METADATA_TABLE} VALUES (?, ?, ?, ?)",
            values
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return False, "Ошибка: iid уже существует"

    conn.close()
    return True, "Запись добавлена"


def get_metadata_by_iid(iid):
    ensure_metadata_table()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {METADATA_TABLE} WHERE iid = ?", (iid,))
    row = cur.fetchone()

    conn.close()
    return row


def update_metadata(iid, data_dict):
    ensure_metadata_table()

    data, error = validate_metadata_form(data_dict)
    if error:
        return False, error

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"""
        UPDATE {METADATA_TABLE}
        SET phenotype = ?, sex = ?, age = ?
        WHERE iid = ?
    """, (
        data['phenotype'],
        data['sex'],
        data['age'],
        iid
    ))

    conn.commit()
    conn.close()

    return True, "Запись обновлена"


def delete_metadata(iid):
    ensure_metadata_table()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"DELETE FROM {METADATA_TABLE} WHERE iid = ?", (iid,))

    conn.commit()
    conn.close()

    return True, "Запись удалена"


def init_metadata_from_csv(csv_path):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    cur.execute("PRAGMA foreign_keys = OFF")

    ensure_genotypes_table()
    cur.execute(f"DELETE FROM {GENOTYPES_TABLE}")
    cur.execute(f"DROP TABLE IF EXISTS {METADATA_TABLE}")
    cur.execute(METADATA_CREATE_SQL)

    with open(csv_path) as file:
        reader = csv.DictReader(file)

        ok, error = validate_metadata_csv_columns(reader.fieldnames)
        if not ok:
            conn.close()
            return False, error

        rows_to_insert = []

        for row in reader:
            normalized, error = normalize_metadata_row(row)
            if error:
                conn.close()
                return False, error

            rows_to_insert.append(tuple(normalized[column] for column in METADATA_COLUMNS))

        try:
            cur.executemany(
                f"INSERT INTO {METADATA_TABLE} VALUES (?, ?, ?, ?)",
                rows_to_insert
            )
        except sqlite3.IntegrityError:
            conn.close()
            return False, "Ошибка: в metadata.csv есть повторяющийся iid"

    conn.commit()
    cur.execute("PRAGMA foreign_keys = ON")
    conn.close()

    return True, "metadata инициализирована; genotypes очищена"


def upload_metadata_csv(uploaded_file):
    ensure_metadata_table()

    decoded = uploaded_file.stream.read().decode('utf-8').splitlines()
    reader = csv.DictReader(decoded)

    ok, error = validate_metadata_csv_columns(reader.fieldnames)
    if not ok:
        return False, error

    conn = get_connection()
    cur = conn.cursor()

    total_rows = 0
    added_rows = 0

    for row in reader:
        total_rows += 1

        normalized, error = normalize_metadata_row(row)
        if error:
            conn.close()
            return False, error

        values = tuple(normalized[column] for column in METADATA_COLUMNS)

        try:
            cur.execute(
                f"INSERT INTO {METADATA_TABLE} VALUES (?, ?, ?, ?)",
                values
            )
            added_rows += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()

    return True, f"В файле {total_rows} строк, добавлено {added_rows} новых"


def get_metadata_table(sort=None, order='asc'):
    ensure_metadata_table()
    columns = get_table_columns(METADATA_TABLE)

    conn = get_connection()
    cur = conn.cursor()

    query = f"SELECT * FROM {METADATA_TABLE}"

    if sort in METADATA_ALLOWED_SORT_COLUMNS:
        if order not in ['asc', 'desc']:
            order = 'asc'
        query += f" ORDER BY {sort} {order.upper()}"

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    return columns, rows


def init_variants_from_csv(csv_path):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    cur.execute("PRAGMA foreign_keys = OFF")

    ensure_genotypes_table()
    cur.execute(f"DELETE FROM {GENOTYPES_TABLE}")
    cur.execute(f"DROP TABLE IF EXISTS {VARIANTS_TABLE}")
    cur.execute(VARIANTS_CREATE_SQL)
    cur.execute(VARIANTS_CREATE_INDEX_SQL)

    with open(csv_path) as file:
        reader = csv.DictReader(file)

        ok, error = validate_variants_csv_columns(reader.fieldnames)
        if not ok:
            conn.close()
            return False, error

        rows_to_insert = []

        for row in reader:
            normalized = normalize_variant_row(row)
            rows_to_insert.append(tuple(normalized[column] for column in VARIANTS_COLUMNS))

        cur.executemany(f"""
            INSERT INTO {VARIANTS_TABLE} (
                {", ".join(VARIANTS_COLUMNS)}
            )
            VALUES ({", ".join(["?"] * len(VARIANTS_COLUMNS))})
        """, rows_to_insert)

    conn.commit()
    cur.execute("PRAGMA foreign_keys = ON")
    conn.close()

    return True, "variants инициализирована; genotypes очищена"


def upload_variants_csv(uploaded_file):
    ensure_variants_table()

    decoded = uploaded_file.stream.read().decode('utf-8').splitlines()
    reader = csv.DictReader(decoded)

    ok, error = validate_variants_csv_columns(reader.fieldnames)
    if not ok:
        return False, error

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT {", ".join(VARIANTS_COLUMNS)}
        FROM {VARIANTS_TABLE}
    """)
    existing_rows = set(cur.fetchall())

    total_rows = 0
    added_rows = 0
    rows_to_insert = []

    for row in reader:
        total_rows += 1
        normalized = normalize_variant_row(row)
        row_tuple = tuple(normalized[column] for column in VARIANTS_COLUMNS)

        if row_tuple not in existing_rows:
            rows_to_insert.append(row_tuple)
            existing_rows.add(row_tuple)
            added_rows += 1

    if rows_to_insert:
        cur.executemany(f"""
            INSERT INTO {VARIANTS_TABLE} (
                {", ".join(VARIANTS_COLUMNS)}
            )
            VALUES ({", ".join(["?"] * len(VARIANTS_COLUMNS))})
        """, rows_to_insert)

    conn.commit()
    conn.close()

    return True, f"В файле {total_rows} строк, добавлено {added_rows} новых"


def get_variants_page(sort=None, order='asc', offset=0, page_size=100):
    ensure_variants_table()

    conn = get_connection()
    cur = conn.cursor()

    columns = get_table_columns(VARIANTS_TABLE)

    query = f"SELECT * FROM {VARIANTS_TABLE}"

    if sort in VARIANTS_ALLOWED_SORT_COLUMNS:
        if order not in ['asc', 'desc']:
            order = 'asc'
        query += f" ORDER BY {sort} {order.upper()}"

    query += f" LIMIT {page_size + 1} OFFSET {offset}"

    cur.execute(query)
    rows = cur.fetchall()

    has_more = len(rows) > page_size
    if has_more:
        rows = rows[:page_size]

    conn.close()

    return columns, rows, has_more


def upload_genotypes_csv(uploaded_file):
    ensure_genotypes_table()
    ensure_metadata_table()
    ensure_variants_table()

    decoded = uploaded_file.stream.read().decode('utf-8').splitlines()
    reader = csv.DictReader(decoded)

    ok, error = validate_genotypes_csv_columns(reader.fieldnames)
    if not ok:
        return False, error

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT iid FROM metadata")
    metadata_iids = {row[0] for row in cur.fetchall()}

    cur.execute("SELECT source_id, id FROM variants WHERE source_id IS NOT NULL")
    variants_map = {}
    for source_id, variant_id in cur.fetchall():
        if source_id not in variants_map:
            variants_map[source_id] = variant_id

    cur.execute("""
        SELECT g.iid, v.source_id
        FROM genotypes g
        JOIN variants v ON g.variant_id = v.id
    """)
    existing_pairs = set(cur.fetchall())

    total_rows = 0
    added_rows = 0
    rows_to_insert = []

    for row in reader:
        total_rows += 1

        data, error = normalize_genotype_row(row)
        if error:
            continue

        if data['iid'] not in metadata_iids:
            continue

        if data['snp'] not in variants_map:
            continue

        pair = (data['iid'], data['snp'])

        if pair in existing_pairs:
            continue

        rows_to_insert.append((
            data['iid'],
            variants_map[data['snp']],
            data['gt']
        ))
        existing_pairs.add(pair)
        added_rows += 1

    if rows_to_insert:
        cur.executemany("""
            INSERT INTO genotypes (iid, variant_id, gt)
            VALUES (?, ?, ?)
        """, rows_to_insert)

    conn.commit()
    conn.close()

    return True, f"В файле {total_rows} строк, добавлено {added_rows} новых"


def get_genotypes_page(offset=0, page_size=100):
    ensure_genotypes_table()

    conn = get_connection()
    cur = conn.cursor()

    columns = get_table_columns(GENOTYPES_TABLE)

    cur.execute(f"""
        SELECT * FROM {GENOTYPES_TABLE}
        LIMIT {page_size + 1} OFFSET {offset}
    """)
    rows = cur.fetchall()

    has_more = len(rows) > page_size
    if has_more:
        rows = rows[:page_size]

    conn.close()

    return columns, rows, has_more
    conn.commit()
    conn.close()

    return True, "Запись обновлена"


def delete_metadata(iid):
    ensure_metadata_table()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"DELETE FROM {METADATA_TABLE} WHERE iid = ?", (iid,))

    conn.commit()
    conn.close()

    return True, "Запись удалена"


def init_metadata_from_csv(csv_path):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    cur.execute("PRAGMA foreign_keys = OFF")

    ensure_genotypes_table()
    cur.execute(f"DELETE FROM {GENOTYPES_TABLE}")
    cur.execute(f"DROP TABLE IF EXISTS {METADATA_TABLE}")
    cur.execute(METADATA_CREATE_SQL)

    with open(csv_path) as file:
        reader = csv.DictReader(file)

        ok, error = validate_metadata_csv_columns(reader.fieldnames)
        if not ok:
            conn.close()
            return False, error

        rows_to_insert = []

        for row in reader:
            normalized, error = normalize_metadata_row(row)
            if error:
                conn.close()
                return False, error

            rows_to_insert.append(tuple(normalized[column] for column in METADATA_COLUMNS))

        try:
            cur.executemany(
                f"INSERT INTO {METADATA_TABLE} VALUES (?, ?, ?, ?)",
                rows_to_insert
            )
        except sqlite3.IntegrityError:
            conn.close()
            return False, "Ошибка: в metadata.csv есть повторяющийся iid"

    conn.commit()
    cur.execute("PRAGMA foreign_keys = ON")
    conn.close()

    return True, "metadata инициализирована; genotypes очищена"


def upload_metadata_csv(uploaded_file):
    ensure_metadata_table()

    decoded = uploaded_file.stream.read().decode('utf-8').splitlines()
    reader = csv.DictReader(decoded)

    ok, error = validate_metadata_csv_columns(reader.fieldnames)
    if not ok:
        return False, error

    conn = get_connection()
    cur = conn.cursor()

    total_rows = 0
    added_rows = 0

    for row in reader:
        total_rows += 1

        normalized, error = normalize_metadata_row(row)
        if error:
            conn.close()
            return False, error

        values = tuple(normalized[column] for column in METADATA_COLUMNS)

        try:
            cur.execute(
                f"INSERT INTO {METADATA_TABLE} VALUES (?, ?, ?, ?)",
                values
            )
            added_rows += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()

    return True, f"В файле {total_rows} строк, добавлено {added_rows} новых"


def get_metadata_table(sort=None, order='asc'):
    ensure_metadata_table()
    columns = get_table_columns(METADATA_TABLE)

    conn = get_connection()
    cur = conn.cursor()

    query = f"SELECT * FROM {METADATA_TABLE}"

    if sort in METADATA_ALLOWED_SORT_COLUMNS:
        if order not in ['asc', 'desc']:
            order = 'asc'
        query += f" ORDER BY {sort} {order.upper()}"

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    return columns, rows


def init_variants_from_csv(csv_path):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    cur.execute("PRAGMA foreign_keys = OFF")

    ensure_genotypes_table()
    cur.execute(f"DELETE FROM {GENOTYPES_TABLE}")
    cur.execute(f"DROP TABLE IF EXISTS {VARIANTS_TABLE}")
    cur.execute(VARIANTS_CREATE_SQL)
    cur.execute(VARIANTS_CREATE_INDEX_SQL)

    with open(csv_path) as file:
        reader = csv.DictReader(file)

        ok, error = validate_variants_csv_columns(reader.fieldnames)
        if not ok:
            conn.close()
            return False, error

        rows_to_insert = []

        for row in reader:
            normalized = normalize_variant_row(row)
            rows_to_insert.append(tuple(normalized[column] for column in VARIANTS_COLUMNS))

        cur.executemany(f"""
            INSERT INTO {VARIANTS_TABLE} (
                {", ".join(VARIANTS_COLUMNS)}
            )
            VALUES ({", ".join(["?"] * len(VARIANTS_COLUMNS))})
        """, rows_to_insert)

    conn.commit()
    cur.execute("PRAGMA foreign_keys = ON")
    conn.close()

    return True, "variants инициализирована; genotypes очищена"


def upload_variants_csv(uploaded_file):
    ensure_variants_table()

    decoded = uploaded_file.stream.read().decode('utf-8').splitlines()
    reader = csv.DictReader(decoded)

    ok, error = validate_variants_csv_columns(reader.fieldnames)
    if not ok:
        return False, error

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT {", ".join(VARIANTS_COLUMNS)}
        FROM {VARIANTS_TABLE}
    """)
    existing_rows = set(cur.fetchall())

    total_rows = 0
    added_rows = 0
    rows_to_insert = []

    for row in reader:
        total_rows += 1
        normalized = normalize_variant_row(row)
        row_tuple = tuple(normalized[column] for column in VARIANTS_COLUMNS)

        if row_tuple not in existing_rows:
            rows_to_insert.append(row_tuple)
            existing_rows.add(row_tuple)
            added_rows += 1

    if rows_to_insert:
        cur.executemany(f"""
            INSERT INTO {VARIANTS_TABLE} (
                {", ".join(VARIANTS_COLUMNS)}
            )
            VALUES ({", ".join(["?"] * len(VARIANTS_COLUMNS))})
        """, rows_to_insert)

    conn.commit()
    conn.close()

    return True, f"В файле {total_rows} строк, добавлено {added_rows} новых"


def get_variants_page(sort=None, order='asc', offset=0, page_size=100):
    ensure_variants_table()

    conn = get_connection()
    cur = conn.cursor()

    columns = get_table_columns(VARIANTS_TABLE)

    query = f"SELECT * FROM {VARIANTS_TABLE}"

    if sort in VARIANTS_ALLOWED_SORT_COLUMNS:
        if order not in ['asc', 'desc']:
            order = 'asc'
        query += f" ORDER BY {sort} {order.upper()}"

    query += f" LIMIT {page_size + 1} OFFSET {offset}"

    cur.execute(query)
    rows = cur.fetchall()

    has_more = len(rows) > page_size
    if has_more:
        rows = rows[:page_size]

    conn.close()

    return columns, rows, has_more


def upload_genotypes_csv(uploaded_file):
    ensure_genotypes_table()
    ensure_metadata_table()
    ensure_variants_table()

    decoded = uploaded_file.stream.read().decode('utf-8').splitlines()
    reader = csv.DictReader(decoded)

    ok, error = validate_genotypes_csv_columns(reader.fieldnames)
    if not ok:
        return False, error

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT iid FROM metadata")
    metadata_iids = {row[0] for row in cur.fetchall()}

    cur.execute("SELECT source_id, id FROM variants WHERE source_id IS NOT NULL")
    variants_map = {}
    for source_id, variant_id in cur.fetchall():
        if source_id not in variants_map:
            variants_map[source_id] = variant_id

    cur.execute("""
        SELECT g.iid, v.source_id
        FROM genotypes g
        JOIN variants v ON g.variant_id = v.id
    """)
    existing_pairs = set(cur.fetchall())

    total_rows = 0
    added_rows = 0
    rows_to_insert = []

    for row in reader:
        total_rows += 1

        data, error = normalize_genotype_row(row)
        if error:
            continue

        if data['iid'] not in metadata_iids:
            continue

        if data['snp'] not in variants_map:
            continue

        pair = (data['iid'], data['snp'])

        if pair in existing_pairs:
            continue

        rows_to_insert.append((
            data['iid'],
            variants_map[data['snp']],
            data['gt']
        ))
        existing_pairs.add(pair)
        added_rows += 1

    if rows_to_insert:
        cur.executemany("""
            INSERT INTO genotypes (iid, variant_id, gt)
            VALUES (?, ?, ?)
        """, rows_to_insert)

    conn.commit()
    conn.close()

    return True, f"В файле {total_rows} строк, добавлено {added_rows} новых"


def get_genotypes_page(offset=0, page_size=100):
    ensure_genotypes_table()

    conn = get_connection()
    cur = conn.cursor()

    columns = get_table_columns(GENOTYPES_TABLE)

    cur.execute(f"""
        SELECT * FROM {GENOTYPES_TABLE}
        LIMIT {page_size + 1} OFFSET {offset}
    """)
    rows = cur.fetchall()

    has_more = len(rows) > page_size
    if has_more:
        rows = rows[:page_size]

    conn.close()

    return columns, rows, has_more