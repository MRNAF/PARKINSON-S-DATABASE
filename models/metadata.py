TABLE_NAME = 'metadata'

COLUMNS = ['iid', 'phenotype', 'sex', 'age']

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS metadata (
    iid TEXT PRIMARY KEY,
    phenotype INTEGER NOT NULL CHECK (phenotype IN (0, 1)),
    sex INTEGER NOT NULL CHECK (sex IN (1, 2)),
    age INTEGER NOT NULL CHECK (age > 0)
)
"""
