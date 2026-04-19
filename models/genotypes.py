TABLE_NAME = 'genotypes'

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS genotypes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    iid TEXT NOT NULL,
    variant_id INTEGER NOT NULL,
    gt INTEGER NOT NULL CHECK (gt IN (0, 1, 2)),
    UNIQUE (iid, variant_id),
    FOREIGN KEY (iid) REFERENCES metadata(iid) ON DELETE CASCADE,
    FOREIGN KEY (variant_id) REFERENCES variants(id) ON DELETE CASCADE
)
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_genotypes_iid ON genotypes(iid)",
    "CREATE INDEX IF NOT EXISTS idx_genotypes_variant_id ON genotypes(variant_id)"
]
