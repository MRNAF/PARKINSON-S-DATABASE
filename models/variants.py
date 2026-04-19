TABLE_NAME = 'variants'

COLUMNS = [
    'source_id', 'location', 'allele', 'gene', 'feature', 'feature_type',
    'consequence', 'existing_variation', 'impact', 'symbol', 'ref_allele',
    'uploaded_allele', 'cadd_phred', 'cadd_raw', 'gnomade_nfe_af',
    'gnomadg_nfe_af', 'sift', 'polyphen', 'clin_sig', 'var_synonyms', 'af'
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS variants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER,
    location TEXT NOT NULL,
    allele TEXT,
    gene TEXT,
    feature TEXT,
    feature_type TEXT,
    consequence TEXT,
    existing_variation TEXT,
    impact TEXT,
    symbol TEXT,
    ref_allele TEXT,
    uploaded_allele TEXT,
    cadd_phred REAL,
    cadd_raw REAL,
    gnomade_nfe_af REAL,
    gnomadg_nfe_af REAL,
    sift TEXT,
    polyphen TEXT,
    clin_sig TEXT,
    var_synonyms TEXT,
    af REAL
)
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_variants_source_id ON variants(source_id)
"""
