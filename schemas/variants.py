EXPECTED_COLUMNS = [
    'ID', 'Location', 'Allele', 'Gene', 'Feature', 'Feature_type',
    'Consequence', 'Existing_variation', 'IMPACT', 'SYMBOL', 'REF_ALLELE',
    'UPLOADED_ALLELE', 'CADD_PHRED', 'CADD_RAW', 'gnomADe_NFE_AF',
    'gnomADg_NFE_AF', 'SIFT', 'PolyPhen', 'CLIN_SIG', 'VAR_SYNONYMS', 'AF'
]

ALLOWED_SORT_COLUMNS = [
    'id', 'source_id', 'location', 'gene', 'feature', 'consequence', 'impact',
    'symbol', 'cadd_phred', 'cadd_raw', 'gnomade_nfe_af', 'gnomadg_nfe_af', 'af'
]


def parse_nullable_text(value):
    if value is None:
        return None
    value = value.strip()
    if value == '' or value.upper() == 'NA' or value == '-':
        return None
    return value


def parse_nullable_float(value):
    value = parse_nullable_text(value)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_nullable_int(value):
    value = parse_nullable_text(value)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def validate_variants_csv_columns(fieldnames):
    if fieldnames is None:
        return False, 'Ошибка: CSV-файл пустой или не читается'
    missing = [column for column in EXPECTED_COLUMNS if column not in fieldnames]
    if missing:
        return False, 'Ошибка: в CSV отсутствуют колонки: ' + ', '.join(missing)
    return True, None


def normalize_variant_row(row):
    return {
        'source_id': parse_nullable_int(row.get('ID')),
        'location': parse_nullable_text(row.get('Location')),
        'allele': parse_nullable_text(row.get('Allele')),
        'gene': parse_nullable_text(row.get('Gene')),
        'feature': parse_nullable_text(row.get('Feature')),
        'feature_type': parse_nullable_text(row.get('Feature_type')),
        'consequence': parse_nullable_text(row.get('Consequence')),
        'existing_variation': parse_nullable_text(row.get('Existing_variation')),
        'impact': parse_nullable_text(row.get('IMPACT')),
        'symbol': parse_nullable_text(row.get('SYMBOL')),
        'ref_allele': parse_nullable_text(row.get('REF_ALLELE')),
        'uploaded_allele': parse_nullable_text(row.get('UPLOADED_ALLELE')),
        'cadd_phred': parse_nullable_float(row.get('CADD_PHRED')),
        'cadd_raw': parse_nullable_float(row.get('CADD_RAW')),
        'gnomade_nfe_af': parse_nullable_float(row.get('gnomADe_NFE_AF')),
        'gnomadg_nfe_af': parse_nullable_float(row.get('gnomADg_NFE_AF')),
        'sift': parse_nullable_text(row.get('SIFT')),
        'polyphen': parse_nullable_text(row.get('PolyPhen')),
        'clin_sig': parse_nullable_text(row.get('CLIN_SIG')),
        'var_synonyms': parse_nullable_text(row.get('VAR_SYNONYMS')),
        'af': parse_nullable_float(row.get('AF')),
    }
