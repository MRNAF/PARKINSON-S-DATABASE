EXPECTED_COLUMNS = ['IID', 'SNP', 'GT']


def validate_genotypes_csv_columns(fieldnames):
    if fieldnames is None:
        return False, 'Ошибка: CSV пустой'
    missing = [c for c in EXPECTED_COLUMNS if c not in fieldnames]
    if missing:
        return False, 'Ошибка: нет колонок: ' + ', '.join(missing)
    return True, None


def normalize_genotype_row(row):
    data = {}

    iid = row.get('IID', '').strip()
    if not iid:
        return None, 'Ошибка: пустой IID'
    data['iid'] = iid

    snp = row.get('SNP', '').strip()
    if not snp:
        return None, 'Ошибка: пустой SNP'
    try:
        data['snp'] = int(snp)
    except Exception:
        return None, 'Ошибка: SNP должен быть числом'

    gt = row.get('GT', '').strip()
    if gt not in ['0', '1', '2']:
        return None, 'Ошибка: GT должен быть 0/1/2'
    data['gt'] = int(gt)

    return data, None
