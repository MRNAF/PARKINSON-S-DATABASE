EXPECTED_COLUMNS = ['IID', 'PHENOTYPE', 'Sex', 'Age']
FIELDS = ['iid', 'phenotype', 'sex', 'age']


def validate_metadata_csv_columns(fieldnames):
    if fieldnames is None:
        return False, 'Ошибка: CSV пустой или не читается'
    missing = [col for col in EXPECTED_COLUMNS if col not in fieldnames]
    if missing:
        return False, 'Ошибка: отсутствуют колонки: ' + ', '.join(missing)
    return True, None


def normalize_metadata_row(row):
    data = {}
    iid = row.get('IID', '').strip()
    if not iid:
        return None, 'Ошибка: пустой IID'
    data['iid'] = iid

    phenotype = row.get('PHENOTYPE', '').strip()
    if phenotype not in ['0', '1']:
        return None, 'Ошибка: phenotype должен быть 0 или 1'
    data['phenotype'] = int(phenotype)

    sex = row.get('Sex', '').strip()
    if sex not in ['1', '2']:
        return None, 'Ошибка: sex должен быть 1 или 2'
    data['sex'] = int(sex)

    age = row.get('Age', '').strip()
    try:
        age = int(age)
        if age <= 0:
            return None, 'Ошибка: age должен быть > 0'
        data['age'] = age
    except Exception:
        return None, 'Ошибка: age должен быть числом'

    return data, None


def validate_metadata_form(form):
    data = {}
    for field in FIELDS:
        data[field] = form.get(field, '').strip()

    if not data['iid']:
        return None, 'Ошибка: поле iid должно быть заполнено'
    if data['phenotype'] not in ['0', '1']:
        return None, 'Ошибка: phenotype должен быть 0 или 1'
    if data['sex'] not in ['1', '2']:
        return None, 'Ошибка: sex должен быть 1 или 2'

    try:
        age = int(data['age'])
        if age <= 0:
            return None, 'Ошибка: age должен быть положительным'
        data['age'] = age
    except Exception:
        return None, 'Ошибка: age должен быть числом'

    data['phenotype'] = int(data['phenotype'])
    data['sex'] = int(data['sex'])
    return data, None
