from flask import Flask, request, render_template

from crud import (
    get_existing_tables,
    ensure_metadata_table,
    ensure_variants_table,
    ensure_genotypes_table,
    init_metadata_from_csv,
    init_variants_from_csv,
    create_metadata,
    get_metadata_by_iid,
    update_metadata,
    delete_metadata,
    upload_metadata_csv,
    upload_variants_csv,
    upload_genotypes_csv,
    get_metadata_table,
    get_variants_page,
    get_genotypes_page
)

from models.metadata import TABLE_NAME as METADATA_TABLE
from models.variants import TABLE_NAME as VARIANTS_TABLE
from models.genotypes import TABLE_NAME as GENOTYPES_TABLE

app = Flask(__name__)

METADATA_CSV_PATH = 'bio_db_project/data/metadata.csv'
VARIANTS_CSV_PATH = 'bio_db_project/data/variants.csv'


@app.route('/')
def index():
    existing_tables = get_existing_tables()

    return render_template(
        'index.html',
        existing_tables=existing_tables,
        metadata_table=METADATA_TABLE,
        variants_table=VARIANTS_TABLE,
        genotypes_table=GENOTYPES_TABLE
    )


@app.route('/metadata')
def metadata_list():
    sort = request.args.get('sort')
    order = request.args.get('order', 'asc').lower()

    columns, rows = get_metadata_table(sort, order)

    return render_template(
        'metadata/list.html',
        rows=rows,
        columns=columns,
        sort=sort,
        order=order
    )


@app.route('/variants')
def variants_list():
    try:
        offset = int(request.args.get('offset', 0))
    except ValueError:
        offset = 0

    sort = request.args.get('sort')
    order = request.args.get('order', 'asc').lower()

    columns, rows, has_more = get_variants_page(sort, order, offset, 100)

    return render_template(
        'variants/list.html',
        rows=rows,
        columns=columns,
        sort=sort,
        order=order,
        offset=offset,
        page_size=100,
        has_more=has_more
    )


@app.route('/genotypes')
def genotypes_list():
    try:
        offset = int(request.args.get('offset', 0))
    except ValueError:
        offset = 0

    columns, rows, has_more = get_genotypes_page(offset, 100)

    return render_template(
        'genotypes/list.html',
        rows=rows,
        columns=columns,
        offset=offset,
        page_size=100,
        has_more=has_more
    )


@app.route('/init_metadata')
def init_metadata():
    ok, message = init_metadata_from_csv(METADATA_CSV_PATH)

    if not ok:
        return message + " <br><a href='/'>На главную</a>"

    return f"{message}<br><a href='/metadata'>Открыть metadata</a><br><a href='/'>На главную</a>"


@app.route('/init_variants')
def init_variants():
    ok, message = init_variants_from_csv(VARIANTS_CSV_PATH)

    if not ok:
        return message + " <br><a href='/'>На главную</a>"

    return f"{message}<br><a href='/variants'>Открыть variants</a><br><a href='/'>На главную</a>"


@app.route('/metadata/add', methods=['GET', 'POST'])
def metadata_add():
    ensure_metadata_table()

    if request.method == 'POST':
        ok, message = create_metadata(request.form)

        if not ok:
            return message + " <br><a href='/metadata/add'>Назад</a><br><a href='/'>На главную</a>"

        return f"{message}. <a href='/metadata'>Назад</a><br><a href='/'>На главную</a>"

    return render_template('metadata/form.html', row=None)


@app.route('/metadata/edit/<iid>', methods=['GET', 'POST'])
def metadata_edit(iid):
    if request.method == 'POST':
        ok, message = update_metadata(iid, request.form)

        if not ok:
            return message + " <br><a href='/metadata'>Назад</a><br><a href='/'>На главную</a>"

        return f"{message}. <a href='/metadata'>Назад</a><br><a href='/'>На главную</a>"

    row = get_metadata_by_iid(iid)
    return render_template('metadata/form.html', row=row)


@app.route('/metadata/delete/<iid>')
def metadata_delete(iid):
    ok, message = delete_metadata(iid)
    return f"{message}. <a href='/metadata'>Назад</a><br><a href='/'>На главную</a>"


@app.route('/metadata/upload', methods=['GET', 'POST'])
def metadata_upload():
    ensure_metadata_table()

    if request.method == 'POST':
        uploaded_file = request.files.get('file')

        if not uploaded_file or uploaded_file.filename == '':
            return "Ошибка: файл не выбран <br><a href='/metadata/upload'>Назад</a><br><a href='/'>На главную</a>"

        ok, message = upload_metadata_csv(uploaded_file)

        if not ok:
            return message + " <br><a href='/metadata/upload'>Назад</a><br><a href='/'>На главную</a>"

        return f"{message}. <a href='/metadata'>Открыть таблицу</a><br><a href='/'>На главную</a>"

    return render_template('metadata/upload.html')


@app.route('/variants/upload', methods=['GET', 'POST'])
def variants_upload():
    ensure_variants_table()

    if request.method == 'POST':
        uploaded_file = request.files.get('file')

        if not uploaded_file or uploaded_file.filename == '':
            return "Ошибка: файл не выбран <br><a href='/variants/upload'>Назад</a><br><a href='/'>На главную</a>"

        ok, message = upload_variants_csv(uploaded_file)

        if not ok:
            return message + " <br><a href='/variants/upload'>Назад</a><br><a href='/'>На главную</a>"

        return f"{message}. <a href='/variants'>Открыть таблицу</a><br><a href='/'>На главную</a>"

    return render_template('variants/upload.html')


@app.route('/genotypes/upload', methods=['GET', 'POST'])
def genotypes_upload():
    ensure_genotypes_table()

    if request.method == 'POST':
        uploaded_file = request.files.get('file')

        if not uploaded_file or uploaded_file.filename == '':
            return "Ошибка: файл не выбран <br><a href='/genotypes/upload'>Назад</a><br><a href='/'>На главную</a>"

        ok, message = upload_genotypes_csv(uploaded_file)

        if not ok:
            return message + " <br><a href='/genotypes/upload'>Назад</a><br><a href='/'>На главную</a>"

        return f"{message}. <a href='/genotypes'>Открыть таблицу</a><br><a href='/'>На главную</a>"

    return render_template('genotypes/upload.html')


if __name__ == '__main__':
    app.run(debug=True)