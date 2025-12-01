from flask import Flask, render_template, request, redirect, url_for, flash
import re
from markupsafe import Markup, escape
from db import init_db, import_from_excel, search_catalogs, get_filter_options

app = Flask(__name__)
# Нужен для flash-сообщений. В проде лучше сменить на случайную строку.
app.secret_key = "change-me-to-random-secret"

# Инициализация БД при старте
init_db()

# При первом запуске (и вообще при каждом старте) можно пробовать подтянуть данные из Excel
try:
    import_from_excel()
except Exception as e:
    print(f"Внимание: не удалось импортировать Excel при старте: {e}")


# ---------- Jinja-фильтр для подсветки совпадений ----------

@app.template_filter("highlight")
def highlight(text: str, query: str | None) -> Markup:
    """
    Подсвечивает все вхождения слов из query в text с помощью <mark>.
    """
    if not text or not query:
        return Markup(escape(text or ""))

    terms = [t.strip() for t in query.split() if t.strip()]
    if not terms:
        return Markup(escape(text))

    pattern = re.compile("(" + "|".join(re.escape(t) for t in terms) + ")", re.IGNORECASE)

    def _repl(match: re.Match) -> str:
        return f"<mark>{escape(match.group(0))}</mark>"

    return Markup(pattern.sub(_repl, escape(text)))


# ---------- Маршруты ----------

@app.route("/", methods=["GET"])
def index():
    group = request.args.get("group", "").strip() or None
    model = request.args.get("model", "").strip() or None
    catalog_type = request.args.get("catalog_type", "").strip() or None
    query = request.args.get("query", "").strip() or None
    country = request.args.get("country", "").strip() or None

    filters = get_filter_options()

    records = search_catalogs(
        group=group,
        model_fragment=model,
        catalog_type=catalog_type,
        query=query,
        country_filter=country,
    )

    return render_template(
        "index.html",
        records=records,
        groups=filters["groups"],
        catalog_types=filters["types"],
        countries=filters["countries"],
        current_group=group or "",
        current_model=model or "",
        current_type=catalog_type or "",
        current_query=query or "",
        current_country=country or "",
    )


@app.route("/refresh", methods=["POST"])
def refresh():
    """
    Кнопка 'Обновить базу':
    - перечитывает Excel
    - полностью пересобирает таблицу catalogs
    В будущем сюда можно добавить автоматический поиск новых ссылок.
    """
    try:
        count = import_from_excel()
        flash(f"База каталога обновлена. Загружено записей: {count}.", "success")
    except Exception as e:
        flash(f"Ошибка при обновлении базы: {e}", "error")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True)
