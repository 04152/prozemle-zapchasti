from flask import Flask, render_template, request, redirect, url_for, flash
import re
import os
from markupsafe import Markup, escape

from db import (
    init_db,
    import_from_excel,
    search_catalogs,
    get_filter_options,
    get_catalog_by_id,
    toggle_favorite_flag,
    update_engineer_note,
    log_search,
    get_recent_searches,
    create_saved_query,
    get_saved_queries,
    get_saved_query_by_id,
    record_click,
    get_usage_stats,
)

app = Flask(__name__)
# Нужен для flash-сообщений. В проде лучше сменить на случайную строку.
app.secret_key = "change-me-to-random-secret"

# Служебный пароль для обновления базы
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "prozemle-admin")

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


# ---------- Вспомогательная функция ----------

def _current_filters_from_request(source: str = "args") -> dict:
    """
    Забирает текущие фильтры либо из request.args, либо из request.form.
    """
    container = request.args if source == "args" else request.form

    group = container.get("group", "").strip()
    model = container.get("model", "").strip()
    catalog_type = container.get("catalog_type", "").strip()
    query = container.get("query", "").strip()
    country = container.get("country", "").strip()
    favorites_only = container.get("favorites_only", "") == "1"

    return {
        "group": group,
        "model": model,
        "catalog_type": catalog_type,
        "query": query,
        "country": country,
        "favorites_only": favorites_only,
    }


# ---------- Маршруты ----------

@app.route("/", methods=["GET"])
def index():
    filters = _current_filters_from_request("args")

    records = search_catalogs(
        group=filters["group"] or None,
        model_fragment=filters["model"] or None,
        catalog_type=filters["catalog_type"] or None,
        query=filters["query"] or None,
        country_filter=filters["country"] or None,
        favorites_only=filters["favorites_only"],
    )

    # Логируем только осмысленные запросы
    log_search(filters)

    options = get_filter_options()
    recent_searches = get_recent_searches(limit=10)
    saved_queries = get_saved_queries(limit=20)

    return render_template(
        "index.html",
        records=records,
        groups=options["groups"],
        catalog_types=options["types"],
        countries=options["countries"],
        current_group=filters["group"],
        current_model=filters["model"],
        current_type=filters["catalog_type"],
        current_query=filters["query"],
        current_country=filters["country"],
        favorites_only=filters["favorites_only"],
        recent_searches=recent_searches,
        saved_queries=saved_queries,
    )


@app.route("/refresh", methods=["POST"])
def refresh():
    """
    Кнопка 'Обновить базу':
    - требует служебный пароль ADMIN_TOKEN
    - перечитывает Excel
    - полностью пересобирает таблицу catalogs
    """
    token = request.form.get("token", "").strip()

    if token != ADMIN_TOKEN:
        flash("Неверный пароль администратора. База не обновлена.", "error")
        return redirect(url_for("index"))

    try:
        count = import_from_excel()
        flash(f"База каталога обновлена. Загружено записей: {count}.", "success")
    except Exception as e:
        flash(f"Ошибка при обновлении базы: {e}", "error")
    return redirect(url_for("index"))


@app.route("/favorite/<int:catalog_id>", methods=["POST"])
def toggle_favorite(catalog_id: int):
    """
    Переключение флага 'избранное' для конкретного каталога.
    Возвращаемся на ту же страницу с теми же фильтрами.
    """
    filters = _current_filters_from_request("form")

    result = toggle_favorite_flag(catalog_id)
    if result is None:
        flash("Запись каталога не найдена.", "error")
    else:
        flash(
            "Каталог помечен как избранный." if result else "Каталог убран из избранного.",
            "success",
        )

    return redirect(
        url_for(
            "index",
            group=filters["group"],
            model=filters["model"],
            catalog_type=filters["catalog_type"],
            query=filters["query"],
            country=filters["country"],
            favorites_only="1" if filters["favorites_only"] else "",
        )
    )


@app.route("/note/<int:catalog_id>", methods=["GET", "POST"])
def edit_note(catalog_id: int):
    """
    Просмотр/редактирование примечания инженера по каталогу.
    """
    if request.method == "POST":
        note = request.form.get("engineer_note", "")
        filters = _current_filters_from_request("form")

        ok = update_engineer_note(catalog_id, note)
        if not ok:
            flash("Запись каталога не найдена.", "error")
        else:
            flash("Примечание сохранено.", "success")

        return redirect(
            url_for(
                "index",
                group=filters["group"],
                model=filters["model"],
                catalog_type=filters["catalog_type"],
                query=filters["query"],
                country=filters["country"],
                favorites_only="1" if filters["favorites_only"] else "",
            )
        )

    # GET-запрос: показываем форму
    filters = _current_filters_from_request("args")
    record = get_catalog_by_id(catalog_id)
    if not record:
        flash("Запись каталога не найдена.", "error")
        return redirect(url_for("index"))

    return render_template(
        "note.html",
        record=record,
        current_group=filters["group"],
        current_model=filters["model"],
        current_type=filters["catalog_type"],
        current_query=filters["query"],
        current_country=filters["country"],
        favorites_only=filters["favorites_only"],
    )


@app.route("/save_query", methods=["POST"])
def save_query():
    """
    Сохраняем текущий набор фильтров как шаблон.
    """
    filters = _current_filters_from_request("form")
    title = request.form.get("title", "").strip()

    if not title:
        flash("Нужно указать название шаблона.", "error")
        return redirect(
            url_for(
                "index",
                group=filters["group"],
                model=filters["model"],
                catalog_type=filters["catalog_type"],
                query=filters["query"],
                country=filters["country"],
                favorites_only="1" if filters["favorites_only"] else "",
            )
        )

    obj = create_saved_query(title, filters)
    if not obj:
        flash("Нельзя сохранить пустой запрос (без фильтров и поиска).", "error")
    else:
        flash("Шаблон запроса сохранён.", "success")

    return redirect(
        url_for(
            "index",
            group=filters["group"],
            model=filters["model"],
            catalog_type=filters["catalog_type"],
            query=filters["query"],
            country=filters["country"],
            favorites_only="1" if filters["favorites_only"] else "",
        )
    )


@app.route("/use_query/<int:query_id>", methods=["GET"])
def use_query(query_id: int):
    """
    Применяем сохранённый шаблон запроса.
    """
    obj = get_saved_query_by_id(query_id)
    if not obj:
        flash("Шаблон запроса не найден.", "error")
        return redirect(url_for("index"))

    return redirect(
        url_for(
            "index",
            group=obj.group_name or "",
            model=obj.model_fragment or "",
            catalog_type=obj.catalog_type or "",
            query=obj.query or "",
            country=obj.country or "",
        )
    )


@app.route("/open/<int:catalog_id>", methods=["GET"])
def open_catalog(catalog_id: int):
    """
    Логируем клик по каталогу и перенаправляем на реальную ссылку.
    """
    rec = get_catalog_by_id(catalog_id)
    if not rec:
        flash("Запись каталога не найдена.", "error")
        return redirect(url_for("index"))

    record_click(catalog_id)
    return redirect(rec.url)


@app.route("/stats", methods=["GET"])
def stats():
    """
    Страница с обзорной статистикой по использованию каталога.
    """
    stats_data = get_usage_stats(limit=10)
    return render_template("stats.html", stats=stats_data)


if __name__ == "__main__":
    app.run(debug=True)
