import os
import re
import uuid

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    g,
)
from markupsafe import Markup, escape

from db import (
    init_db,
    import_from_excel,
    search_catalogs,
    get_filter_options,
    log_search,
    get_recent_searches,
    get_saved_queries,
    create_saved_query,
    get_saved_query_by_id,
    get_usage_stats,
    get_catalog_by_id,
    toggle_favorite_flag,
    update_engineer_note,
    record_click,
    add_access_log,
    get_last_access_logs,
    get_access_log_stats,
    search_stock,
    get_stock_filter_options,
)

app = Flask(__name__)
# Нужен для flash и сессий
app.secret_key = "change-me-to-random-secret"

# Служебный пароль для обновления базы И админ-панели
# Если в окружении не задан ADMIN_TOKEN — по умолчанию 1234567890
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "1234567890")

# Инициализация БД при старте
init_db()

# При старте пробуем подтянуть данные из Excel
try:
    import_from_excel()
except Exception as e:
    print(f"Внимание: не удалось импортировать Excel при старте: {e}")


# ---------- Jinja-фильтр для подсветки совпадений ----------

@app.template_filter("highlight")
def highlight(text: str, query: str | None) -> Markup:
    if not text or not query:
        return Markup(escape(text or ""))

    terms = [t.strip() for t in query.split() if t.strip()]
    if not terms:
        return Markup(escape(text))

    pattern = re.compile("(" + "|".join(re.escape(t) for t in terms) + ")", re.IGNORECASE)

    def _repl(match: re.Match) -> str:
        return f"<mark>{escape(match.group(0))}</mark>"

    return Markup(pattern.sub(_repl, escape(text)))


# ---------- Вспомогательная функция для фильтров ----------

def _current_filters_from_request(source: str = "args") -> dict:
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


# ---------- Хук для client_id и логирования заходов ----------

@app.before_request
def assign_client_id():
    """
    Присваиваем каждому браузеру client_id через cookie.
    Это позволит видеть, как один и тот же человек ходит по страницам,
    даже если IP меняется.
    """
    # Не трогаем статику и favicon
    if request.path.startswith("/static") or request.path.startswith("/favicon"):
        return

    client_id = request.cookies.get("client_id")
    if not client_id:
        client_id = uuid.uuid4().hex
        g.new_client_id = client_id
    g.client_id = client_id


@app.after_request
def log_request(response):
    """
    Логируем каждый запрос (кроме статики) в access_logs.
    """
    try:
        if not (request.path.startswith("/static") or request.path.startswith("/favicon")):
            # IP с учётом X-Forwarded-For (на PythonAnywhere реальный IP там)
            xff = request.headers.get("X-Forwarded-For", "")
            if xff:
                ip = xff.split(",")[0].strip()
            else:
                ip = request.remote_addr or ""

            ua = request.headers.get("User-Agent", "")[:480]
            referrer = (request.referrer or "")[:480]
            path = request.path
            method = request.method
            client_id = getattr(g, "client_id", "")

            catalog_id = None
            # Если это /open/<id> — вытащим id
            m = re.match(r"^/open/(\d+)", path)
            if m:
                try:
                    catalog_id = int(m.group(1))
                except ValueError:
                    catalog_id = None

            add_access_log(
                path=path,
                method=method,
                ip=ip,
                user_agent=ua,
                referrer=referrer,
                client_id=client_id,
                catalog_id=catalog_id,
            )

        # Проставляем cookie с client_id, если только что выдали
        if hasattr(g, "new_client_id"):
            # 3 года жизни cookie
            response.set_cookie("client_id", g.new_client_id, max_age=60 * 60 * 24 * 365 * 3)
    except Exception as e:
        # Не ломаем ответ, даже если логирование упало
        print(f"Ошибка логирования access_log: {e}")

    return response


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

    # Логируем поиск (упрощённо: только фильтры)
    try:
        log_search(filters)
    except Exception as e:
        print(f"Ошибка log_search: {e}")

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
    rec = get_catalog_by_id(catalog_id)
    if not rec:
        flash("Запись каталога не найдена.", "error")
        return redirect(url_for("index"))

    # Лог клика по каталогу
    try:
        record_click(catalog_id)
    except Exception as e:
        print(f"Ошибка record_click: {e}")

    return redirect(rec.url)


@app.route("/stats", methods=["GET"])
def stats():
    stats_data = get_usage_stats(limit=10)
    return render_template("stats.html", stats=stats_data)


# ---------- Админ-панель логов ----------

@app.route("/admin/logs", methods=["GET", "POST"])
def admin_logs():
    """
    Простая админ-страница:
    - при первом заходе просит пароль администратора (ADMIN_TOKEN)
    - после ввода показывает последние заходы и сводку
    """
    if request.method == "POST":
        token = request.form.get("token", "").strip()
        if token == ADMIN_TOKEN:
            session["is_admin"] = True
            return redirect(url_for("admin_logs"))
        else:
            flash("Неверный пароль администратора.", "error")
            return redirect(url_for("admin_logs"))

    # GET
    if not session.get("is_admin"):
        return render_template("admin_login.html")

    logs = get_last_access_logs(limit=200)
    stats = get_access_log_stats(limit=20)
    return render_template("admin_logs.html", logs=logs, stats=stats)


@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    flash("Админ-доступ отключён.", "success")
    return redirect(url_for("index"))


@app.route("/sklad")
def warehouse():
    """
    Страница склада (пока только просмотр остатков, без редактирования).
    """
    part = (request.args.get("part") or "").strip()
    name = (request.args.get("name") or "").strip()
    group = (request.args.get("group") or "").strip()
    status = (request.args.get("status") or "").strip()

    options = get_stock_filter_options()
    records = search_stock(
        part_number=part or None,
        name=name or None,
        group=group or None,
        status=status or None,
    )

    return render_template(
        "sklad.html",
        records=records,
        stock_groups=options["groups"],
        stock_statuses=options["statuses"],
        current_part=part,
        current_name=name,
        current_group=group,
        current_status=status,
    )


if __name__ == "__main__":
    app.run(debug=True)
