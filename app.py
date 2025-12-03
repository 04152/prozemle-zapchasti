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
    add_part_request,
    get_part_requests,
    update_request_status,  # <-- добавили импорт
)

app = Flask(__name__)
app.secret_key = "change-me-to-random-secret"

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "1234567890")

# Допустимые статусы заявок
ALLOWED_REQUEST_STATUSES = ("new", "in_work", "ordered", "received", "cancelled")

# Инициализация БД
init_db()

# Попытка импортировать каталоги из Excel при старте
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


# ---------- Вспомогательные функции ----------

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


# ---------- client_id и логирование запросов ----------

@app.before_request
def assign_client_id():
    if request.path.startswith("/static") or request.path.startswith("/favicon"):
        return

    client_id = request.cookies.get("client_id")
    if not client_id:
        client_id = uuid.uuid4().hex
        g.new_client_id = client_id
    g.client_id = client_id


@app.after_request
def log_request(response):
    try:
        if not (request.path.startswith("/static") or request.path.startswith("/favicon")):
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

        if hasattr(g, "new_client_id"):
            response.set_cookie("client_id", g.new_client_id, max_age=60 * 60 * 24 * 365 * 3)
    except Exception as e:
        print(f"Ошибка логирования access_log: {e}")

    return response


# ---------- Каталоги ----------

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
    if request.method == "POST":
        token = request.form.get("token", "").strip()
        if token == ADMIN_TOKEN:
            session["is_admin"] = True
            return redirect(url_for("admin_logs"))
        else:
            flash("Неверный пароль администратора.", "error")
            return redirect(url_for("admin_logs"))

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


# ---------- Склад ----------

@app.route("/sklad")
def warehouse():
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


# ---------- Новая заявка ----------

@app.route("/request/new", methods=["GET", "POST"])
def new_request():
    if request.method == "POST":
        part_number = (request.form.get("part_number") or "").strip()
        name = (request.form.get("name") or "").strip()
        model = (request.form.get("model") or "").strip()
        group_name = (request.form.get("group_name") or "").strip()
        catalog_id_raw = (request.form.get("catalog_id") or "").strip()
        source_url = (request.form.get("source_url") or "").strip()

        if not part_number and not name:
            flash("Укажите хотя бы номер детали или наименование.", "error")
            return render_template(
                "request_new.html",
                part_number=part_number,
                name=name,
                model=model,
                group_name=group_name,
                catalog_id=catalog_id_raw,
                source_url=source_url,
            )

        xff = request.headers.get("X-Forwarded-For", "")
        if xff:
            ip = xff.split(",")[0].strip()
        else:
            ip = request.remote_addr or ""

        ua = request.headers.get("User-Agent", "")[:480]

        catalog_id = None
        if catalog_id_raw:
            try:
                catalog_id = int(catalog_id_raw)
            except ValueError:
                catalog_id = None

        try:
            req_id = add_part_request(
                part_number=part_number or None,
                name=name or None,
                model=model or None,
                group_name=group_name or None,
                catalog_id=catalog_id,
                source_url=source_url or None,
                requester_ip=ip,
                requester_ua=ua,
            )
            flash(f"Заявка №{req_id} сохранена. Инженер увидит её в разделе заявок.", "success")
            return redirect(url_for("index"))
        except Exception as e:
            flash(f"Ошибка при сохранении заявки: {e}", "error")
            return render_template(
                "request_new.html",
                part_number=part_number,
                name=name,
                model=model,
                group_name=group_name,
                catalog_id=catalog_id_raw,
                source_url=source_url,
            )

    part_number = (request.args.get("part_number") or "").strip()
    name = (request.args.get("name") or "").strip()
    model = (request.args.get("model") or "").strip()
    group_name = (request.args.get("group_name") or "").strip()
    catalog_id = (request.args.get("catalog_id") or "").strip()
    source_url = (request.args.get("source_url") or "").strip()

    return render_template(
        "request_new.html",
        part_number=part_number,
        name=name,
        model=model,
        group_name=group_name,
        catalog_id=catalog_id,
        source_url=source_url,
    )


# ---------- Заявки (просмотр + смена статуса) ----------

@app.route("/requests", methods=["GET"])
def requests_admin():
    if not session.get("is_admin"):
        return render_template("admin_login.html")

    status = (request.args.get("status") or "").strip()
    if status and status not in ALLOWED_REQUEST_STATUSES:
        status = ""

    requests_list = get_part_requests(status=status or None)

    return render_template(
        "requests.html",
        requests=requests_list,
        current_status=status,
        allowed_statuses=ALLOWED_REQUEST_STATUSES,
    )


@app.route("/requests/<int:request_id>/status", methods=["POST"])
def request_change_status(request_id: int):
    """
    Смена статуса заявки. Доступ только админу.
    """
    if not session.get("is_admin"):
        flash("Нет прав для изменения статусов заявок.", "error")
        return redirect(url_for("admin_logs"))

    new_status = (request.form.get("new_status") or "").strip()
    return_status = (request.form.get("return_status") or "").strip()

    if new_status not in ALLOWED_REQUEST_STATUSES:
        flash("Недопустимый статус.", "error")
        return redirect(url_for("requests_admin", status=return_status))

    ok = update_request_status(request_id, new_status)
    if not ok:
        flash("Заявка не найдена.", "error")
    else:
        human_map = {
            "new": "Новая",
            "in_work": "В работе",
            "ordered": "Заказано",
            "received": "Получено",
            "cancelled": "Отменено",
        }
        flash(f"Статус заявки #{request_id} изменён на: {human_map.get(new_status, new_status)}.", "success")

    return redirect(url_for("requests_admin", status=return_status))


if __name__ == "__main__":
    app.run(debug=True)
