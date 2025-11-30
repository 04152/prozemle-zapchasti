from flask import Flask, render_template, request
from catalog_data import load_catalog_df, filter_catalog

app = Flask(__name__)

# Загружаем каталог один раз при старте приложения
CATALOG_DF = load_catalog_df()


@app.route("/", methods=["GET"])
def index():
    group = request.args.get("group", "").strip()
    model = request.args.get("model", "").strip()
    catalog_type = request.args.get("catalog_type", "").strip()
    query = request.args.get("query", "").strip()

    filtered = filter_catalog(
        CATALOG_DF,
        group=group or None,
        model=model or None,
        catalog_type=catalog_type or None,
        query=query or None,
    )

    groups = sorted(CATALOG_DF["Группа техники"].dropna().unique())
    catalog_types = sorted(CATALOG_DF["Тип каталога"].dropna().unique())

    return render_template(
        "index.html",
        records=filtered.to_dict(orient="records"),
        groups=groups,
        catalog_types=catalog_types,
        current_group=group,
        current_model=model,
        current_type=catalog_type,
        current_query=query,
    )


if __name__ == "__main__":
    # Локальный запуск
    app.run(debug=True)
