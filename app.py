# app.py
from pathlib import Path
from flask import Flask, render_template, request
from catalog_data import load_catalog_df  # ← вот этот импорт и ругался раньше

BASE_DIR = Path(__file__).resolve().parent

app = Flask(__name__)

CATALOG_DF = load_catalog_df()



@app.route("/", methods=["GET"])
def index():
    # Параметры из формы
    selected_group = request.args.get("group", "").strip()
    selected_model = request.args.get("model", "").strip()
    selected_type = request.args.get("catalog_type", "").strip()
    search_text = request.args.get("text", "").strip()

    df = CATALOG_DF.copy()

    # Фильтр по группе техники
    if selected_group:
        df = df[df["Группа техники"] == selected_group]

    # Фильтр по модели (подстрока, без учёта регистра)
    if selected_model:
        df = df[df["Модели"].str.contains(selected_model, case=False, na=False)]

    # Фильтр по типу каталога
    if selected_type:
        df = df[df["Тип каталога"] == selected_type]

    # Поиск по описанию и моделям
    if search_text:
        mask_desc = df["Описание"].str.contains(search_text, case=False, na=False)
        mask_model = df["Модели"].str.contains(search_text, case=False, na=False)
        df = df[mask_desc | mask_model]

    results = df.to_dict(orient="records")

    # Список групп техники и типов каталогов для выпадающих списков
    groups = sorted(CATALOG_DF["Группа техники"].dropna().unique())
    catalog_types = sorted(CATALOG_DF["Тип каталога"].dropna().unique())

    selected = {
        "group": selected_group,
        "model": selected_model,
        "catalog_type": selected_type,
        "text": search_text,
    }

    return render_template(
        "index.html",
        results=results,
        groups=groups,
        catalog_types=catalog_types,
        selected=selected,
    )


if __name__ == "__main__":
    # host="0.0.0.0" — чтобы открыть с других устройств в сети
    app.run(host="0.0.0.0", port=8000, debug=True)
