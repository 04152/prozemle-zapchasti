from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent

# Если имя файла у тебя другое — ПОДПРАВЬ эту строку
DATA_FILE = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро_очищено.xlsx"

REQUIRED_COLUMNS = [
    "Группа техники",
    "Модели",
    "Тип каталога",
    "Описание",
    "Ссылка",
]

# Домены, которые считаем платными/закрытыми/проблемными
BLOCKED_DOMAINS = {
    "machinetechdoc.com",
    "servicepartmanuals.com",
    "interdalnoboy.com",
    "www.avtozapchasty.ru",
    "avtofiles.com",
    "www.niva-club.net",
}


def _get_domain(url: str) -> str:
    try:
        return urlparse((url or "").strip()).netloc.lower()
    except Exception:
        return ""


def load_catalog_df() -> pd.DataFrame:
    """
    Загружаем Excel, чистим данные и убираем платные/проблемные ссылки.
    """
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Файл с каталогами не найден: {DATA_FILE}")

    df = pd.read_excel(DATA_FILE, sheet_name="Sheet1")

    # Проверяем наличие нужных колонок
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(
            "В Excel не хватает обязательных колонок: " + ", ".join(missing)
        )

    # Заполняем пропуски строками, чтобы не падали фильтры
    for col in REQUIRED_COLUMNS:
        df[col] = df[col].fillna("")

    # Чистим и нормализуем ссылки
    df["Ссылка"] = df["Ссылка"].astype(str).str.strip()
    df = df[df["Ссылка"].str.startswith("http")]

    # Если в файле есть колонка 'Статус_ссылки' (после проверки validate_links.py),
    # то берём только хорошие ссылки
    if "Статус_ссылки" in df.columns:
        df["Статус_ссылки"] = df["Статус_ссылки"].astype(str).str.lower()
        df = df[df["Статус_ссылки"] == "ok"]

    # Фильтрация по доменам
    df["domain"] = df["Ссылка"].apply(_get_domain)
    df = df[~df["domain"].isin(BLOCKED_DOMAINS)]

    # Немного наводим порядок
    df = df.sort_values(
        ["Группа техники", "Модели", "Тип каталога"]
    ).reset_index(drop=True)

    # domain во фронтенд не нужен
    df = df.drop(columns=["domain"])

    return df
