from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро.xlsx"

# Доменам отсюда мы не доверяем (платные/закрытые/проблемные каталоги)
BLOCKED_DOMAINS = {
    "machinetechdoc.com",
    "servicepartmanuals.com",
    "interdalnoboy.com",
    "www.avtozapchasty.ru",
    "avtozapchasty.ru",
    "avtofiles.com",
    "www.niva-club.net",
    "niva-club.net",
}


def _is_allowed_row(row: pd.Series) -> bool:
    """
    Фильтрация строк с точки зрения доступности ссылки:
    - есть ли URL
    - не попадает ли домен в чёрный список
    - не помечен ли каталог как 'Платный'
    """
    url = str(row.get("Ссылка", "")).strip()
    if not url:
        return False

    # только http/https
    if not (url.startswith("http://") or url.startswith("https://")):
        return False

    netloc = urlsplit(url).netloc.lower()

    if netloc in BLOCKED_DOMAINS:
        return False

    catalog_type = str(row.get("Тип каталога", "")).lower()
    if "платный" in catalog_type:
        return False

    return True


def load_catalog_df() -> pd.DataFrame:
    """
    Загружает Excel, очищает базу (удаляет пустые/заблокированные ссылки),
    подготавливает к использованию в приложении.
    """
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Не найден файл с каталогами: {DATA_FILE}")

    df = pd.read_excel(DATA_FILE)

    required_cols = {"Группа техники", "Модели", "Тип каталога", "Описание", "Ссылка"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"В Excel отсутствуют обязательные столбцы: {', '.join(missing)}")

    # убираем пустые ссылки
    df = df.dropna(subset=["Ссылка"]).copy()

    # фильтруем по доменам и 'Платный'
    df = df[df.apply(_is_allowed_row, axis=1)].copy()

    # нормализуем текст
    for col in ["Группа техники", "Модели", "Тип каталога", "Описание", "Ссылка"]:
        df[col] = df[col].astype(str).str.strip()

    # сортировка для аккуратного вывода
    df = df.sort_values(["Группа техники", "Модели", "Тип каталога"]).reset_index(drop=True)

    return df


def filter_catalog(
    df: pd.DataFrame,
    group: str | None = None,
    model: str | None = None,
    catalog_type: str | None = None,
    query: str | None = None,
) -> pd.DataFrame:
    """
    Универсальный фильтр каталога по параметрам.
    query — текстовый поиск по Модели / Описанию / Ссылке /
            а также по дополнительным колонкам, если они есть
            (Номер каталога, Каталожный номер и т.п.).
    """
    result = df

    # Фильтр по группе техники (точное совпадение)
    if group:
        result = result[result["Группа техники"] == group]

    # Фильтр по модели (подстрока в колонке "Модели")
    if model:
        pattern = str(model).strip()
        if pattern:
            mask_model = result["Модели"].str.contains(pattern, case=False, na=False)
            result = result[mask_model]

    # Фильтр по типу каталога
    if catalog_type:
        result = result[result["Тип каталога"] == catalog_type]

    # Универсальный текстовый поиск
    if query:
        pattern = str(query).strip()
        if pattern:
            # Список колонок, в которых будем искать текст
            candidate_cols = [
                "Модели",
                "Описание",
                "Ссылка",
                "Номер каталога",
                "Каталожный номер",
                "Каталожный номер детали",
            ]

            masks = []
            for col in candidate_cols:
                if col in result.columns:
                    masks.append(result[col].str.contains(pattern, case=False, na=False))

            if masks:
                # Объединяем все маски через ИЛИ
                combined = masks[0]
                for m in masks[1:]:
                    combined = combined | m
                result = result[combined]

    return result.reset_index(drop=True)

