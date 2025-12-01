from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit
from datetime import datetime

import pandas as pd
from sqlalchemy import (
    create_engine,
    String,
    Integer,
    DateTime,
    select,
    and_,
    or_,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро.xlsx"
DB_FILE = BASE_DIR / "catalogs.db"


# ---------- Базовый класс SQLAlchemy ----------

class Base(DeclarativeBase):
    pass


class Catalog(Base):
    __tablename__ = "catalogs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Основные поля
    group_name: Mapped[str] = mapped_column(String(255), index=True)
    models: Mapped[str] = mapped_column(String(255), index=True)
    catalog_type: Mapped[str] = mapped_column(String(100), index=True)
    description: Mapped[str] = mapped_column(String(1000))
    url: Mapped[str] = mapped_column(String(1000))

    # Дополнительные поля
    domain: Mapped[str] = mapped_column(String(255), index=True)
    source_country: Mapped[str] = mapped_column(String(10), index=True, default="")
    catalog_number: Mapped[str] = mapped_column(String(255), default="")
    part_numbers: Mapped[str] = mapped_column(String(1000), default="")

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


engine = create_engine(f"sqlite:///{DB_FILE}", echo=False, future=True)


def init_db() -> None:
    """Создаёт таблицы, если их ещё нет."""
    Base.metadata.create_all(engine)


# ---------- Импорт из Excel в БД ----------

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


def get_domain(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower()
    except Exception:
        return ""


def guess_country_by_domain(domain: str) -> str:
    """
    Грубая эвристика: BY / RU / другие.
    """
    if domain.endswith(".by"):
        return "BY"
    if domain.endswith(".ru"):
        return "RU"
    return "OTHER"


def import_from_excel() -> int:
    """
    Полностью пересоздаёт содержимое таблицы catalogs
    на основе Excel-файла. Возвращает количество загруженных записей.
    """
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Не найден Excel-файл: {DATA_FILE}")

    df = pd.read_excel(DATA_FILE)

    required = {"Группа техники", "Модели", "Тип каталога", "Описание", "Ссылка"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В Excel не хватает столбцов: {', '.join(missing)}")

    df = df.dropna(subset=["Ссылка"]).copy()

    # Нормализация строк
    for col in ["Группа техники", "Модели", "Тип каталога", "Описание", "Ссылка"]:
        df[col] = df[col].astype(str).str.strip()

    records: list[Catalog] = []

    for _, row in df.iterrows():
        url = row["Ссылка"].strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            continue

        domain = get_domain(url)
        if domain in BLOCKED_DOMAINS:
            continue

        group_name = row["Группа техники"].strip()
        models = row["Модели"].strip()
        catalog_type = row["Тип каталога"].strip()
        description = row["Описание"].strip()

        # Дополнительные поля — если колонок нет, просто будут пустые
        catalog_number = str(row.get("Номер каталога", "") or "").strip()
        part_numbers = str(row.get("Каталожные номера", "") or "").strip()

        record = Catalog(
            group_name=group_name,
            models=models,
            catalog_type=catalog_type,
            description=description,
            url=url,
            domain=domain,
            source_country=guess_country_by_domain(domain),
            catalog_number=catalog_number,
            part_numbers=part_numbers,
        )
        records.append(record)

    with Session(engine) as session:
        # Полностью очищаем таблицу и загружаем заново
        session.query(Catalog).delete()
        session.add_all(records)
        session.commit()

    return len(records)


# ---------- Поиск по БД с фильтрами ----------

def search_catalogs(
    group: str | None = None,
    model_fragment: str | None = None,
    catalog_type: str | None = None,
    query: str | None = None,
    country_filter: str | None = None,  # "BY", "RU", "OTHER"
) -> list[Catalog]:
    """
    Выполняет поиск по таблице catalogs.

    - group           — точное совпадение по группе техники
    - model_fragment  — подстрока в поле models
    - catalog_type    — точное совпадение типа
    - query           — общий текстовый поиск по нескольким полям
    - country_filter  — фильтр страны источника (BY/RU/OTHER)
    """
    with Session(engine) as session:
        stmt = select(Catalog)
        conditions = []

        if group:
            conditions.append(Catalog.group_name == group)

        if model_fragment:
            pattern = f"%{model_fragment.strip()}%"
            conditions.append(Catalog.models.ilike(pattern))

        if catalog_type:
            conditions.append(Catalog.catalog_type == catalog_type)

        if country_filter in {"BY", "RU", "OTHER"}:
            conditions.append(Catalog.source_country == country_filter)

        if query:
            # Разбиваем строку на слова
            terms = [t.strip() for t in query.lower().split() if t.strip()]
            if terms:
                word_conditions = []
                for term in terms:
                    p = f"%{term}%"
                    word_conditions.append(
                        or_(
                            Catalog.models.ilike(p),
                            Catalog.description.ilike(p),
                            Catalog.url.ilike(p),
                            Catalog.catalog_number.ilike(p),
                            Catalog.part_numbers.ilike(p),
                        )
                    )
                # Все слова должны встретиться (логика И)
                conditions.append(and_(*word_conditions))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.order_by(Catalog.group_name, Catalog.models, Catalog.catalog_type)

        result = session.scalars(stmt).all()
        return list(result)


def get_filter_options() -> dict:
    """
    Возвращает уникальные значения для выпадающих списков:
    - группы техники
    - типы каталогов
    - страны источников
    """
    with Session(engine) as session:
        groups = [g[0] for g in session.query(Catalog.group_name).distinct().order_by(Catalog.group_name)]
        types = [t[0] for t in session.query(Catalog.catalog_type).distinct().order_by(Catalog.catalog_type)]
        countries = [c[0] for c in session.query(Catalog.source_country).distinct().order_by(Catalog.source_country)]
    return {
        "groups": groups,
        "types": types,
        "countries": countries,
    }
