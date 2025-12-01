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
    Boolean,
    select,
    and_,
    or_,
    desc,
    func,
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

    # Дополнительные поля по источнику
    domain: Mapped[str] = mapped_column(String(255), index=True)
    source_country: Mapped[str] = mapped_column(String(10), index=True, default="")
    catalog_number: Mapped[str] = mapped_column(String(255), default="")
    part_numbers: Mapped[str] = mapped_column(String(1000), default="")

    # Расширенные служебные поля
    status: Mapped[str] = mapped_column(String(50), index=True, default="")
    source_type: Mapped[str] = mapped_column(String(50), index=True, default="")
    favorite: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    engineer_note: Mapped[str] = mapped_column(String(1000), default="")

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class SearchLog(Base):
    __tablename__ = "search_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
    )

    group_name: Mapped[str] = mapped_column(String(255), default="")
    model_fragment: Mapped[str] = mapped_column(String(255), default="")
    catalog_type: Mapped[str] = mapped_column(String(100), default="")
    country: Mapped[str] = mapped_column(String(10), default="")
    query: Mapped[str] = mapped_column(String(1000), default="")


class SavedQuery(Base):
    __tablename__ = "saved_queries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
    )

    title: Mapped[str] = mapped_column(String(255))

    group_name: Mapped[str] = mapped_column(String(255), default="")
    model_fragment: Mapped[str] = mapped_column(String(255), default="")
    catalog_type: Mapped[str] = mapped_column(String(100), default="")
    country: Mapped[str] = mapped_column(String(10), default="")
    query: Mapped[str] = mapped_column(String(1000), default="")


class CatalogClickLog(Base):
    __tablename__ = "catalog_click_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    catalog_id: Mapped[int] = mapped_column(Integer, index=True)
    clicked_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
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
    Грубая эвристика: BY / RU / OTHER.
    """
    if domain.endswith(".by"):
        return "BY"
    if domain.endswith(".ru"):
        return "RU"
    return "OTHER"


def _parse_favorite(value) -> bool:
    """Пытаемся понять, отмечено ли 'Избранное' в Excel."""
    if value is None:
        return False
    s = str(value).strip().lower()
    if not s:
        return False
    return s in {"1", "да", "yes", "true", "y", "д"}


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

        # Дополнительные поля (могут отсутствовать в Excel — тогда будут пустые)
        catalog_number = str(row.get("Номер каталога", "") or "").strip()
        part_numbers = str(row.get("Каталожные номера", "") or "").strip()

        status = str(row.get("Статус", "") or "").strip()
        if not status:
            status = "актуальный"  # значение по умолчанию

        source_type = str(row.get("Источник", "") or "").strip()
        favorite = _parse_favorite(row.get("Избранное"))
        engineer_note = str(row.get("Примечание инженера", "") or "").strip()

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
            status=status,
            source_type=source_type,
            favorite=favorite,
            engineer_note=engineer_note,
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
    favorites_only: bool = False,
) -> list[Catalog]:
    """
    Выполняет поиск по таблице catalogs.

    - group           — точное совпадение по группе техники
    - model_fragment  — подстрока в поле models
    - catalog_type    — точное совпадение типа
    - query           — общий текстовый поиск по нескольким полям
    - country_filter  — фильтр страны источника (BY/RU/OTHER)
    - favorites_only  — если True, показываем только избранные
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

        if favorites_only:
            conditions.append(Catalog.favorite.is_(True))

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


# ---------- Операции с одной записью ----------

def get_catalog_by_id(catalog_id: int) -> Catalog | None:
    with Session(engine) as session:
        return session.get(Catalog, catalog_id)


def toggle_favorite_flag(catalog_id: int) -> bool | None:
    """
    Переключает флаг favorite у записи.
    Возвращает новое значение или None, если запись не найдена.
    """
    with Session(engine) as session:
        obj = session.get(Catalog, catalog_id)
        if not obj:
            return None
        obj.favorite = not bool(obj.favorite)
        session.commit()
        return obj.favorite


def update_engineer_note(catalog_id: int, note: str) -> bool:
    """
    Обновляет примечание инженера.
    Возвращает True, если запись найдена и обновлена.
    """
    with Session(engine) as session:
        obj = session.get(Catalog, catalog_id)
        if not obj:
            return False
        obj.engineer_note = note.strip()
        session.commit()
        return True


# ---------- Логирование запросов и шаблоны ----------

def log_search(filters: dict) -> None:
    """
    Логирует выполненный поиск, если он не полностью пустой.
    filters: словарь с ключами group, model, catalog_type, country, query, favorites_only.
    """
    has_content = any(
        [
            filters.get("group"),
            filters.get("model"),
            filters.get("catalog_type"),
            filters.get("country"),
            filters.get("query"),
        ]
    )
    if not has_content:
        return

    with Session(engine) as session:
        entry = SearchLog(
            group_name=filters.get("group", "") or "",
            model_fragment=filters.get("model", "") or "",
            catalog_type=filters.get("catalog_type", "") or "",
            country=filters.get("country", "") or "",
            query=filters.get("query", "") or "",
        )
        session.add(entry)
        session.commit()


def get_recent_searches(limit: int = 10) -> list[SearchLog]:
    with Session(engine) as session:
        stmt = (
            select(SearchLog)
            .order_by(desc(SearchLog.created_at), desc(SearchLog.id))
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


def create_saved_query(title: str, filters: dict) -> SavedQuery | None:
    """
    Создаёт шаблон запроса. Если запрос полностью пустой — не создаём.
    """
    has_content = any(
        [
            filters.get("group"),
            filters.get("model"),
            filters.get("catalog_type"),
            filters.get("country"),
            filters.get("query"),
        ]
    )
    if not has_content:
        return None

    with Session(engine) as session:
        obj = SavedQuery(
            title=title.strip(),
            group_name=filters.get("group", "") or "",
            model_fragment=filters.get("model", "") or "",
            catalog_type=filters.get("catalog_type", "") or "",
            country=filters.get("country", "") or "",
            query=filters.get("query", "") or "",
        )
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj


def get_saved_queries(limit: int = 20) -> list[SavedQuery]:
    with Session(engine) as session:
        stmt = (
            select(SavedQuery)
            .order_by(desc(SavedQuery.created_at), desc(SavedQuery.id))
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


def get_saved_query_by_id(query_id: int) -> SavedQuery | None:
    with Session(engine) as session:
        return session.get(SavedQuery, query_id)


# ---------- Клики по каталогам и статистика ----------

def record_click(catalog_id: int) -> None:
    """Записываем факт клика по каталогу."""
    with Session(engine) as session:
        session.add(CatalogClickLog(catalog_id=catalog_id))
        session.commit()


def get_usage_stats(limit: int = 10) -> dict:
    """Возвращает агрегированную статистику по поискам и кликам."""
    with Session(engine) as session:
        total_searches = session.query(func.count(SearchLog.id)).scalar() or 0
        total_saved_queries = session.query(func.count(SavedQuery.id)).scalar() or 0
        total_clicks = session.query(func.count(CatalogClickLog.id)).scalar() or 0

        # ТОП групп по поискам
        group_data = (
            session.query(SearchLog.group_name, func.count(SearchLog.id))
            .filter(SearchLog.group_name != "")
            .group_by(SearchLog.group_name)
            .order_by(func.count(SearchLog.id).desc())
            .limit(limit)
            .all()
        )
        group_searches = [
            {"name": name, "count": cnt} for name, cnt in group_data
        ]

        # ТОП моделей по поискам
        model_data = (
            session.query(SearchLog.model_fragment, func.count(SearchLog.id))
            .filter(SearchLog.model_fragment != "")
            .group_by(SearchLog.model_fragment)
            .order_by(func.count(SearchLog.id).desc())
            .limit(limit)
            .all()
        )
        model_searches = [
            {"name": name, "count": cnt} for name, cnt in model_data
        ]

        # ТОП каталогов по кликам
        catalog_data = (
            session.query(Catalog, func.count(CatalogClickLog.id))
            .join(CatalogClickLog, CatalogClickLog.catalog_id == Catalog.id)
            .group_by(Catalog.id)
            .order_by(func.count(CatalogClickLog.id).desc())
            .limit(limit)
            .all()
        )
        top_catalog_clicks = [
            {"catalog": catalog, "count": cnt} for catalog, cnt in catalog_data
        ]

        # ТОП доменов по кликам
        domain_data = (
            session.query(Catalog.domain, func.count(CatalogClickLog.id))
            .join(CatalogClickLog, CatalogClickLog.catalog_id == Catalog.id)
            .filter(Catalog.domain != "")
            .group_by(Catalog.domain)
            .order_by(func.count(CatalogClickLog.id).desc())
            .limit(limit)
            .all()
        )
        domain_clicks = [
            {"domain": d, "count": cnt} for d, cnt in domain_data
        ]

    return {
        "total_searches": total_searches,
        "total_saved_queries": total_saved_queries,
        "total_clicks": total_clicks,
        "group_searches": group_searches,
        "model_searches": model_searches,
        "top_catalog_clicks": top_catalog_clicks,
        "domain_clicks": domain_clicks,
    }
