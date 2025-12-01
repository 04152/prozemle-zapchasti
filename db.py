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


# --- Пути к файлам ---

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро.xlsx"
DB_FILE = BASE_DIR / "catalogs.db"


# --- Попытка подключить GeoLite2-City для геолокации IP ---

GEOIP_READER = None
try:
    import geoip2.database  # type: ignore

    mmdb_path = BASE_DIR / "GeoLite2-City.mmdb"
    if mmdb_path.exists():
        try:
            GEOIP_READER = geoip2.database.Reader(str(mmdb_path))
            print("GeoIP: GeoLite2-City подключена.")
        except Exception as e:
            print(f"GeoIP: ошибка чтения GeoLite2-City.mmdb: {e}")
    else:
        print("GeoIP: файл GeoLite2-City.mmdb не найден, геолокация отключена.")
except ImportError:
    print("GeoIP: пакет geoip2 не установлен, геолокация отключена.")


# --- Базовый класс SQLAlchemy ---

class Base(DeclarativeBase):
    pass


# --- Таблицы ---

class Catalog(Base):
    __tablename__ = "catalogs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Основные поля
    group_name: Mapped[str] = mapped_column(String(255), index=True)
    models: Mapped[str] = mapped_column(String(255), index=True)
    catalog_type: Mapped[str] = mapped_column(String(100), index=True)
    description: Mapped[str] = mapped_column(String(1000))
    url: Mapped[str] = mapped_column(String(1000))

    # Сведения об источнике
    domain: Mapped[str] = mapped_column(String(255), index=True)
    source_country: Mapped[str] = mapped_column(String(10), index=True, default="")
    catalog_number: Mapped[str] = mapped_column(String(255), default="")
    part_numbers: Mapped[str] = mapped_column(String(1000), default="")

    # Служебные поля
    status: Mapped[str] = mapped_column(String(50), index=True, default="")
    source_type: Mapped[str] = mapped_column(String(50), index=True, default="")
    favorite: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    engineer_note: Mapped[str] = mapped_column(String(1000), default="")

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
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


class AccessLog(Base):
    """Лог всех заходов на сайт (кроме статики)."""

    __tablename__ = "access_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
    )

    path: Mapped[str] = mapped_column(String(255), index=True)
    method: Mapped[str] = mapped_column(String(10))
    ip: Mapped[str] = mapped_column(String(45), index=True)
    user_agent: Mapped[str] = mapped_column(String(500))
    referrer: Mapped[str] = mapped_column(String(500))
    client_id: Mapped[str] = mapped_column(String(64), index=True, default="")
    catalog_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    country: Mapped[str] = mapped_column(String(64), index=True, default="")
    city: Mapped[str] = mapped_column(String(128), index=True, default="")


# --- Движок БД ---

engine = create_engine(f"sqlite:///{DB_FILE}", echo=False, future=True)


def init_db() -> None:
    """Создать все таблицы, если их ещё нет."""
    Base.metadata.create_all(engine)


# --- Настройки импорта и фильтрации доменов ---

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
    """Очень грубо: по домену .by / .ru / прочее."""
    if domain.endswith(".by"):
        return "BY"
    if domain.endswith(".ru"):
        return "RU"
    return "OTHER"


def _parse_favorite(value) -> bool:
    """Понять, что в Excel ячейка означает 'Избранное'."""
    if value is None:
        return False
    s = str(value).strip().lower()
    if not s:
        return False
    return s in {"1", "да", "yes", "true", "y", "д"}


# --- Импорт из Excel в таблицу catalogs ---

def import_from_excel() -> int:
    """
    Полностью пересобирает таблицу catalogs из Excel-файла.
    Возвращает количество загруженных записей.
    """
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Не найден Excel-файл: {DATA_FILE}")

    df = pd.read_excel(DATA_FILE)

    required = {"Группа техники", "Модели", "Тип каталога", "Описание", "Ссылка"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В Excel не хватает столбцов: {', '.join(missing)}")

    df = df.dropna(subset=["Ссылка"]).copy()

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

        catalog_number = str(row.get("Номер каталога", "") or "").strip()
        part_numbers = str(row.get("Каталожные номера", "") or "").strip()

        status = str(row.get("Статус", "") or "").strip()
        if not status:
            status = "актуальный"

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
        session.query(Catalog).delete()
        session.add_all(records)
        session.commit()

    return len(records)


# --- Поиск по каталогам ---

def search_catalogs(
    group: str | None = None,
    model_fragment: str | None = None,
    catalog_type: str | None = None,
    query: str | None = None,
    country_filter: str | None = None,
    favorites_only: bool = False,
) -> list[Catalog]:
    """
    Поиск по таблице catalogs с учётом всех фильтров.
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
                conditions.append(and_(*word_conditions))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.order_by(Catalog.group_name, Catalog.models, Catalog.catalog_type)

        result = session.scalars(stmt).all()
        return list(result)


def get_filter_options() -> dict:
    """
    Наборы значений для выпадающих списков (группа, тип, страна).
    """
    with Session(engine) as session:
        groups = [
            g[0]
            for g in session.query(Catalog.group_name)
            .distinct()
            .order_by(Catalog.group_name)
        ]
        types = [
            t[0]
            for t in session.query(Catalog.catalog_type)
            .distinct()
            .order_by(Catalog.catalog_type)
        ]
        countries = [
            c[0]
            for c in session.query(Catalog.source_country)
            .distinct()
            .order_by(Catalog.source_country)
        ]
    return {
        "groups": groups,
        "types": types,
        "countries": countries,
    }


# --- Операции с одной записью каталога ---

def get_catalog_by_id(catalog_id: int) -> Catalog | None:
    with Session(engine) as session:
        return session.get(Catalog, catalog_id)


def toggle_favorite_flag(catalog_id: int) -> bool | None:
    with Session(engine) as session:
        obj = session.get(Catalog, catalog_id)
        if not obj:
            return None
        obj.favorite = not bool(obj.favorite)
        session.commit()
        return obj.favorite


def update_engineer_note(catalog_id: int, note: str) -> bool:
    with Session(engine) as session:
        obj = session.get(Catalog, catalog_id)
        if not obj:
            return False
        obj.engineer_note = note.strip()
        session.commit()
        return True


# --- Логирование поисков и сохранённые шаблоны ---

def log_search(filters: dict) -> None:
    """
    Сохраняет в SearchLog содержательный поиск (если есть хоть один фильтр).
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
    Создаёт сохранённый шаблон поиска.
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


# --- Логирование кликов по каталогам и общая статистика ---

def record_click(catalog_id: int) -> None:
    with Session(engine) as session:
        session.add(CatalogClickLog(catalog_id=catalog_id))
        session.commit()


def get_usage_stats(limit: int = 10) -> dict:
    """
    Статистика использования каталога (по поискам и кликам по ссылкам).
    """
    with Session(engine) as session:
        total_searches = session.query(func.count(SearchLog.id)).scalar() or 0
        total_saved_queries = session.query(func.count(SavedQuery.id)).scalar() or 0
        total_clicks = session.query(func.count(CatalogClickLog.id)).scalar() or 0

        group_data = (
            session.query(SearchLog.group_name, func.count(SearchLog.id))
            .filter(SearchLog.group_name != "")
            .group_by(SearchLog.group_name)
            .order_by(func.count(SearchLog.id).desc())
            .limit(limit)
            .all()
        )
        group_searches = [{"name": name, "count": cnt} for name, cnt in group_data]

        model_data = (
            session.query(SearchLog.model_fragment, func.count(SearchLog.id))
            .filter(SearchLog.model_fragment != "")
            .group_by(SearchLog.model_fragment)
            .order_by(func.count(SearchLog.id).desc())
            .limit(limit)
            .all()
        )
        model_searches = [{"name": name, "count": cnt} for name, cnt in model_data]

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

        domain_data = (
            session.query(Catalog.domain, func.count(CatalogClickLog.id))
            .join(CatalogClickLog, CatalogClickLog.catalog_id == Catalog.id)
            .filter(Catalog.domain != "")
            .group_by(Catalog.domain)
            .order_by(func.count(CatalogClickLog.id).desc())
            .limit(limit)
            .all()
        )
        domain_clicks = [{"domain": d, "count": cnt} for d, cnt in domain_data]

    return {
        "total_searches": total_searches,
        "total_saved_queries": total_saved_queries,
        "total_clicks": total_clicks,
        "group_searches": group_searches,
        "model_searches": model_searches,
        "top_catalog_clicks": top_catalog_clicks,
        "domain_clicks": domain_clicks,
    }


# --- Геолокация по IP и логирование заходов (access_logs) ---

def lookup_geo(ip: str) -> tuple[str, str]:
    """
    Возвращает (country_iso2, city_name) по IP, если возможно.
    Если база/пакет недоступны или IP локальный — ("", "").
    """
    if not ip:
        return "", ""

    # Локальные/частные сети не геолоцируем
    private_prefixes = (
        "127.",
        "10.",
        "192.168.",
        "172.16.",
        "172.17.",
        "172.18.",
        "172.19.",
        "172.20.",
        "172.21.",
        "172.22.",
        "172.23.",
        "172.24.",
        "172.25.",
        "172.26.",
        "172.27.",
        "172.28.",
        "172.29.",
        "172.30.",
        "172.31.",
    )
    if ip.startswith(private_prefixes):
        return "", ""

    if GEOIP_READER is None:
        return "", ""

    try:
        r = GEOIP_READER.city(ip)
        country = r.country.iso_code or ""
        city = r.city.name or ""
        return country or "", city or ""
    except Exception:
        return "", ""


def add_access_log(
    *,
    path: str,
    method: str,
    ip: str,
    user_agent: str,
    referrer: str,
    client_id: str | None = None,
    catalog_id: int | None = None,
) -> None:
    """
    Добавляет запись в AccessLog, с попыткой определить страну и город.
    """
    country, city = lookup_geo(ip)

    with Session(engine) as session:
        log = AccessLog(
            path=path,
            method=method,
            ip=ip,
            user_agent=user_agent,
            referrer=referrer,
            client_id=client_id or "",
            catalog_id=catalog_id,
            country=country,
            city=city,
        )
        session.add(log)
        session.commit()


def get_last_access_logs(limit: int = 200) -> list[AccessLog]:
    """
    Возвращает последние N записей из журнала посещений.
    """
    with Session(engine) as session:
        stmt = (
            select(AccessLog)
            .order_by(desc(AccessLog.timestamp), desc(AccessLog.id))
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


def get_access_log_stats(limit: int = 20) -> dict:
    """
    Сводка по журналу посещений: всего записей, уникальных IP,
    топ IP, топ путей, топ user-agent, топ стран и городов.
    """
    with Session(engine) as session:
        total_entries = session.query(func.count(AccessLog.id)).scalar() or 0
        unique_ips = session.query(
            func.count(func.distinct(AccessLog.ip))
        ).scalar() or 0

        ip_data = (
            session.query(AccessLog.ip, func.count(AccessLog.id))
            .filter(AccessLog.ip != "")
            .group_by(AccessLog.ip)
            .order_by(func.count(AccessLog.id).desc())
            .limit(limit)
            .all()
        )
        top_ips = [{"ip": ip, "count": cnt} for ip, cnt in ip_data]

        path_data = (
            session.query(AccessLog.path, func.count(AccessLog.id))
            .filter(AccessLog.path != "")
            .group_by(AccessLog.path)
            .order_by(func.count(AccessLog.id).desc())
            .limit(limit)
            .all()
        )
        top_paths = [{"path": p, "count": cnt} for p, cnt in path_data]

        agent_data = (
            session.query(AccessLog.user_agent, func.count(AccessLog.id))
            .filter(AccessLog.user_agent != "")
            .group_by(AccessLog.user_agent)
            .order_by(func.count(AccessLog.id).desc())
            .limit(limit)
            .all()
        )
        top_agents = [{"user_agent": ua, "count": cnt} for ua, cnt in agent_data]

        country_data = (
            session.query(AccessLog.country, func.count(AccessLog.id))
            .filter(AccessLog.country != "")
            .group_by(AccessLog.country)
            .order_by(func.count(AccessLog.id).desc())
            .limit(limit)
            .all()
        )
        top_countries = [{"country": c, "count": cnt} for c, cnt in country_data]

        city_data = (
            session.query(AccessLog.city, func.count(AccessLog.id))
            .filter(AccessLog.city != "")
            .group_by(AccessLog.city)
            .order_by(func.count(AccessLog.id).desc())
            .limit(limit)
            .all()
        )
        top_cities = [{"city": c, "count": cnt} for c, cnt in city_data]

    return {
        "total_entries": total_entries,
        "unique_ips": unique_ips,
        "top_ips": top_ips,
        "top_paths": top_paths,
        "top_agents": top_agents,
        "top_countries": top_countries,
        "top_cities": top_cities,
    }
