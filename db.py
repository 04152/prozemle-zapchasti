from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from typing import Optional

import pandas as pd
from sqlalchemy import (
    create_engine,
    String,
    Integer,
    DateTime,
    Boolean,
    Float,
    select,
    and_,
    or_,
    desc,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

# ============================
#  БАЗОВАЯ НАСТРОЙКА
# ============================

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "catalogs.db"
EXCEL_PATH = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро.xlsx"

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    echo=False,
    future=True,
)


class Base(DeclarativeBase):
    pass


# ============================
#  МОДЕЛИ
# ============================


class Catalog(Base):
    __tablename__ = "catalogs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    group_name: Mapped[Optional[str]] = mapped_column(String(200))
    models: Mapped[Optional[str]] = mapped_column(String(300))
    type: Mapped[Optional[str]] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(String(500))

    url: Mapped[str] = mapped_column(String(500))
    domain: Mapped[Optional[str]] = mapped_column(String(200))

    status: Mapped[Optional[str]] = mapped_column(String(50))
    source_type: Mapped[Optional[str]] = mapped_column(String(50))

    is_favorite: Mapped[bool] = mapped_column(Boolean, default=False)
    engineer_note: Mapped[Optional[str]] = mapped_column(String(500))

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class SearchLog(Base):
    __tablename__ = "search_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query: Mapped[Optional[str]] = mapped_column(String(300))
    group_filter: Mapped[Optional[str]] = mapped_column(String(200))
    type_filter: Mapped[Optional[str]] = mapped_column(String(200))
    has_favorite: Mapped[bool] = mapped_column(Boolean, default=False)
    results_count: Mapped[int] = mapped_column(Integer, default=0)

    client_id: Mapped[Optional[str]] = mapped_column(String(100))
    ip: Mapped[Optional[str]] = mapped_column(String(50))
    ua: Mapped[Optional[str]] = mapped_column(String(500))

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class SavedQuery(Base):
    __tablename__ = "saved_queries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(200))
    query: Mapped[Optional[str]] = mapped_column(String(300))
    group_filter: Mapped[Optional[str]] = mapped_column(String(200))
    type_filter: Mapped[Optional[str]] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # --- дополнительные "удобные" свойства, чтобы use_query в app.py работал как есть ---

    @property
    def group_name(self) -> str:
        return self.group_filter or ""

    @property
    def model_fragment(self) -> str:
        # пока нигде не храним фильтр по модели — оставляем пустым
        return ""

    @property
    def catalog_type(self) -> str:
        return self.type_filter or ""

    @property
    def country(self) -> str:
        # страна тоже пока не хранится отдельно
        return ""


class CatalogClickLog(Base):
    __tablename__ = "catalog_click_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    catalog_id: Mapped[int] = mapped_column(Integer)

    client_id: Mapped[Optional[str]] = mapped_column(String(100))
    ip: Mapped[Optional[str]] = mapped_column(String(50))
    ua: Mapped[Optional[str]] = mapped_column(String(500))
    referrer: Mapped[Optional[str]] = mapped_column(String(500))

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class AccessLog(Base):
    __tablename__ = "access_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    path: Mapped[str] = mapped_column(String(200))
    method: Mapped[str] = mapped_column(String(10))
    ip: Mapped[Optional[str]] = mapped_column(String(50))
    ua: Mapped[Optional[str]] = mapped_column(String(500))
    referrer: Mapped[Optional[str]] = mapped_column(String(500))

    country: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))

    client_id: Mapped[Optional[str]] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class PartStock(Base):
    """
    Склад запчастей (остатки).
    """

    __tablename__ = "parts_stock"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    part_number: Mapped[Optional[str]] = mapped_column(String(100))     # номер детали / артикул
    name: Mapped[Optional[str]] = mapped_column(String(300))            # наименование
    group_name: Mapped[Optional[str]] = mapped_column(String(200))      # группа техники
    models: Mapped[Optional[str]] = mapped_column(String(300))          # модели техники (строка)

    quantity: Mapped[Optional[float]] = mapped_column(Float)            # остаток
    min_quantity: Mapped[Optional[float]] = mapped_column(Float)        # минимальный остаток

    location: Mapped[Optional[str]] = mapped_column(String(200))        # место хранения
    status: Mapped[Optional[str]] = mapped_column(String(50))           # например: "в работе", "устарело", ...

    engineer_note: Mapped[Optional[str]] = mapped_column(String(500))   # примечание инженера
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ================================
#  Таблица заявок "На закупку"
# ================================

class PartRequest(Base):
    __tablename__ = "part_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Основная информация о детали
    part_number: Mapped[Optional[str]] = mapped_column(String(100), default=None)
    name: Mapped[Optional[str]] = mapped_column(String(300), default=None)
    model: Mapped[Optional[str]] = mapped_column(String(200), default=None)
    group_name: Mapped[Optional[str]] = mapped_column(String(200), default=None)

    # Связь с каталогом (опционально)
    catalog_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(String(500), default=None)

    # Кто подал заявку
    requester_ip: Mapped[Optional[str]] = mapped_column(String(50), default=None)
    requester_ua: Mapped[Optional[str]] = mapped_column(String(500), default=None)

    # Статус обработки заявки
    # new / in_work / ordered / received / cancelled
    status: Mapped[str] = mapped_column(String(30), default="new", index=True)

    # Комментарий инженера
    note: Mapped[Optional[str]] = mapped_column(String(500), default=None)

    # Дата создания
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


# ============================
#  GEOIP (СТРАНА/ГОРОД ПО IP)
# ============================

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


def lookup_geo(ip: Optional[str]) -> tuple[str, str]:
    """
    Возвращает (country, city) по IP, если база GeoLite2 есть.
    Иначе — ("", "").
    """
    if not ip or not GEOIP_READER:
        return "", ""
    try:
        r = GEOIP_READER.city(ip)
        country = r.country.name or ""
        city = r.city.name or ""
        return country or "", city or ""
    except Exception:
        return "", ""


# ============================
#  ИНИЦИАЛИЗАЦИЯ БД
# ============================

def init_db() -> None:
    Base.metadata.create_all(engine)


# ============================
#  РАБОТА С КАТАЛОГАМИ
# ============================

def _load_excel_catalogs() -> pd.DataFrame:
    """
    Загружает каталоги из Excel в DataFrame.
    Ожидается структура столбцов, как в твоём файле.
    """
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"Excel-файл с каталогами не найден: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH)
    # Подчистим пробелы в названиях столбцов
    df.columns = [str(c).strip() for c in df.columns]
    return df


def refresh_catalogs_from_excel() -> int:
    """
    Полностью пересобирает таблицу catalogs из Excel-файла.
    Возвращает количество загруженных строк.
    """
    df = _load_excel_catalogs()

    # Нормализуем ожидаемые поля
    for col in ["Группа техники", "Модели", "Тип", "Описание", "Ссылка", "Статус", "Источник"]:
        if col not in df.columns:
            df[col] = None

    records: list[Catalog] = []
    for _, row in df.iterrows():
        url = str(row.get("Ссылка", "")).strip()
        if not url:
            continue

        domain = urlsplit(url).netloc or None

        rec = Catalog(
            group_name=str(row.get("Группа техники") or "").strip() or None,
            models=str(row.get("Модели") or "").strip() or None,
            type=str(row.get("Тип") or "").strip() or None,
            description=str(row.get("Описание") or "").strip() or None,
            url=url,
            domain=domain,
            status=str(row.get("Статус") or "").strip() or None,
            source_type=str(row.get("Источник") or "").strip() or None,
        )
        records.append(rec)

    with Session(engine) as session:
        session.query(Catalog).delete()
        session.bulk_save_objects(records)
        session.commit()

    return len(records)


def import_from_excel() -> int:
    """
    Обёртка для удобства — используется в app.py.
    """
    return refresh_catalogs_from_excel()


def _extract_country_code_from_domain(domain: str | None) -> Optional[str]:
    if not domain:
        return None
    parts = domain.lower().split(".")
    if len(parts) < 2:
        return None
    tld = parts[-1]
    return tld  # by, ru, com, ...

def get_catalog_filters() -> dict:
    """
    Возвращает варианты значений для фильтров:
    - группы техники
    - типы
    - страны (по TLD домена: by, ru, com и т.п.)
    """
    with Session(engine) as session:
        groups = [
            g for (g,) in session.execute(
                select(Catalog.group_name)
                .where(Catalog.group_name.is_not(None))
                .distinct()
                .order_by(Catalog.group_name)
            )
        ]
        types = [
            t for (t,) in session.execute(
                select(Catalog.type)
                .where(Catalog.type.is_not(None))
                .distinct()
                .order_by(Catalog.type)
            )
        ]

        domains = session.execute(
            select(Catalog.domain)
            .where(Catalog.domain.is_not(None))
            .distinct()
        ).all()

        country_codes: set[str] = set()
        for (dom,) in domains:
            code = _extract_country_code_from_domain(dom)
            if code:
                country_codes.add(code.upper())

    countries = sorted(country_codes)

    return {
        "groups": groups,
        "types": types,
        "countries": countries,
    }


def get_filter_options() -> dict:
    """
    Старое/универсальное имя для app.py.
    """
    return get_catalog_filters()


def search_catalogs(
    group: Optional[str] = None,
    model_fragment: Optional[str] = None,
    catalog_type: Optional[str] = None,
    query: Optional[str] = None,
    country_filter: Optional[str] = None,
    favorites_only: bool = False,
) -> list[Catalog]:
    """
    Поиск по каталогам под сигнатуру, которую ожидает app.py.
    """
    with Session(engine) as session:
        stmt = select(Catalog)
        conditions = []

        # Поисковая строка
        if query:
            q = f"%{query.strip().lower()}%"
            conditions.append(
                or_(
                    func.lower(Catalog.models).like(q),
                    func.lower(Catalog.description).like(q),
                    func.lower(Catalog.group_name).like(q),
                )
            )

        # Фрагмент модели
        if model_fragment:
            mf = f"%{model_fragment.strip().lower()}%"
            conditions.append(func.lower(Catalog.models).like(mf))

        # Группа техники
        if group:
            conditions.append(Catalog.group_name == group)

        # Тип каталога
        if catalog_type:
            conditions.append(Catalog.type == catalog_type)

        # Фильтр по "стране" (по TLD домена)
        if country_filter:
            cf = country_filter.strip().lower()
            # домен заканчивается на нужный TLD
            like_pattern = f"%.{cf}"
            conditions.append(func.lower(Catalog.domain).like(like_pattern))

        if favorites_only:
            conditions.append(Catalog.is_favorite.is_(True))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.order_by(
            desc(Catalog.is_favorite),
            Catalog.group_name,
            Catalog.models,
        )

        return list(session.scalars(stmt).all())


def toggle_favorite(catalog_id: int) -> bool | None:
    with Session(engine) as session:
        obj = session.get(Catalog, catalog_id)
        if not obj:
            return None
        obj.is_favorite = not obj.is_favorite
        session.commit()
        return obj.is_favorite


def toggle_favorite_flag(catalog_id: int) -> bool | None:
    """
    Обёртка под имя, которое использует app.py.
    """
    return toggle_favorite(catalog_id)


def update_notes(
    catalog_id: int,
    note: Optional[str] = None,
) -> bool:
    with Session(engine) as session:
        obj = session.get(Catalog, catalog_id)
        if not obj:
            return False
        obj.engineer_note = note
        session.commit()
        return True


def update_engineer_note(catalog_id: int, note: str) -> bool:
    """
    Обёртка для app.py.
    """
    return update_notes(catalog_id, note=note)


def get_catalog_by_id(catalog_id: int) -> Optional[Catalog]:
    with Session(engine) as session:
        return session.get(Catalog, catalog_id)


# ============================
#  ЛОГИ ПОИСКА И КЛИКОВ
# ============================

def _log_search_low_level(
    *,
    query: Optional[str],
    group_filter: Optional[str],
    type_filter: Optional[str],
    has_favorite: bool,
    results_count: int,
    client_id: Optional[str],
    ip: Optional[str],
    ua: Optional[str],
) -> None:
    with Session(engine) as session:
        rec = SearchLog(
            query=query,
            group_filter=group_filter,
            type_filter=type_filter,
            has_favorite=has_favorite,
            results_count=results_count,
            client_id=client_id,
            ip=ip,
            ua=ua,
        )
        session.add(rec)
        session.commit()


def log_search(filters: dict) -> None:
    """
    Упрощённый лог поиска под то, как его вызывает app.py:
    log_search(filters)
    """
    query = (filters.get("query") or "").strip() or None
    group_filter = (filters.get("group") or "").strip() or None
    type_filter = (filters.get("catalog_type") or "").strip() or None
    has_favorite = bool(filters.get("favorites_only"))
    # Пока не считаем реальное число результатов — пишем 0
    _log_search_low_level(
        query=query,
        group_filter=group_filter,
        type_filter=type_filter,
        has_favorite=has_favorite,
        results_count=0,
        client_id=None,
        ip=None,
        ua=None,
    )


def log_click(
    *,
    catalog_id: int,
    client_id: Optional[str],
    ip: Optional[str],
    ua: Optional[str],
    referrer: Optional[str],
) -> None:
    with Session(engine) as session:
        rec = CatalogClickLog(
            catalog_id=catalog_id,
            client_id=client_id,
            ip=ip,
            ua=ua,
            referrer=referrer,
        )
        session.add(rec)
        session.commit()


def record_click(catalog_id: int) -> None:
    """
    Обёртка для app.py — минимальный лог клика по каталогу.
    """
    log_click(
        catalog_id=catalog_id,
        client_id=None,
        ip=None,
        ua=None,
        referrer=None,
    )


# ============================
#  СОХРАНЁННЫЕ ЗАПРОСЫ / СТАТИСТИКА
# ============================

def get_recent_queries(limit: int = 10) -> list[SearchLog]:
    with Session(engine) as session:
        stmt = (
            select(SearchLog)
            .order_by(desc(SearchLog.created_at))
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


def get_recent_searches(limit: int = 10) -> list[SearchLog]:
    """
    Старое имя функции, используемое в app.py.
    """
    return get_recent_queries(limit=limit)


def get_saved_queries(limit: Optional[int] = None) -> list[SavedQuery]:
    with Session(engine) as session:
        stmt = select(SavedQuery).order_by(desc(SavedQuery.created_at))
        if limit:
            stmt = stmt.limit(limit)
        return list(session.scalars(stmt).all())


def save_query(
    title: str,
    query: Optional[str],
    group_filter: Optional[str],
    type_filter: Optional[str],
) -> SavedQuery:
    with Session(engine) as session:
        rec = SavedQuery(
            title=title,
            query=query,
            group_filter=group_filter,
            type_filter=type_filter,
        )
        session.add(rec)
        session.commit()
        session.refresh(rec)
        return rec


def create_saved_query(title: str, filters: dict) -> Optional[SavedQuery]:
    """
    Используется в app.py: create_saved_query(title, filters).
    Сохраняем только часть фильтров (query, group, catalog_type),
    чтобы не трогать структуру БД.
    """
    query = (filters.get("query") or "").strip()
    group_filter = (filters.get("group") or "").strip()
    type_filter = (filters.get("catalog_type") or "").strip()

    # Если все фильтры пустые — не сохраняем
    if not any([query, group_filter, type_filter]):
        return None

    return save_query(
        title=title,
        query=query or None,
        group_filter=group_filter or None,
        type_filter=type_filter or None,
    )


def get_saved_query_by_id(saved_id: int) -> Optional[SavedQuery]:
    with Session(engine) as session:
        return session.get(SavedQuery, saved_id)


def delete_saved_query(saved_id: int) -> bool:
    with Session(engine) as session:
        obj = session.get(SavedQuery, saved_id)
        if not obj:
            return False
        session.delete(obj)
        session.commit()
        return True


def get_stats(top_limit: int = 10) -> dict:
    with Session(engine) as session:
        total_catalogs = session.scalar(select(func.count(Catalog.id))) or 0
        total_favorites = session.scalar(
            select(func.count(Catalog.id)).where(Catalog.is_favorite.is_(True))
        ) or 0

        total_searches = session.scalar(select(func.count(SearchLog.id))) or 0

        # Топ запросов
        top_queries = (
            session.execute(
                select(
                    SearchLog.query,
                    func.count(SearchLog.id).label("cnt")
                )
                .where(SearchLog.query.is_not(None))
                .group_by(SearchLog.query)
                .order_by(desc("cnt"))
                .limit(top_limit)
            )
            .all()
        )

        # Топ доменов
        top_domains = (
            session.execute(
                select(
                    Catalog.domain,
                    func.count(Catalog.id).label("cnt")
                )
                .where(Catalog.domain.is_not(None))
                .group_by(Catalog.domain)
                .order_by(desc("cnt"))
                .limit(top_limit)
            )
            .all()
        )

    return {
        "total_catalogs": total_catalogs,
        "total_favorites": total_favorites,
        "total_searches": total_searches,
        "top_queries": top_queries,
        "top_domains": top_domains,
    }


def get_usage_stats(limit: int = 10) -> dict:
    """
    Обёртка для app.py.
    """
    return get_stats(top_limit=limit)


# ============================
#  ЛОГИ ПОСЕЩЕНИЙ (ACCESS_LOG)
# ============================

def add_access_log(
    *,
    path: str,
    method: str,
    ip: Optional[str],
    user_agent: Optional[str],
    referrer: Optional[str],
    client_id: Optional[str],
    catalog_id: Optional[int] = None,  # сейчас не используем, но параметр принимаем
) -> None:
    country, city = lookup_geo(ip)
    with Session(engine) as session:
        rec = AccessLog(
            path=path,
            method=method,
            ip=ip,
            ua=user_agent,
            referrer=referrer,
            country=country or None,
            city=city or None,
            client_id=client_id,
        )
        session.add(rec)
        session.commit()


def get_last_access_logs(limit: int = 200) -> list[AccessLog]:
    with Session(engine) as session:
        stmt = (
            select(AccessLog)
            .order_by(desc(AccessLog.created_at))
            .limit(limit)
        )
        return list(session.scalars(stmt).all())


def get_access_log_stats(limit: int = 20) -> dict:
    with Session(engine) as session:
        last_entries = list(
            session.scalars(
                select(AccessLog)
                .order_by(desc(AccessLog.created_at))
                .limit(limit)
            ).all()
        )

        total_visits = session.scalar(select(func.count(AccessLog.id))) or 0

        per_path = session.execute(
            select(
                AccessLog.path,
                func.count(AccessLog.id).label("cnt")
            )
            .group_by(AccessLog.path)
            .order_by(desc("cnt"))
            .limit(limit)
        ).all()

        per_country = session.execute(
            select(
                AccessLog.country,
                func.count(AccessLog.id).label("cnt")
            )
            .group_by(AccessLog.country)
            .order_by(desc("cnt"))
            .limit(limit)
        ).all()

        per_city = session.execute(
            select(
                AccessLog.city,
                func.count(AccessLog.id).label("cnt")
            )
            .group_by(AccessLog.city)
            .order_by(desc("cnt"))
            .limit(limit)
        ).all()

    return {
        "total_visits": total_visits,
        "last_entries": last_entries,
        "per_path": per_path,
        "per_country": per_country,
        "per_city": per_city,
    }


# ============================
#  СКЛАД: ПОИСК И ФИЛЬТРЫ
# ============================

def search_stock(
    part_number: Optional[str] = None,
    name: Optional[str] = None,
    group: Optional[str] = None,
    status: Optional[str] = None,
    part: Optional[str] = None,
) -> list[PartStock]:
    """
    Поиск по складу. Для совместимости с разными версиями
    параметр номера детали может приходить как part_number, так и part.
    """
    # для совместимости: если part_number пустой, берём part
    if not part_number and part:
        part_number = part

    with Session(engine) as session:
        stmt = select(PartStock)
        conditions = []

        if part_number:
            q = f"%{part_number.strip().lower()}%"
            conditions.append(func.lower(PartStock.part_number).like(q))

        if name:
            qn = f"%{name.strip().lower()}%"
            conditions.append(func.lower(PartStock.name).like(qn))

        if group:
            conditions.append(PartStock.group_name == group)

        if status:
            conditions.append(PartStock.status == status)

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.order_by(PartStock.group_name, PartStock.part_number)

        return list(session.scalars(stmt).all())


def get_stock_filter_options() -> dict:
    with Session(engine) as session:
        groups = [
            g for (g,) in session.execute(
                select(PartStock.group_name)
                .where(PartStock.group_name.is_not(None))
                .distinct()
                .order_by(PartStock.group_name)
            )
        ]
        statuses = [
            s for (s,) in session.execute(
                select(PartStock.status)
                .where(PartStock.status.is_not(None))
                .distinct()
                .order_by(PartStock.status)
            )
        ]

    return {
        "groups": groups,
        "statuses": statuses,
    }


# ============================
#  ЗАЯВКИ "НА ЗАКУПКУ" (PartRequest)
# ============================

def add_part_request(
    *,
    part_number: Optional[str] = None,
    name: Optional[str] = None,
    model: Optional[str] = None,
    group_name: Optional[str] = None,
    catalog_id: Optional[int] = None,
    source_url: Optional[str] = None,
    requester_ip: Optional[str] = None,
    requester_ua: Optional[str] = None,
) -> int:
    """
    Добавляет новую заявку на закупку запчасти.
    Возвращает ID созданной заявки.
    """
    with Session(engine) as session:
        req = PartRequest(
            part_number=part_number,
            name=name,
            model=model,
            group_name=group_name,
            catalog_id=catalog_id,
            source_url=source_url,
            requester_ip=requester_ip,
            requester_ua=requester_ua,
            status="new",
        )
        session.add(req)
        session.commit()
        session.refresh(req)
        return req.id


def get_part_requests(status: Optional[str] = None) -> list[PartRequest]:
    """
    Возвращает список заявок, отсортированных по дате (новые сверху).
    Можно фильтровать по статусу ('new', 'in_work', 'ordered', 'received', 'cancelled').
    """
    with Session(engine) as session:
        stmt = select(PartRequest).order_by(
            desc(PartRequest.created_at), desc(PartRequest.id)
        )

        if status:
            stmt = stmt.where(PartRequest.status == status)

        return list(session.scalars(stmt).all())


def update_request_status(request_id: int, new_status: str) -> bool:
    """
    Обновляет статус заявки. Возвращает True, если заявка найдена и обновлена.
    """
    with Session(engine) as session:
        req = session.get(PartRequest, request_id)
        if not req:
            return False
        req.status = new_status
        session.commit()
        return True
