# validate_links.py
from pathlib import Path
from urllib.parse import urlparse
import time

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро.xlsx"

OUTPUT_CHECKED = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро_проверено.xlsx"
OUTPUT_CLEAN = BASE_DIR / "Каталоги_запчастей_ПроземлеАгро_очищено.xlsx"

BLOCKED_DOMAINS = {
    "machinetechdoc.com",
    "servicepartmanuals.com",
    "interdalnoboy.com",
    "www.avtozapchasty.ru",
    "avtofiles.com",
    "www.niva-club.net",
}


def get_domain(url: str) -> str:
    try:
        return urlparse((url or "").strip()).netloc.lower()
    except Exception:
        return ""


def check_url(url: str, timeout: int = 10) -> dict:
    url = (url or "").strip()
    if not url or not url.startswith("http"):
        return {
            "Статус_ссылки": "bad",
            "Код_ответа": None,
            "Тип_контента": None,
            "Причина": "no_or_bad_url",
        }

    domain = get_domain(url)
    if domain in BLOCKED_DOMAINS:
        return {
            "Статус_ссылки": "bad",
            "Код_ответа": None,
            "Тип_контента": None,
            "Причина": f"blocked_domain:{domain}",
        }

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ProzemleCatalogChecker/1.0)",
        "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8",
    }

    try:
        # сначала пробуем HEAD
        resp = requests.head(
            url, allow_redirects=True, headers=headers, timeout=timeout
        )
        status = resp.status_code
        ctype = resp.headers.get("Content-Type", "")
        final_url = resp.url

        # если сайт не любит HEAD или даёт ошибку — пробуем GET
        if status >= 400 or status in (403, 405):
            resp = requests.get(
                url, allow_redirects=True, headers=headers, timeout=timeout
            )
            status = resp.status_code
            ctype = resp.headers.get("Content-Type", "")
            final_url = resp.url

        if status >= 400:
            return {
                "Статус_ссылки": "bad",
                "Код_ответа": status,
                "Тип_контента": ctype,
                "Причина": "http_error",
            }

        # Простая эвристика: если HTML и на странице явно корзина/оплата/логин — считаем платным/закрытым
        if "text/html" in (ctype or "").lower():
            # осторожно: ограничим объём текста
            text = resp.text[:5000].lower()
            pay_words = [
                "add to cart",
                "buy now",
                "корзина",
                "оформить заказ",
                "оплатить",
                "подписка",
                "sign in",
                "login",
                "логин",
                "вход",
            ]
            if any(w in text for w in pay_words):
                return {
                    "Статус_ссылки": "bad",
                    "Код_ответа": status,
                    "Тип_контента": ctype,
                    "Причина": "probably_paid_or_login",
                }

        return {
            "Статус_ссылки": "ok",
            "Код_ответа": status,
            "Тип_контента": ctype,
            "Причина": "ok",
        }

    except Exception as e:
        return {
            "Статус_ссылки": "bad",
            "Код_ответа": None,
            "Тип_контента": None,
            "Причина": f"exception:{e}",
        }


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Не найден входной файл: {INPUT_FILE}")

    df = pd.read_excel(INPUT_FILE, sheet_name="Sheet1").copy()

    statuses = []
    for i, row in df.iterrows():
        url = str(row.get("Ссылка", "")).strip()
        domain = get_domain(url)
        print(f"[{i+1}/{len(df)}] Проверка: {url} ({domain})")
        info = check_url(url)
        info["Домен"] = domain
        statuses.append(info)

        # чтобы не долбить чужие сайты слишком агрессивно
        time.sleep(0.5)

    status_df = pd.DataFrame(statuses)
    df_out = pd.concat([df.reset_index(drop=True), status_df], axis=1)

    # Сохраним полный отчёт
    df_out.to_excel(OUTPUT_CHECKED, index=False)
    print(f"Полный отчёт сохранён в: {OUTPUT_CHECKED}")

    # И отдельный очищенный вариант только с ok
    df_clean = df_out[df_out["Статус_ссылки"] == "ok"].reset_index(drop=True)
    df_clean.to_excel(OUTPUT_CLEAN, index=False)
    print(f"Очищенный файл сохранён в: {OUTPUT_CLEAN}")
    print(f"Осталось ссылок: {len(df_clean)} из {len(df)}")


if __name__ == "__main__":
    main()
