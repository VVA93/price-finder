import os
import time
import traceback
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://quotes.toscrape.com"
HEADERS = {
    "User-Agent": "PriceFinder/0.1 (+https://github.com/VVA93/price-finder)"
}

def fetch_page(url: str) -> str:
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return r.text
        time.sleep(1 + i)
    raise RuntimeError(f"Failed to fetch {url}, status={r.status_code}")

def parse_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    items = []
    for q in soup.select(".quote"):
        text = q.select_one(".text").get_text(strip=True)
        author = q.select_one(".author").get_text(strip=True)
        tags = [t.get_text(strip=True) for t in q.select(".tag")]
        items.append({"text": text, "author": author, "tags": ", ".join(tags)})
    next_a = soup.select_one("li.next > a")
    next_url = f"{BASE}{next_a.get('href')}" if next_a else None
    return items, next_url

def run() -> str:
    os.makedirs("results", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    log_path = os.path.join("logs", "run.log")
    with open(log_path, "w", encoding="utf-8") as log:
        try:
            url = f"{BASE}/page/1/"
            all_rows = []
            while url:
                log.write(f"Fetch: {url}\n")
                html = fetch_page(url)
                rows, url = parse_page(html)
                log.write(f"Parsed rows: {len(rows)}\n")
                all_rows.extend(rows)

            # превращаем в «офферы» (для теста shop="training-site")
            offers = []
            for r in all_rows:
                offers.append({
                    "shop": "training-site",
                    "title": f"{r['author']}: {r['text'][:50]}",
                    "price": None,            # в учебных данных цены нет
                    "url": BASE               # тоже учебное
                })

            # сохраняем в БД и экспортим Excel
            from db import save_offers, export_to_excel
            save_offers(offers)
            out_path = os.path.join("results", "results.xlsx")
            export_to_excel(out_path)

            print(f"Saved {len(offers)} offers → {out_path}")
            log.write(f"Saved {len(offers)} offers → {out_path}\n")
            return out_path

        except Exception as e:
            err = traceback.format_exc()
            print("ERROR:", e)
            log.write(err + "\n")
            out_path = os.path.join("results", "results.xlsx")
            pd.DataFrame([]).to_excel(out_path, index=False)
            return out_path
