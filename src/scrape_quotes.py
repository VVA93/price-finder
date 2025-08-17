import os
import time
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
    return ""

def parse_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    items = []
    for q in soup.select(".quote"):
        text = q.select_one(".text").get_text(strip=True)
        author = q.select_one(".author").get_text(strip=True)
        tags = [t.get_text(strip=True) for t in q.select(".tag")]
        items.append({"text": text, "author": author, "tags": ", ".join(tags)})
    # ссылка на следующую страницу
    next_a = soup.select_one("li.next > a")
    next_url = BASE + next_a.get("href") if next_a else None
    return items, next_url

def run():
    url = f"{BASE}/page/1/"
    all_rows = []
    while url:
        html = fetch_page(url)
        if not html:
            break
        rows, url = parse_page(html)
        all_rows.extend(rows)

    # Сохраняем результат
    os.makedirs("results", exist_ok=True)
    out_path = os.path.join("results", "results.xlsx")
    pd.DataFrame(all_rows).to_excel(out_path, index=False)
    print(f"Saved {len(all_rows)} rows to {out_path}")

if __name__ == "__main__":
    run()
