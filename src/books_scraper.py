#!/usr/bin/env python3
"""
books_scraper.py
Scrape 'Books to Scrape' (http://books.toscrape.com/) into CSV + SQLite.

Highlights
- Uses requests + BeautifulSoup
- Correctly resolves relative links with urllib.parse.urljoin
- Polite delay between requests
- Light parsing/cleaning (price, rating, availability)
- Exports to output/books.csv and output/books.db (SQLite)
"""

from __future__ import annotations

import time
import re
import sqlite3
from pathlib import Path
from typing import List, Dict
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "http://books.toscrape.com/"
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = (ROOT / "output")
OUT_DIR.mkdir(exist_ok=True)


# ------------------------- helpers -------------------------

def get_soup(url: str) -> BeautifulSoup:
    """
    HTTP GET with a small delay; returns a BeautifulSoup object.
    Raises for non-2xx responses so failures are visible.
    """
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    time.sleep(0.4)  # be polite
    return BeautifulSoup(resp.text, "lxml")


def parse_price(text: str) -> float:
    """'£53.74' -> 53.74"""
    return float(re.sub(r"[^\d.]", "", text))


def parse_rating(classes: List[str]) -> int | pd._libs.missing.NAType:
    """
    Map rating from CSS class names on <p class="star-rating Three">.
    Returns 1..5 or NA if not found.
    """
    mapping = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    for c in classes:
        if c in mapping:
            return mapping[c]
    return pd.NA


def parse_availability(text: str) -> int | pd._libs.missing.NAType:
    """'In stock (22 available)' -> 22; returns NA if not present."""
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else pd.NA


# ------------------------- scrape core -------------------------

def scrape_books(start_url: str = BASE) -> pd.DataFrame:
    """
    Walk all paginated list pages starting at start_url.
    For each book on the list page:
      - capture title, price, rating, availability, and detail URL
      - follow the detail page to fetch UPC and Category
    Returns a pandas DataFrame of all rows.
    """
    url = start_url
    rows: List[Dict[str, object]] = []

    while True:
        soup = get_soup(url)

        # Each product card
        for art in soup.select("article.product_pod"):
            # Detail link can be relative; urljoin with the *current* page URL
            title = art.h3.a.get("title", "").strip()
            rel_detail = art.h3.a.get("href", "")
            detail_url = urljoin(url, rel_detail)

            price = parse_price(art.select_one("p.price_color").get_text(strip=True))
            rating = parse_rating(art.select_one("p.star-rating")["class"])
            availability_text = art.select_one("p.instock.availability").get_text(" ", strip=True)
            availability = parse_availability(availability_text)

            # Fetch detail page for UPC + Category
            d = get_soup(detail_url)

            # UPC is the first row of the product table
            upc_cell = d.select_one("table.table.table-striped tr:nth-of-type(1) td")
            upc = upc_cell.get_text(strip=True) if upc_cell else ""

            # Category is in breadcrumb: Home > Books > Category > Title
            crumbs = [c.get_text(strip=True) for c in d.select("ul.breadcrumb li a, ul.breadcrumb li.active")]
            category = crumbs[2] if len(crumbs) >= 3 else "Books"

            rows.append({
                "Title": title,
                "Price": price,
                "Rating": rating,
                "Availability": availability,
                "DetailURL": detail_url,
                "UPC": upc,
                "Category": category,
            })

        # Pagination: find the "next" link and resolve properly
        next_a = soup.select_one("li.next a")
        if not next_a:
            break
        url = urljoin(url, next_a.get("href", ""))

    return pd.DataFrame(rows)


# ------------------------- save outputs -------------------------

def save_outputs(df: pd.DataFrame) -> None:
    csv_path = OUT_DIR / "books.csv"
    db_path = OUT_DIR / "books.db"

    # CSV
    df.to_csv(csv_path, index=False)

    # SQLite
    with sqlite3.connect(db_path) as conn:
        df.to_sql("books", conn, if_exists="replace", index=False)

    print(f"Saved CSV    → {csv_path}")
    print(f"Saved SQLite → {db_path}")
    print("\nDataFrame info:")
    print(df.info())
    print("\nTop categories:")
    print(df["Category"].value_counts().head(10))
    print("\nAvg price by rating:")
    print(df.groupby("Rating", dropna=False)["Price"].mean())


# ------------------------- main -------------------------

def main() -> None:
    try:
        df = scrape_books(BASE)
    except requests.HTTPError as e:
        print(f"HTTP error while fetching pages: {e}")
        raise
    except requests.RequestException as e:
        print(f"Network error: {e}")
        raise

    save_outputs(df)


if __name__ == "__main__":
    main()
