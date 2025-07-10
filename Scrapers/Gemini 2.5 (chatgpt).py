#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Inmuebles24  –  modo “tranquilo” (SeleniumBase, 1 hilo)

• Reintenta suavemente y abandona si Cloudflare devuelve “Attention Required”.
• No usa ningún proxy: pensado para tests manuales IP-única.
"""

from __future__ import annotations
import argparse, os, random, re, time, datetime as dt
from pathlib import Path
from typing import Dict, List

import pandas as pd
from bs4 import BeautifulSoup

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ─────────────── Config básica ────────────────
BASE_URL  = "https://www.inmuebles24.com"
CITY_SLUG = "zapopan"
SEARCH_TMPL = f"{BASE_URL}/departamentos-en-venta-en-{CITY_SLUG}-pagina-{{}}.html"

DATA_DIR  = Path("data/inmuebles24")
DATA_DIR.mkdir(parents=True, exist_ok=True)

UAS = [
    # pequeña rotación – añade más si quieres
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
]

def new_driver() -> Driver:
    ua = random.choice(UAS)
    print(f"→ UA elegido: {ua}")
    drv = Driver(headless=True, uc=True, block_images=True)
    drv.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": ua})
    drv.set_page_load_timeout(60)
    return drv

# ──────────────── helpers de bloqueo ─────────────
def looks_blocked(html: str) -> bool:
    head = html[:2_048].lower()
    return ("attention required" in head and "cloudflare" in head) or "sorry, you have been blocked" in head

# ────────────── Listados ────────────────────────
def scrape_listing_urls(drv: Driver, page_num: int) -> List[str] | None:
    url = SEARCH_TMPL.format(page_num)
    print(f"[LIST] {page_num} → {url}")
    try:
        drv.uc_open_with_reconnect(url, 4)
        html = drv.page_source
        if looks_blocked(html):
            print("⚠️  Cloudflare dice 'Attention Required' → paro suave.")
            return None
        WebDriverWait(drv, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR,
                                            "div.postingCardLayout-module__posting-card-layout"))
        )
        soup = BeautifulSoup(html, "html.parser")
        urls = [BASE_URL + a["href"]
                for a in soup.select("div.postingCardLayout-module__posting-card-layout a[href]")
                if "/propiedades/" in a["href"]]
        print(f"   • {len(urls)} URLs encontradas")
        return urls
    except Exception as e:
        print(f"   ⚠️  Error en página {page_num}: {e}")
        return []

# ───────────── Detalle (estático + tabs) ─────────────
def parse_static(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}
    sel = lambda css: soup.select_one(css)

    try:
        h2 = sel("h2.title-type-sup-property")
        toks = [t.strip() for t in h2.get_text("·").split("·") if t.strip()] if h2 else []
        out["tipo_propiedad"]     = toks[0] if len(toks) > 0 else ""
        out["area_m2"]            = toks[1] if len(toks) > 1 else ""
        out["recamaras"]          = re.search(r"\d+", toks[2]).group() if len(toks) > 2 else ""
        out["estacionamientos"]   = re.search(r"\d+", toks[3]).group() if len(toks) > 3 else ""
    except: pass

    try: out["titulo"]  = sel("h1.title-property").get_text(strip=True)
    except: out["titulo"] = ""

    try:
        price_div = sel("div.price-value span")
        out["precio"] = price_div.get_text(strip=True) if price_div else ""
    except: out["precio"] = ""

    return out

def scrape_detail(drv: Driver, url: str) -> Dict[str, str] | None:
    print(f"[DET] → {url}")
    try:
        drv.uc_open_with_reconnect(url, 4)
        html = drv.page_source
        if looks_blocked(html):
            print("   ⚠️  Detalle bloqueado por Cloudflare – salto.")
            return None
        WebDriverWait(drv, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.title-property"))
        )
        soup = BeautifulSoup(html, "html.parser")
        data = parse_static(soup)
        data["url"] = url

        # Tabs dinámicas
        try:
            cont = drv.find_element(By.ID, "reactGeneralFeatures")
            for btn in cont.find_elements(By.TAG_NAME, "button"):
                label = btn.text.strip()
                if not label:
                    continue
                drv.execute_script("arguments[0].click()", btn)
                time.sleep(0.4)
                panel = btn.find_element(By.XPATH, "..//div[contains(@role,'tabpanel')]")
                feats = [s.text.strip() for s in panel.find_elements(By.TAG_NAME, "span") if s.text.strip()]
                key = re.sub(r"[^a-z0-9_]", "", label.lower().replace(" ", "_"))
                data[key] = "; ".join(feats)
        except Exception:
            pass

        return data
    except Exception as e:
        print(f"   ⚠️  Error detalle: {e}")
        return None

# ──────────────── Guardado incremental ────────────
def save_row(row: Dict[str, str]):
    today = dt.date.today().isoformat()
    out_dir = DATA_DIR / today
    out_dir.mkdir(exist_ok=True)
    fpath = out_dir / f"reporte_detallado_{today}.csv"

    mode = "a" if fpath.exists() else "w"
    pd.DataFrame([row]).to_csv(fpath, mode=mode, header=not fpath.exists(),
                               index=False, encoding="utf-8")

# ─────────────────────────── MAIN ────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=3, help="cuántas páginas de listados")
    ap.add_argument("--from-page", type=int, default=1, help="página inicial")
    args = ap.parse_args()

    drv = new_driver()
    try:
        # ---------- LISTADOS ----------
        all_urls: List[str] = []
        for p in range(args.from_page, args.from_page + args.max_pages):
            urls = scrape_listing_urls(drv, p)
            if urls is None:   # bloqueo
                break
            all_urls.extend(urls)
            time.sleep(random.uniform(2, 5))
        if not all_urls:
            print("Sin URLs para procesar, termina.")
            return
        print(f"→ Total URLs a detalle: {len(all_urls)}")

        # ---------- DETALLES ----------
        for i, u in enumerate(all_urls, 1):
            print(f"[{i}/{len(all_urls)}]", end=" ")
            row = scrape_detail(drv, u)
            if row:
                save_row(row)
            time.sleep(random.uniform(3, 7))

    finally:
        drv.quit()
        print("✔︎ Fin. Driver cerrado.")

if __name__ == "__main__":
    main()
