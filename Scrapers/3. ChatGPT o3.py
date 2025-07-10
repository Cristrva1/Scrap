#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper unificado Inmuebles24 – versión Playwright async (producción)
• 1ª fase: listados  ➜  CSV_A
• 2ª fase: detalles  ➜  CSV_B  (con pestañas dinámicas)
• Concurrencia configurable, reintentos exponenciales, cierre correcto de “pages”
"""

from __future__ import annotations
import argparse, asyncio, os, re, datetime as dt
from pathlib import Path
from typing import Dict, List

import pandas as pd
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ───────────────────── CONFIG ─────────────────────
BASE_DIR    = Path(__file__).resolve().parent
DATA_DIR    = BASE_DIR / "data"; DATA_DIR.mkdir(parents=True, exist_ok=True)
CITY_SLUG   = "zapopan"
UA          = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/125 Safari/537.36")
CONCURRENCY = 4                       # pestañas de detalle simultáneas
PROXY_URL   = os.getenv("PROXY_URL", "")  # si usas proxy rotativo

# ─────────── helpers BeautifulSoup ────────────
def parse_static(html: str) -> Dict[str, str]:
    """Extrae los campos *no dinámicos* de la página de propiedad."""
    soup = BeautifulSoup(html, "html.parser")
    sel  = soup.select_one
    out: Dict[str, str] = {}

    # TODO ─── sustituye / amplía estos ejemplos por todos tus campos
    out["titulo"]  = sel("h1.title-property").get_text(strip=True) if sel("h1.title-property") else ""
    out["precio"]  = sel("div.price-value span").get_text(strip=True) if sel("div.price-value span") else ""
    out["direccion"] = sel("div.section-location-property h4").get_text(strip=True) if sel("div.section-location-property h4") else ""
    # … añade aquí los ~20 campos restantes con la misma estructura try/except …

    return out


def scrape_tabs(page_html: str) -> Dict[str, str]:
    """Devuelve texto de pestañas ‘Características’, ‘Servicios’, ‘Amenidades’…"""
    soup, info = BeautifulSoup(page_html, "html.parser"), {}
    nav = soup.select_one("#reactGeneralFeatures")
    if not nav:
        return info

    for btn in nav.select("button[role='tab']"):
        label = btn.get_text(strip=True).lower()
        panel = btn.find_next("div", attrs={"role": "tabpanel"})
        if not label or not panel:
            continue
        feats = [
            re.sub(r"\s+", " ", t.get_text(" ", strip=True))
            for t in panel.find_all(["span", "li", "p"])
            if t.get_text(strip=True)
        ]
        key = "tab_" + re.sub(r"[^a-z0-9_]+", "", label.replace(" ", "_"))
        info[key] = "; ".join(feats)
    return info


# ───────────── Playwright helpers ──────────────
async def new_browser(pw) -> Browser:
    launch_args = ["--no-sandbox"]
    if PROXY_URL:
        return await pw.chromium.launch(headless=True, args=launch_args, proxy={"server": PROXY_URL})
    return await pw.chromium.launch(headless=True, args=launch_args)


# ──────────────── FASE 1 – LISTADOS ─────────────
async def run_listings(page: Page, pages_to_scrape: int) -> Path | None:
    today   = dt.date.today().isoformat()
    out_dir = DATA_DIR / today; out_dir.mkdir(exist_ok=True)
    csv_path = out_dir / f"listings_{CITY_SLUG}.csv"

    listings: List[Dict[str, str]] = []
    for i in range(1, pages_to_scrape + 1):
        url = f"https://www.inmuebles24.com/departamentos-en-venta-en-{CITY_SLUG}-pagina-{i}.html"
        print(f"[LIST] {i}/{pages_to_scrape} → {url}")
        try:
            await page.goto(url, timeout=45_000)
            # espera explícita a que aparezcan cards
            await page.wait_for_selector("div.postingCardLayout-module__posting-card-layout", timeout=20_000)
            soup = BeautifulSoup(await page.content(), "html.parser")
            for card in soup.select("div.postingCardLayout-module__posting-card-layout"):
                a = card.select_one("h3[data-qa='POSTING_CARD_DESCRIPTION'] a[href]")
                if a and "href" in a.attrs:
                    listings.append({"url": "https://www.inmuebles24.com" + a["href"]})
        except Exception as e:
            print(f"⚠️  error listados: {e}")
            # guarda depuración
            dbg = out_dir / "debug"; dbg.mkdir(exist_ok=True)
            await page.screenshot(path=str(dbg / f"error_list_{i}.png"))
            (dbg / f"error_list_{i}.html").write_text(await page.content(), encoding="utf-8")
            print("  · depuración guardada, deteniendo.")
            break

    if listings:
        pd.DataFrame(listings).to_csv(csv_path, index=False)
        print("✔︎ Listados guardados en", csv_path)
        return csv_path
    print("✖︎ No se obtuvieron listados.")
    return None


# ─────────────── FASE 2 – DETALLES ──────────────
@retry(wait=wait_exponential(multiplier=2), stop=stop_after_attempt(3))
async def fetch_detail(ctx: BrowserContext, url: str) -> Dict[str, str]:
    """Visita una URL y devuelve sus datos; cierra la pestaña luego."""
    page = await ctx.new_page()                 # ←  await obligatorio
    try:
        await page.goto(url, timeout=45_000)
        await page.wait_for_selector("h2.title-type-sup-property", timeout=25_000)

        # clic en pestañas para que se cargue su HTML
        for tab in await page.query_selector_all("#reactGeneralFeatures button[role='tab']"):
            try:
                await tab.click(timeout=2_500)
                await asyncio.sleep(0.25)
            except Exception:
                pass

        html  = await page.content()
        data  = parse_static(html)
        data.update(scrape_tabs(html))
        data["url"] = url
        return data

    finally:
        await page.close()                      # ← libera memoria


async def run_details(browser: Browser, csv_listings: Path):
    out_csv = csv_listings.parent / "detalles_completos.csv"
    urls = pd.read_csv(csv_listings)["url"].dropna().tolist()
    done = set(pd.read_csv(out_csv)["url"]) if out_csv.exists() else set()

    ctx   = await browser.new_context(user_agent=UA)
    sem   = asyncio.Semaphore(CONCURRENCY)
    rows  = []

    async def worker(u):
        async with sem:
            try:
                rows.append(await fetch_detail(ctx, u))
            except Exception as e:
                print(f"⚠️  detalle falló: {e}  {u}")

    tasks = [worker(u) for u in urls if u not in done and "clasificado" in u]
    print(f"[DET] Scraping {len(tasks)} URLs con concurrencia {CONCURRENCY}…")
    await asyncio.gather(*tasks)
    await ctx.close()

    if rows:
        df_new = pd.DataFrame(rows)
        df_final = (pd.concat([pd.read_csv(out_csv), df_new], ignore_index=True)
                    if out_csv.exists() else df_new)
        df_final.to_csv(out_csv, index=False)
        print("✔︎ Detalles guardados en", out_csv)
    else:
        print("ℹ︎ Sin nuevos detalles.")


# ─────────────────────────── MAIN ──────────────────────────
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=3, help="Páginas de listado a scrapear")
    args = parser.parse_args()

    async with async_playwright() as pw:
        browser = await new_browser(pw)
        page    = await browser.new_page()

        csv_a = await run_listings(page, args.pages)
        if csv_a and csv_a.exists():
            await run_details(browser, csv_a)

        await browser.close()
    print("✨ Proceso completado.")


if __name__ == "__main__":
    asyncio.run(main())
