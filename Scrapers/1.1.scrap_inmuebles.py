#supabase pw "8.g!fdLM5UkA-_w"
import os
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup
from seleniumbase import Driver
import time

DDIR = 'data/'

def scrape_page_source(html):
    columns = ['nombre', 'descripcion', 'ubicacion', 'url', 'precio', 'tipo', 'habitaciones', 'baños']
    data = pd.DataFrame(columns=columns)
    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.find_all("div", class_="postingCardLayout-module__posting-card-layout")

    for card in cards:
        temp_dict = {col: None for col in columns}
        temp_dict['tipo'] = 'venta'
        desc_h3 = card.find("h3", {"data-qa": "POSTING_CARD_DESCRIPTION"})
        if desc_h3:
            link_a = desc_h3.find("a")
            if link_a:
                temp_dict['nombre'] = link_a.get_text(strip=True)
                temp_dict['descripcion'] = link_a.get_text(strip=True)
                temp_dict['url'] = "https://www.inmuebles24.com" + link_a.get('href', '')
        price_div = card.find("div", {"data-qa": "POSTING_CARD_PRICE"})
        if price_div:
            temp_dict['precio'] = price_div.get_text(strip=True)
        address_div = card.find("div", class_="postingLocations-module__location-address")
        address_txt = address_div.get_text(strip=True) if address_div else ""
        loc_h2 = card.find("h2", {"data-qa": "POSTING_CARD_LOCATION"})
        loc_txt = loc_h2.get_text(strip=True) if loc_h2 else ""
        temp_dict['ubicacion'] = f"{address_txt}, {loc_txt}" if address_txt and loc_txt else address_txt or loc_txt
        features = card.find("h3", {"data-qa": "POSTING_CARD_FEATURES"})
        if features:
            for sp in features.find_all("span"):
                txt = sp.get_text(strip=True).lower()
                if "rec" in txt:
                    temp_dict['habitaciones'] = txt
                if "bañ" in txt:
                    temp_dict['baños'] = txt
        data = pd.concat([data, pd.DataFrame([temp_dict])], ignore_index=True)
    return data

def save(df_page):
    today_str = dt.date.today().isoformat()
    out_dir = os.path.join(DDIR, today_str)
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, "inmuebles24-zapopan-departamentos-venta.csv")
    try:
        df_existing = pd.read_csv(fname)
    except FileNotFoundError:
        df_existing = pd.DataFrame()
    final_df = pd.concat([df_existing, df_page], ignore_index=True)
    final_df.to_csv(fname, index=False)
    print(f"Datos guardados en: {fname}")

def main():
    i = 1
    total_urls = 75 # 30 por página
    while i <= total_urls:   
        URL = f'https://www.inmuebles24.com/departamentos-en-venta-en-zapopan-pagina-{i}.html'
        print(f"Iteración {i} of {total_urls}")
        driver = Driver(uc=True)
        i += 1
        try:
            print(f"Navegando a: {URL}")
            driver.uc_open_with_reconnect(URL, 4)
            driver.uc_gui_click_captcha()
            time.sleep(5)  # Esperar a que la página se cargue completamente
            html = driver.page_source
            df_page = scrape_page_source(html)
            save(df_page)
        except Exception as e:
            print(f"Error al cargar la página: {e}")
        finally:
            driver.quit()

if __name__ == "__main__":
    main()