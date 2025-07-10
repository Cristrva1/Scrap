import os
import pandas as pd
import datetime as dt
import time
import re
from bs4 import BeautifulSoup
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURACIÓN ---
BASE_URL = "https://www.inmuebles24.com"
SEARCH_URL_TEMPLATE = "https://www.inmuebles24.com/departamentos-en-venta-en-zapopan-pagina-{}.html"
MAX_PAGES = 75
DATA_DIR_BASE = 'data/inmuebles24/'

def setup_driver():
    """Configura e inicia el driver de SeleniumBase en modo headless."""
    print("Configurando el driver de SeleniumBase en modo headless...")
    driver = Driver(headless=True, uc=True, block_images=True)
    driver.set_page_load_timeout(60)
    print("Driver configurado exitosamente.")
    return driver

def scrape_listing_page_urls(driver, page_number):
    """Obtiene todas las URLs de propiedades de una página de listado."""
    page_urls = []
    url = SEARCH_URL_TEMPLATE.format(page_number)
    print(f"\nObteniendo URLs de la página de listado: {url}")
    try:
        driver.uc_open_with_reconnect(url, 4)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "postingCardLayout-module__posting-card-layout")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        cards = soup.find_all("div", class_="postingCardLayout-module__posting-card-layout")
        for card in cards:
            link_a = card.find("a", href=True)
            if link_a:
                page_urls.append(BASE_URL + link_a['href'])
        print(f"Se encontraron {len(page_urls)} propiedades en la página {page_number}.")
    except Exception as e:
        print(f"Error o no hay más propiedades en la página {page_number}: {e}")
    return page_urls

def scrape_property_details(driver, property_url):
    """Función completa que extrae todas las variables de la página de una propiedad."""
    print(f"  -> Scrapeando detalles de: {property_url}")
    property_data = {'url_fuente': property_url}
    try:
        driver.uc_open_with_reconnect(property_url, 4)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.title-property")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # --- INICIO DE LÓGICA DE EXTRACCIÓN ESTÁTICA COMPLETA ---
        
        # 1. Tipo de inmueble, área, recámaras y estacionamientos
        h2 = soup.find("h2", class_="title-type-sup-property")
        if h2:
            tokens = [t.strip() for t in h2.get_text(separator="·").split("·") if t.strip()]
            property_data["tipo_propiedad"] = tokens[0] if len(tokens) > 0 else ""
            property_data["area_m2"] = tokens[1] if len(tokens) > 1 else ""
            if len(tokens) > 2:
                match = re.search(r"(\d+)", tokens[2]); property_data["recamaras"] = match.group(1) if match else ""
            if len(tokens) > 3:
                match = re.search(r"(\d+)", tokens[3]); property_data["estacionamientos"] = match.group(1) if match else ""

        # 2. Operación, precio y mantenimiento
        price_container = soup.find("div", class_="price-container-property")
        if price_container:
            price_value_div = price_container.find("div", class_="price-value")
            if price_value_div:
                text = price_value_div.get_text(" ", strip=True)
                if "venta" in text.lower(): property_data["operacion"] = "venta"
                elif "renta" in text.lower(): property_data["operacion"] = "renta"
                span_precio = price_value_div.find("span"); property_data["precio"] = span_precio.get_text(strip=True) if span_precio else ""
            extra_div = price_container.find("div", class_="price-extra")
            if extra_div:
                span_mant = extra_div.find("span", class_="price-expenses"); property_data["mantenimiento"] = span_mant.get_text(strip=True) if span_mant else ""

        # 3. Dirección y URL de mapa
        location_div = soup.find("div", class_="section-location-property")
        if location_div:
            h4 = location_div.find("h4"); property_data["direccion"] = h4.get_text(strip=True) if h4 else ""
        map_container = soup.find("div", class_="static-map-container")
        if map_container:
            img = map_container.find("img", id="static-map")
            if img and img.get("src"):
                url = img.get("src"); property_data["ubicacion_url"] = "https:" + url if url.startswith("//") else url

        # 4. Título principal y 5. Descripción completa
        h1 = soup.find("h1", class_="title-property"); property_data["titulo"] = h1.get_text(strip=True) if h1 else ""
        desc_section = soup.find("section", class_="article-section-description")
        if desc_section:
            long_desc = desc_section.find("div", id="longDescription"); property_data["descripcion"] = long_desc.get_text(" ", strip=True) if long_desc else ""

        # 7. Información del anunciante
        anunciante = soup.find("h3", attrs={"data-qa": "linkMicrositioAnunciante"}); property_data["anunciante"] = anunciante.get_text(strip=True) if anunciante else ""

        # 8. Códigos del anuncio
        codes_section = soup.find("section", id="reactPublisherCodes")
        if codes_section:
            for li in codes_section.find_all("li"):
                text = li.get_text(" ", strip=True)
                if "Cód. del anunciante" in text: property_data["codigo_anunciante"] = text.split(":")[1].strip() if len(text.split(":")) > 1 else ""
                elif "Cód. Inmuebles24" in text: property_data["codigo_inmuebles24"] = text.split(":")[1].strip() if len(text.split(":")) > 1 else ""
        
        # 9. Tiempo de publicación
        user_views = soup.find("div", id="user-views")
        if user_views:
            p = user_views.find("p"); property_data["tiempo_publicacion"] = p.get_text(strip=True) if p else ""

        # 10. Información del listado de iconos
        features_ul = soup.find("ul", id="section-icon-features-property")
        if features_ul:
            for li in features_ul.find_all("li", class_="icon-feature"):
                icon_class = li.find("i").get("class", []) if li.find("i") else []
                text = re.sub(r"\s+", " ", li.get_text(" ", strip=True)).strip()
                if "icon-stotal" in icon_class: property_data["area_total"] = text
                elif "icon-scubierta" in icon_class: property_data["area_cubierta"] = text
                elif "icon-bano" in icon_class: property_data["banos_icon"] = text
                elif "icon-cochera" in icon_class: property_data["estacionamientos_icon"] = text
                elif "icon-dormitorio" in icon_class: property_data["recamaras_icon"] = text
                elif "icon-toilete" in icon_class: property_data["medio_banos_icon"] = text
                elif "icon-antiguedad" in icon_class: property_data["antiguedad_icon"] = text
        
        # --- LÓGICA DE EXTRACCIÓN DINÁMICA (BOTONES) ---
        try:
            container = driver.find_element(By.ID, "reactGeneralFeatures")
            feature_blocks = container.find_elements(By.XPATH, "./div/div")
            for block in feature_blocks:
                try:
                    button = block.find_element(By.TAG_NAME, "button")
                    button_text = button.text.strip()
                    if button_text:
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.5)
                        details_container = block.find_element(By.TAG_NAME, "div")
                        features = [span.text.strip() for span in details_container.find_elements(By.TAG_NAME, "span") if span.text.strip()]
                        clean_button_text = re.sub(r'[^a-z0-9_]', '', button_text.lower().replace(' ', '_'))
                        property_data[clean_button_text] = "; ".join(features)
                except Exception:
                    pass
        except Exception as e:
            print(f"    -> Advertencia general: No se pudieron encontrar las características dinámicas: {e}")

    except Exception as e:
        print(f"  -> ERROR al obtener detalles de {property_url}: {e}")
        return None
        
    return property_data

def save_data(all_properties_data, base_dir):
    if not all_properties_data:
        print("No hay datos para guardar.")
        return
    today_str = dt.date.today().isoformat()
    out_dir = os.path.join(base_dir, today_str)
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, f"reporte_detallado_{today_str}.csv")
    df = pd.DataFrame(all_properties_data)
    df.to_csv(fname, index=False, encoding="utf-8")
    print(f"\n¡Éxito! {len(df)} registros guardados en: {fname}")

def main():
    driver = setup_driver()
    all_properties_data = []
    try:
        for i in range(1, MAX_PAGES + 1):
            property_urls_on_page = scrape_listing_page_urls(driver, i)
            if not property_urls_on_page:
                print(f"No se obtuvieron más URLs en la página {i}. Terminando proceso.")
                break
            for url in property_urls_on_page:
                details = scrape_property_details(driver, url)
                if details:
                    all_properties_data.append(details)
                time.sleep(2)
            print(f"Fin de la página de listado {i}. Pausa de 5 segundos.")
            time.sleep(5)
    finally:
        print("Cerrando el driver y guardando datos...")
        if driver:
            driver.quit()
        save_data(all_properties_data, DATA_DIR_BASE)

if __name__ == "__main__":
    main()