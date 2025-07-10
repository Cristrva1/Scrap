import os
import pandas as pd
import datetime as dt
import re
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DDIR = 'data/'

def close_cookie_banner(driver):
    """
    Intenta cerrar o remover el banner de cookies, si está presente,
    para evitar que interfiera con los clics.
    """
    try:
        banner = driver.find_element(By.CSS_SELECTOR, ".CookiesPolicyBanner-module__label___3IraT")
        if banner.is_displayed():
            driver.execute_script("arguments[0].remove();", banner)
            time.sleep(0.5)
    except Exception as e:
        pass

def scrape_property_detail(driver, html):
    soup = BeautifulSoup(html, "html.parser")
    data = {}

    # 1. Tipo de inmueble, área, recámaras y estacionamientos
    h2 = soup.find("h2", class_="title-type-sup-property")
    if h2:
        tokens = [t.strip() for t in h2.get_text(separator="·").split("·") if t.strip()]

        data["tipo_propiedad"] = tokens[0] if len(tokens) > 0 else ""
        print("tipo_propiedad:", data["tipo_propiedad"])
        time.sleep(0.05)

        data["area_m2"] = tokens[1] if len(tokens) > 1 else ""
        print("area_m2:", data["area_m2"])
        time.sleep(0.05)

        if len(tokens) > 2:
            match = re.search(r"(\d+)", tokens[2])
            data["recamaras"] = match.group(1) if match else ""
        else:
            data["recamaras"] = ""
        print("recamaras:", data["recamaras"])
        time.sleep(0.05)

        if len(tokens) > 3:
            match = re.search(r"(\d+)", tokens[3])
            data["estacionamientos"] = match.group(1) if match else ""
        else:
            data["estacionamientos"] = ""
        print("estacionamientos:", data["estacionamientos"])
        time.sleep(0.05)
    else:
        data["tipo_propiedad"] = data["area_m2"] = data["recamaras"] = data["estacionamientos"] = ""
        print("tipo_propiedad:", data["tipo_propiedad"])
        print("area_m2:", data["area_m2"])
        print("recamaras:", data["recamaras"])
        print("estacionamientos:", data["estacionamientos"])
        time.sleep(0.05)

    # 2. Operación, precio y mantenimiento
    price_container = soup.find("div", class_="price-container-property")
    if price_container:
        price_value_div = price_container.find("div", class_="price-value")
        if price_value_div:
            text = price_value_div.get_text(" ", strip=True)
            if "venta" in text.lower():
                data["operacion"] = "venta"
            elif "renta" in text.lower():
                data["operacion"] = "renta"
            else:
                data["operacion"] = ""
            print("operacion:", data["operacion"])
            time.sleep(0.05)

            span_precio = price_value_div.find("span")
            data["precio"] = span_precio.get_text(strip=True) if span_precio else ""
            print("precio:", data["precio"])
            time.sleep(0.05)
        else:
            data["operacion"] = data["precio"] = ""
            print("operacion:", data["operacion"])
            print("precio:", data["precio"])
            time.sleep(0.05)

        extra_div = price_container.find("div", class_="price-extra")
        if extra_div:
            span_mant = extra_div.find("span", class_="price-expenses")
            data["mantenimiento"] = span_mant.get_text(strip=True) if span_mant else ""
        else:
            data["mantenimiento"] = ""
        print("mantenimiento:", data["mantenimiento"])
        time.sleep(0.05)
    else:
        data["operacion"] = data["precio"] = data["mantenimiento"] = ""
        print("operacion:", data["operacion"])
        print("precio:", data["precio"])
        print("mantenimiento:", data["mantenimiento"])
        time.sleep(0.05)

    # 3. Dirección
    location_div = soup.find("div", class_="section-location-property")
    if location_div:
        h4 = location_div.find("h4")
        data["direccion"] = h4.get_text(strip=True) if h4 else ""
    else:
        data["direccion"] = ""
    print("direccion:", data["direccion"])
    time.sleep(0.05)

    # Extraer la URL del mapa estático
    map_container = soup.find("div", class_="static-map-container")
    if map_container:
        img = map_container.find("img", id="static-map")
        if img:
            url = img.get("src", "")
            if url.startswith("//"):
                url = "https:" + url
            data["ubicacion_url"] = url
        else:
            data["ubicacion_url"] = ""
    else:
        data["ubicacion_url"] = ""
    print("ubicacion_url:", data["ubicacion_url"])
    time.sleep(0.05)

    # 4. Título principal
    h1 = soup.find("h1", class_="title-property")
    data["titulo"] = h1.get_text(strip=True) if h1 else ""
    print("titulo:", data["titulo"])
    time.sleep(0.05)

    # 5. Descripción completa
    desc_section = soup.find("section", class_="article-section-description")
    if desc_section:
        long_desc = desc_section.find("div", id="longDescription")
        data["descripcion"] = long_desc.get_text(" ", strip=True) if long_desc else ""
    else:
        data["descripcion"] = ""
    print("descripcion:", data["descripcion"])
    time.sleep(0.05)

    # 7. Información del anunciante
    anunciante = soup.find("h3", attrs={"data-qa": "linkMicrositioAnunciante"})
    data["anunciante"] = anunciante.get_text(strip=True) if anunciante else ""
    print("anunciante:", data["anunciante"])
    time.sleep(0.05)

    # 8. Códigos del anuncio
    codes_section = soup.find("section", id="reactPublisherCodes")
    if codes_section:
        lis = codes_section.find_all("li")
        codigo_anunciante = ""
        codigo_inmuebles24 = ""
        for li in lis:
            text = li.get_text(" ", strip=True)
            if "Cód. del anunciante" in text:
                parts = text.split(":")
                codigo_anunciante = parts[1].strip() if len(parts) > 1 else ""
            elif "Cód. Inmuebles24" in text:
                parts = text.split(":")
                codigo_inmuebles24 = parts[1].strip() if len(parts) > 1 else ""
        data["codigo_anunciante"] = codigo_anunciante
        data["codigo_inmuebles24"] = codigo_inmuebles24
    else:
        data["codigo_anunciante"] = data["codigo_inmuebles24"] = ""
    print("codigo_anunciante:", data["codigo_anunciante"])
    time.sleep(0.05)
    print("codigo_inmuebles24:", data["codigo_inmuebles24"])
    time.sleep(0.05)

    # 9. Tiempo de publicación
    user_views = soup.find("div", id="user-views")
    if user_views:
        p = user_views.find("p")
        data["tiempo_publicacion"] = p.get_text(strip=True) if p else ""
    else:
        data["tiempo_publicacion"] = ""
    print("tiempo_publicacion:", data["tiempo_publicacion"])
    time.sleep(0.05)

    # 10. Información del listado de iconos
    features_ul = soup.find("ul", id="section-icon-features-property")
    if features_ul:
        lis = features_ul.find_all("li", class_="icon-feature")
        data["area_total"] = ""
        data["area_cubierta"] = ""
        data["banos_icon"] = ""
        data["estacionamientos_icon"] = ""
        data["recamaras_icon"] = ""
        data["medio_banos_icon"] = ""
        data["antiguedad_icon"] = ""

        for li in lis:
            icon = li.find("i")
            if not icon:
                continue

            classes = icon.get("class", [])
            # Obtenemos el texto con un espacio en lugar de saltos de línea
            raw_text = li.get_text(" ", strip=True)

            # Opcional: unificar múltiples espacios en uno solo.
            # Esto NO elimina palabras como "baños" ni hace replaces específicos,
            # solo evita que haya muchos espacios o tabs.
            text = re.sub(r"\s+", " ", raw_text).strip()

            # DEBUG para ver qué valor se está capturando
            print("DEBUG => classes:", classes, "| text:", repr(text))

            # Asignamos a cada campo sin reemplazar nada específico.
            if "icon-stotal" in classes:
                data["area_total"] = text
            elif "icon-scubierta" in classes:
                data["area_cubierta"] = text
            elif "icon-bano" in classes:
                data["banos_icon"] = text
            elif "icon-cochera" in classes:
                data["estacionamientos_icon"] = text
            elif "icon-dormitorio" in classes:
                data["recamaras_icon"] = text
            elif "icon-toilete" in classes:
                data["medio_banos_icon"] = text
            elif "icon-antiguedad" in classes:
                data["antiguedad_icon"] = text
    else:
        data["area_total"] = data["area_cubierta"] = data["banos_icon"] = ""
        data["estacionamientos_icon"] = data["recamaras_icon"] = data["medio_banos_icon"] = data["antiguedad_icon"] = ""

    print("area_total:", data["area_total"])
    time.sleep(0.05)
    print("area_cubierta:", data["area_cubierta"])
    time.sleep(0.05)
    print("banos_icon:", data["banos_icon"])
    time.sleep(0.05)
    print("estacionamientos_icon:", data["estacionamientos_icon"])
    time.sleep(0.05)
    print("recamaras_icon:", data["recamaras_icon"])
    time.sleep(0.05)
    print("medio_banos_icon:", data["medio_banos_icon"])
    time.sleep(0.05)
    #print("antiguedad_icon:", data["antiguedad_icon"])
    #time.sleep(0.05)

    return data


def extract_information_after_click(driver):
    info_botones = {}
    try:
        # Ubicar el contenedor principal
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "reactGeneralFeatures"))
        )
        
        # Encontrar los botones dentro del contenedor
        buttons = container.find_elements(By.TAG_NAME, "button")
        print(f"🔎 Se encontraron {len(buttons)} botones. Intentando extraer datos...\n")

        for button in buttons:
            try:
                span_btn = button.find_element(By.TAG_NAME, "span")
                button_text = span_btn.text.strip()
                print(f"➡️ Haciendo clic en: {button_text}")
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", button)
                
                # Esperar hasta que el contenido aparezca después del clic
                try:
                    details_container = WebDriverWait(container, 5).until(
                        EC.presence_of_element_located((By.XPATH, ".//div[2]"))  # Usamos XPath para evitar clases cambiantes
                    )
                except:
                    print(f"⚠️ No se encontró el contenedor de detalles para '{button_text}'. Intentando otra estrategia...")
                    details_container = container.find_elements(By.TAG_NAME, "div")[1]  # Respaldo manual
                
                time.sleep(1)  # Pausa corta para asegurar que el contenido se despliegue

                # Extraer la información dentro del <span>
                features = [elem.text.strip() for elem in details_container.find_elements(By.TAG_NAME, "span") if elem.text.strip()]
                info_botones[button_text] = "; ".join(features)
                
                print(f"📌 Información extraída de '{button_text}': {features}\n")

            except Exception as e:
                print(f"❌ No se pudo extraer información de '{button_text}': {e}\n")
    
    except Exception as e:
        print(f"❌ Error al buscar botones: {e}")
    
    return info_botones

def save(data_dict):
    today_str = dt.date.today().isoformat()
    out_dir = os.path.join(DDIR, today_str)
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, "inmuebles24_terrenos_guadalajara_detalle.csv")

    df_new = pd.DataFrame([data_dict])
    try:
        df_existing = pd.read_csv(fname, encoding="utf-8")
    except FileNotFoundError:
        df_existing = pd.DataFrame()

    final_df = pd.concat([df_existing, df_new], ignore_index=True)

    # Reemplazar saltos de línea en todas las columnas por un espacio
    final_df = final_df.replace(r'[\r\n]+', ' ', regex=True)

    final_df.to_csv(fname, index=False, encoding="utf-8")
    print(f"Datos guardados en: {fname}")


def main():
    # Leer el archivo CSV que contiene las URLs en una columna "url"
    urls_df = pd.read_csv("data/2025-04-25/inmuebles24-guadalajara-terrenos-venta.csv")
    urls = urls_df["url"].tolist()
    
    #for URL in urls:
    for i, URL in enumerate(urls, start=1):
        print(f"Iteración {i} de {len(urls)}: {URL}")
        if "clasificado" not in URL:
            print(f"Saltando URL (no clasificado): {URL}")
            continue
        
        print(f"Navegando a: {URL}")
        
        # Iniciar el navegador dentro del bucle
        options = Options()
        # options.add_argument("--headless")  # Descomentar si deseas modo headless
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(60)
        
        try:
            driver.get(URL)
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h2.title-type-sup-property"))
            )
            # Incrementar un poco el tiempo de espera para evitar bloqueos
            time.sleep(2)
            
            html = driver.page_source
            data = scrape_property_detail(driver, html)
            
            # Extraer información adicional mediante los botones
            botones_data = extract_information_after_click(driver)
            data.update(botones_data)
            
            # Guardar todos los datos en un único CSV
            save(data)
            
        except Exception as e:
            print(f"Error al cargar la página {URL}: {e}")
        
        finally:
            # Cerrar el navegador al terminar cada URL
            driver.quit()
        
        # Agregar un pequeño retraso adicional antes de la siguiente URL
        time.sleep(2)

if __name__ == "__main__":
    main()



    """
    def extract_information_after_click(driver):
    info_botones = {}
    try:
        # Ubicar el contenedor principal
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "reactGeneralFeatures"))
        )
        
        # Encontrar los botones dentro del contenedor
        buttons = container.find_elements(By.TAG_NAME, "button")
        print(f"🔎 Se encontraron {len(buttons)} botones. Intentando extraer datos...\n")

        for button in buttons:
            try:
                span_btn = button.find_element(By.TAG_NAME, "span")
                button_text = span_btn.text.strip()
                print(f"➡️ Haciendo clic en: {button_text}")
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", button)
                
                # Esperar hasta que el contenido aparezca después del clic
                try:
                    details_container = WebDriverWait(container, 5).until(
                        EC.presence_of_element_located((By.XPATH, ".//div[2]"))  # Usamos XPath para evitar clases cambiantes
                    )
                except:
                    print(f"⚠️ No se encontró el contenedor de detalles para '{button_text}'. Intentando otra estrategia...")
                    details_container = container.find_elements(By.TAG_NAME, "div")[1]  # Respaldo manual
                
                time.sleep(1)  # Pausa corta para asegurar que el contenido se despliegue

                # Extraer la información dentro del <span>
                features = [elem.text.strip() for elem in details_container.find_elements(By.TAG_NAME, "span") if elem.text.strip()]
                info_botones[button_text] = "; ".join(features)
                
                print(f"📌 Información extraída de '{button_text}': {features}\n")

            except Exception as e:
                print(f"❌ No se pudo extraer información de '{button_text}': {e}\n")
    
    except Exception as e:
        print(f"❌ Error al buscar botones: {e}")
    
    return info_botones"""