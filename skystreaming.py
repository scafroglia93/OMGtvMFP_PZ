import re
import urllib.parse
import requests
from bs4 import BeautifulSoup, SoupStrainer
import sys
import os
import json
import time
import asyncio
import aiohttp
from dotenv import load_dotenv
load_dotenv()

# Carica le variabili d'ambiente
PROXY_URL = os.getenv("HLSPROXYMFP", "")
SKYSTR = os.getenv("SKYSTR")
PROXY_NOPSW = os.getenv("HLSPROXYMFPNOPSW", "")

# Definizione degli headers per le richieste
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive"
}

async def extract_category_links(session):
    """Estrae tutti i link delle categorie dalla homepage di skystreaming.stream"""
    try:
        main_url = f"https://skystreaming.{SKYSTR}/"
        async with session.get(main_url, headers=headers) as response:
            html_content = await response.text()

        soup = BeautifulSoup(html_content, 'html.parser')
        category_links = []

        # Cerca tutti i link con classe "categories"
        categories = soup.find_all(class_="categories")
        for category in categories:
            links = category.find_all('a')
            for link in links:
                if 'href' in link.attrs:
                    href = link['href'].strip()
                    if href.startswith(f'https://skystreaming.{SKYSTR}/channel/video/'):
                        category_links.append(href)
                    elif href.startswith('/channel/video/'):
                        # Costruisci URL completo se è relativo
                        category_links.append(f"https://skystreaming.{SKYSTR}{href}")

        # Se non troviamo link con classe "categories", cerchiamo tutti i link che contengono "/channel/video/"
        if not category_links:
            all_links = soup.find_all('a')
            for link in all_links:
                if 'href' in link.attrs:
                    href = link['href'].strip()
                    if 'channel/video' in href:
                        if href.startswith('http'):
                            category_links.append(href)
                        else:
                            # Costruisci URL completo se è relativo
                            category_links.append(f"https://skystreaming.{SKYSTR}{href}")

        return category_links
    except Exception as e:
        print(f"Errore durante l'estrazione dei link delle categorie: {e}")
        return []

async def extract_channel_links_from_category(category_url, session):
    """Estrae i link dei canali da una pagina di categoria"""
    try:
        async with session.get(category_url, headers=headers) as response:
            html_content = await response.text()

        soup = BeautifulSoup(html_content, 'html.parser')
        channel_links = []

        # Cerca tutti gli elementi con classe "mediathumb"
        mediathumbs = soup.find_all(class_="mediathumb")
        for thumb in mediathumbs:
            link = thumb.find('a') # Cerca il tag <a> all'interno di mediathumb
            if link and 'href' in link.attrs:
                href = link['href'].strip()
                # Aggiungi tutti i link trovati all'interno di mediathumb
                if href.startswith('http'):
                    channel_links.append(href)
                else:
                    # Costruisci URL completo se è relativo
                    channel_links.append(f"https://skystreaming.{SKYSTR}{href}")

        return channel_links
    except Exception as e:
        print(f"Errore durante l'estrazione dei link dei canali dalla categoria {category_url}: {e}")
        return []

async def get_skystreaming_url(skystreaming_link, session):
    try:
        if "hls" in skystreaming_link:
            m3u8_url = skystreaming_link
            Host = m3u8_url.replace("https://", "").split("/")[0]
            async with session.get(skystreaming_link, headers=headers, allow_redirects=True) as response:
                Origin = str(response.url).split('/embed')[0]
            # Extract channel name from URL when direct hls link
            channel_name = skystreaming_link.split('/')[-1].split('.')[0].replace('-', ' ')  # Added line
            return m3u8_url, Host, Origin, channel_name  # Now returns 4 values

        # Get the full channel name for logging
        #channel_name = skystreaming_link.split('/')[-2]  # Gets 'al-hilal-vs-al-qadisiya' from URL

        async with session.get(skystreaming_link, headers=headers, allow_redirects=True) as response:
            Origin = str(response.url).split('/embed')[0]
            html_content = await response.text()

        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract channel name from h2 tag
        h2_tag = soup.find('h2', {'itemprop': 'name'})
        channel_name = h2_tag.get_text(strip=True) if h2_tag else 'Unknown Channel'

        # Find iframe first
        iframe = soup.find('iframe')
        if iframe and 'src' in iframe.attrs:
            iframe_src = iframe['src']
            # Get the iframe content
            async with session.get(iframe_src, headers=headers) as iframe_response:
                iframe_html = await iframe_response.text()
                iframe_soup = BeautifulSoup(iframe_html, 'html.parser')

                # Find video source in iframe
                source_tag = iframe_soup.find('source', {'type':'application/x-mpegURL'})
                if source_tag and 'src' in source_tag.attrs:
                    m3u8_url = source_tag['src']
                    Host = m3u8_url.replace("https://", "").split("/")[0]
                    print(f"Trovato link m3u8 per {channel_name}")
                    return m3u8_url, Host, Origin, channel_name  # Added 4th return value

        print(f"Nessun link m3u8 trovato nella pagina per {channel_name}")
        return None, None, None, None  # Now returns 4 None values
    except Exception as e:
        print(f"SkyStreaming failed per {channel_name}: {e}")
        return None, None, None, None  # Now returns 4 None values

# Add this near other configuration variables
BASE_URL = "https://skystreaming."  # Note the trailing dot

def generate_proxy_url(m3u8_url, Host, Origin):
    """Genera l'URL proxy con i parametri richiesti"""
    # Codifica l'URL m3u8 per l'uso come parametro
    encoded_link = urllib.parse.quote(m3u8_url)

    # Codifica gli headers necessari
    user_agent = urllib.parse.quote("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
    referer = urllib.parse.quote(f"{BASE_URL}{SKYSTR}/")
    origin = urllib.parse.quote(f"{BASE_URL}{SKYSTR}")
    # Instead of using Origin variable

    # Costruisci l'URL proxy completo
    proxy_url = f"{PROXY_URL}{encoded_link}&h_user-agent={user_agent}&h_referer={referer}&h_origin={origin}"

    return proxy_url

# Mappa per tvg-id personalizzati e definizione dei group-title
SKYSTR_TVG_ID_MAP = {
    "Eurosport 1": "eurosport1.it",
    "Eurosport 2": "eurosport2.it",
    "Sky Sport Tennis": "skysporttennis.it",
    "Sky Sport MotoGP": "skysportmotogp.it",
    "Sky Sport F1": "skysportf1.it",
    "Sky Sport 251": "skysport251.it",
    "Sky Sport 252": "skysport252.it",
    "Sky Sport 253": "skysport253.it",
    "Sky Sport 254": "skysport254.it",
    "Sky Sport 255": "skysport255.it",
    "Sky Sport 256": "skysport256.it",
    "Sky Sport 257": "skysport257.it",
    "Sky Sport 258": "skysport258.it",
    "Sky Sport 259": "skysport259.it",
    "Sky Sport 260": "skysport260.it",
    "Sky Sport 261": "skysport261.it",
    "Sky Sport Max": "skysportmax.it",       # Corrisponde a "Sky Sport Football" nella richiesta originale
    "Sky Sport Arena": "skysportarena.it",
    "Sky Sport Calcio": "skysportcalcio.it",
    "Sky Sport Uno": "skysportuno.it",       # Corrisponde a "Sky Sport UNO" nella richiesta originale
    "Sky Sport 24": "skysport24.it"
}
SKYSTR_SPORT_GROUP_TITLE = "Sport;SkyStreaming"
SKYSTR_DEFAULT_GROUP_TITLE = "Sport;SkyStreaming"

def create_m3u_entry(channel_name, proxy_url):
    info = get_channel_info(channel_name)
    extinf = f'#EXTINF:-1 tvg-id="{info["tvg_id"]}" tvg-name="{info["tvg_name"]}" tvg-logo="{info["tvg_logo"]}" group-title="{info["group_title"]}", {info["tvg_name"]} {info["suffix"]}'
    return f"{extinf}\n{proxy_url}\n\n"

def get_channel_info(channel_name):
    """Returns channel metadata for M3U entries."""
    tvg_id_to_use = SKYSTR_TVG_ID_MAP.get(channel_name, channel_name)
    group_title_to_use = SKYSTR_SPORT_GROUP_TITLE if channel_name in SKYSTR_TVG_ID_MAP else SKYSTR_DEFAULT_GROUP_TITLE

    return {
        "tvg_id": tvg_id_to_use,
        "tvg_name": channel_name, # tvg-name è il nome del canale come "Eurosport 2"
        "tvg_logo": f"https://skystreaming.{SKYSTR}/content/auto_site_logo.png",
        "group_title": group_title_to_use,
        "suffix": "(SS)" # Suffisso per i canali SkyStreaming
    }

def create_m3u_playlist(channels, m3u_file):
    """Crea una nuova playlist M3U con i canali"""
    try:
        # Crea il file M3U con l'intestazione
        with open(m3u_file, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            f.write("# Playlist generata da SkyStreaming\n")

            # Aggiungi tutti i canali
            for channel_name, proxy_url in channels.items():
                entry = create_m3u_entry(channel_name, proxy_url)
                f.write(entry)

        print(f"Creata playlist M3U con {len(channels)} canali: {m3u_file}")
        return True
    except Exception as e:
        print(f"Errore durante la creazione del file M3U {m3u_file}: {e}")
        return False

async def main():
    # Crea una sessione aiohttp
    async with aiohttp.ClientSession() as session:
        # Estrai i link delle categorie
        print("Estraendo i link delle categorie...")
        category_links = await extract_category_links(session)
        print(f"Trovati {len(category_links)} link di categorie.")

        # Estrai i link dei canali da ogni categoria
        all_channel_links = []
        for i, category_url in enumerate(category_links):
            print(f"Processando categoria {i+1}/{len(category_links)}: {category_url}")
            channel_links = await extract_channel_links_from_category(category_url, session)
            all_channel_links.extend(channel_links)
            print(f"Trovati {len(channel_links)} link di canali nella categoria.")

            # Aggiungi un piccolo ritardo per evitare di sovraccaricare il server
            await asyncio.sleep(1)

        # Rimuovi duplicati
        all_channel_links = list(set(all_channel_links))
        print(f"Totale di {len(all_channel_links)} link di canali unici trovati.")

        # Processa ogni canale
        results = {}
        for i, url in enumerate(all_channel_links):
            m3u8_url, Host, Origin, formatted_channel_name = await get_skystreaming_url(url, session)

            if m3u8_url and Host and Origin:
                proxy_url = generate_proxy_url(m3u8_url, Host, Origin)
                results[formatted_channel_name] = proxy_url
                print(f"Generato URL proxy per {formatted_channel_name}")
            else:
                print(f"Impossibile generare URL proxy per {formatted_channel_name}")

            # Aggiungi un piccolo ritardo per evitare di sovraccaricare il server
            await asyncio.sleep(1)

        # Salva i risultati in un file
        output_file = "skystreaming_channels.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            for channel, url in results.items():
                f.write(f"{channel}: {url}\n\n")

        # Salva anche in formato JSON per un uso più facile
        json_output_file = "skystreaming_channels.json"
        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"\nCompletato! Trovati {len(results)} canali con URL proxy validi.")
        print(f"I risultati sono stati salvati in {output_file} e {json_output_file}")

        # Crea la playlist M3U
        m3u_file = "skystreaming.m3u8"
        print(f"\nCreando la playlist M3U: {m3u_file}...")
        if create_m3u_playlist(results, m3u_file):
            print(f"Playlist M3U creata con successo!")
        else:
            print(f"Errore durante la creazione della playlist M3U.")

if __name__ == "__main__":
    asyncio.run(main())
