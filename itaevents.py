import xml.etree.ElementTree as ET
import random
import uuid
import fetcher
import json
import os
import datetime
import pytz
import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import quote_plus  # Add this import
import urllib.parse
import io
from PIL import Image, ImageDraw, ImageFont
from dotenv import load_dotenv
import concurrent.futures
load_dotenv()

PZPROXY = os.getenv("PZPROXY")
GUARCAL = os.getenv("GUARCAL")
DADDY = os.getenv("DADDY")
SKYSTR = os.getenv("SKYSTR")



# Constants
#REFERER = "forcedtoplay.xyz"
#ORIGIN = "forcedtoplay.xyz"
#HEADER = f"&h_user-agent=Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F133.0.0.0+Safari%2F537.36&h_referer=https%3A%2F%2F{REFERER}%2F&h_origin=https%3A%2F%2F{ORIGIN}"
NUM_CHANNELS = 10000
DADDY_LIVE_CHANNELS_URL = 'https://daddylive.dad/24-7-channels.php' # From 247m3u.py
DADDY_JSON_FILE = "daddyliveSchedule.json"
M3U8_OUTPUT_FILE = "itapigz.m3u8"
LOGO = "https://raw.githubusercontent.com/cribbiox/eventi/refs/heads/main/ddsport.png"

# Base URLs for the standard stream checking mechanism (from lista.py)
NEW_KSO_BASE_URLS = [
    "https://new.newkso.ru/wind/",
    "https://new.newkso.ru/ddy6/",
    "https://new.newkso.ru/zeko/",
    "https://new.newkso.ru/nfs/",
    "https://new.newkso.ru/dokko1/",
]
WIKIHZ_TENNIS_BASE_URL = "https://new.newkso.ru/wikihz/"
# Add a cache for logos to avoid repeated requests
LOGO_CACHE = {}

# Add a cache for logos loaded from the local file
LOCAL_LOGO_CACHE = [] # Changed to a list to store URLs directly
LOCAL_LOGO_FILE = "guardacalcio_image_links.txt"
HTTP_REQUEST_TIMEOUT = 10 # Standard timeout for HTTP requests
STREAM_LOCATION_CACHE = {} # Cache for pre-fetched stream locations: dlhd_id -> raw_m3u8_url

# --- Globals for Indexed Stream Paths ---
INDEXED_KSO_PATHS = {} # Stores {stream_id: (base_url, path_segment_from_index)}
INDEXED_TENNIS_PATHS = {} # Stores {tennis_id: path_segment_from_index}
# --- End Globals for Indexed Stream Paths ---

EXCLUDE_KEYWORDS_FROM_CHANNEL_INFO = ["youth", "college"]


# Dizionario per traduzione termini sportivi inglesi in italiano
SPORT_TRANSLATIONS = {
    "soccer": "calcio",
    "football": "football americano",
    "basketball": "basket",
    "tennis": "tennis",
    "swimming": "nuoto",
    "athletics": "atletica",
    "cycling": "ciclismo",
    "golf": "golf",
    "baseball": "baseball",
    "rugby": "rugby",
    "boxing": "boxe",
    "wrestling": "lotta",
    "volleyball": "pallavolo",
    "hockey": "hockey",
    "horse racing": "ippica",
    "motor sports": "automobilismo",
    "motorsports": "automobilismo",
    "gymnastics": "ginnastica",
    "martial arts": "arti marziali",
    "running": "corsa",
    "ice hockey": "hockey su ghiaccio",
    "field hockey": "hockey su prato",
    "water polo": "pallanuoto",
    "weight lifting": "sollevamento pesi",
    "weightlifting": "sollevamento pesi",
    "skiing": "sci",
    "skating": "pattinaggio",
    "ice skating": "pattinaggio su ghiaccio",
    "fencing": "scherma",
    "archery": "tiro con l'arco",
    "climbing": "arrampicata",
    "rowing": "canottaggio",
    "sailing": "vela",
    "surfing": "surf",
    "fishing": "pesca",
    "dancing": "danza",
    "chess": "scacchi",
    "snooker": "biliardo",
    "billiards": "biliardo",
    "darts": "freccette",
    "badminton": "badminton",
    "cricket": "cricket",
    "aussie rules": "football australiano",
    "australian football": "football australiano",
    "cross country": "corsa campestre",
    "biathlon": "biathlon",
    "waterpolo": "pallanuoto",
    "handball": "pallamano"
}

# --- Constants from 247m3u.py ---
STATIC_LOGOS_247 = {
    "sky uno": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-uno-it.png",
    "rai 1": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/rai-1-it.png",
    "rai 2": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/rai-2-it.png",
    "rai 3": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/rai-3-it.png",
    "eurosport 1": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/spain/eurosport-1-es.png",
    "eurosport 2": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/spain/eurosport-2-es.png",
    "italia 1": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/italia1-it.png",
    "la7": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/la7-it.png",
    "la7d": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/la7d-it.png",
    "rai sport": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/rai-sport-it.png",
    "rai premium": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/rai-premium-it.png",
    "sky sports golf": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-golf-it.png",
    "sky sport motogp": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-motogp-it.png",
    "sky sport tennis": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-tennis-it.png",
    "sky sport f1": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-f1-it.png",
    "sky sport football": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-football-it.png",
    "sky sport uno": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-uno-it.png",
    "sky sport arena": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-arena-it.png",
    "sky cinema collection": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-collection-it.png",
    "sky cinema uno": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-uno-it.png",
    "sky cinema action": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-action-it.png",
    "sky cinema comedy": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-comedy-it.png",
    "sky cinema uno +24": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-uno-plus24-it.png",
    "sky cinema romance": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-romance-it.png",
    "sky cinema family": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-family-it.png",
    "sky cinema due +24": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-due-plus24-it.png",
    "sky cinema drama": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-drama-it.png",
    "sky cinema suspense": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-cinema-suspense-it.png",
    "sky sport 24": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-24-it.png",
    "sky sport calcio": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-sport-calcio-it.png",
    "sky sport": "https://play-lh.googleusercontent.com/u7UNH06SU4KsMM4ZGWr7wghkJYN75PNCEMxnIYULpA__VPg8zfEOYMIAhUaIdmZnqw=w480-h960-rw",
    "sky calcio 1": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/germany/sky-select-1-alt-de.png",
    "sky calcio 2": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/germany/sky-select-2-alt-de.png",
    "sky calcio 3": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/germany/sky-select-3-alt-de.png",
    "sky calcio 4": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/germany/sky-select-4-alt-de.png",
    "sky calcio 5": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/germany/sky-select-5-alt-de.png",
    "sky calcio 6": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/germany/sky-select-6-alt-de.png",
    "sky calcio 7": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/germany/sky-select-7-alt-de.png",
    "sky serie": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/sky-serie-it.png",
    "20 mediaset": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/italy/20-it.png",
    "dazn 1": "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a2/DAZN_1_Logo.svg/774px-DAZN_1_Logo.svg.png"
}
STATIC_TVG_IDS_247 = {
    "sky uno": "skyuno.it", "rai 1": "rai1.it", "rai 2": "rai2.it", "rai 3": "rai3.it",
    "eurosport 1": "eurosport1.it", "eurosport 2": "eurosport2.it", "italia 1": "italia1.it",
    "la7": "la7.it", "la7d": "la7d.it", "rai sport": "raisport.it", "rai premium": "raipremium.it",
    "sky sports golf": "skysportgolf.it", "sky sport motogp": "skysportmotogp.it",
    "sky sport tennis": "skysporttennis.it", "sky sport f1": "skysportf1.it",
    "sky sport football": "skysportmax.it", "sky sport uno": "skysportuno.it",
    "sky sport arena": "skysportarena.it", "sky cinema collection": "skycinemacollectionhd.it",
    "sky cinema uno": "skycinemauno.it", "sky cinema action": "skycinemaaction.it",
    "sky cinema comedy": "skycinemacomedy.it", "sky cinema uno +24": "skycinemauno+24.it",
    "sky cinema romance": "skycinemaromance.it", "sky cinema family": "skycinemafamily.it",
    "sky cinema due +24": "skycinemadue+24.it", "sky cinema drama": "skycinemadrama.it",
    "sky cinema suspense": "skycinemasuspense.it", "sky sport 24": "skysport24.it",
    "sky sport calcio": "skysportcalcio.it", "sky calcio 1": "skysport251.it",
    "sky calcio 2": "skysport252.it", "sky calcio 3": "skysport253.it",
    "sky calcio 4": "skysport254.it", "sky calcio 5": "skysport255.it",
    "sky calcio 6": "skysport256.it", "sky calcio 7": "skysport257.it",
    "sky serie": "skyserie.it", "20 mediaset": "20mediasethd.it", "dazn 1": "dazn1.it",
}
STATIC_CATEGORIES_247 = {
    "sky uno": "Sky", "rai 1": "Rai Tv", "rai 2": "Rai Tv", "rai 3": "Rai Tv",
    "eurosport 1": "Sport", "eurosport 2": "Sport", "italia 1": "Mediaset", "la7": "Tv Italia",
    "la7d": "Tv Italia", "rai sport": "Sport", "rai premium": "Rai Tv", "sky sports golf": "Sport",
    "sky sport motogp": "Sport", "sky sport tennis": "Sport", "sky sport f1": "Sport",
    "sky sport football": "Sport", "sky sport uno": "Sport", "sky sport arena": "Sport",
    "sky cinema collection": "Sky", "sky cinema uno": "Sky", "sky cinema action": "Sky",
    "sky cinema comedy": "Sky", "sky cinema uno +24": "Sky", "sky cinema romance": "Sky",
    "sky cinema family": "Sky", "sky cinema due +24": "Sky", "sky cinema drama": "Sky",
    "sky cinema suspense": "Sky", "sky sport 24": "Sport", "sky sport calcio": "Sport",
    "sky calcio 1": "Sport", "sky calcio 2": "Sport", "sky calcio 3": "Sport",
    "sky calcio 4": "Sport", "sky calcio 5": "Sport", "sky calcio 6": "Sport",
    "sky calcio 7": "Sport", "sky serie": "Sky", "20 mediaset": "Mediaset", "dazn 1": "Sport",
}
DEFAULT_247_LOGO = "https://raw.githubusercontent.com/cribbiox/eventi/refs/heads/main/ddlive.png"
DEFAULT_247_GROUP = "24/7 Channels (IT)"
# --- End of Constants from 247m3u.py ---

# Headers for requests
headers = {
    "Accept": "*/*",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6,ru;q=0.5",
    "Priority": "u=1, i",
    "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    "Sec-Ch-UA-Mobile": "?0",
    "Sec-Ch-UA-Platform": "Windows",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Storage-Access": "active",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
}


def load_local_logos():
    """Loads logo links from the local file into a cache."""
    if not LOCAL_LOGO_CACHE: # Load only once
        try:
            with open(LOCAL_LOGO_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line: # Add non-empty lines to the list
                        LOCAL_LOGO_CACHE.append(line)
            print(f"Caricati {len(LOCAL_LOGO_CACHE)} loghi dal file locale: {LOCAL_LOGO_FILE}")
        except FileNotFoundError:
            print(f"File locale dei loghi non trovato: {LOCAL_LOGO_FILE}. Procedo con lo scraping web.")
        except Exception as e:
            print(f"Errore durante il caricamento del file locale dei loghi {LOCAL_LOGO_FILE}: {e}")

def translate_sport_to_italian(sport_key):
    """Traduce i termini sportivi inglesi in italiano"""
    # Pulisce il termine dai tag HTML
    clean_key = re.sub(r'<[^>]+>', '', sport_key).strip().lower()

    # Cerca la traduzione nel dizionario
    if clean_key in SPORT_TRANSLATIONS:
        translated = SPORT_TRANSLATIONS[clean_key]
        # Mantieni la formattazione originale (maiuscole/minuscole)
        return translated.title()

    # Se non trova traduzione, restituisce il termine originale pulito
    return clean_group_title(sport_key)

# Funzione helper per normalizzare i nomi per Skystreaming
def normalize_team_name_for_skystreaming(team_name):
    words_to_remove = ["calcio", "fc", "club", "united", "city", "ac", "sc", "sport", "team"]
    normalized_name = ' '.join(word for word in team_name.split() if word.lower() not in words_to_remove)
    normalized_name = normalized_name.strip()
    if "bayern" in team_name.lower(): return "Bayern"
    if "internazionale" in team_name.lower() or "inter" in team_name.lower(): return "Inter"
    return normalized_name

def get_dynamic_logo(event_name, sport_key=""):
    """
    Cerca immagini per eventi.
    Priorità: Cache memoria -> File Locale -> Skystreaming (per leghe supportate) -> Default.
    """
    # Estrai i nomi delle squadre dall'evento per usarli come chiave di cache
    teams_match = re.search(r':\s*([^:]+?)\s+vs\s+([^:]+?)(?:\s+[-|]|$)', event_name, re.IGNORECASE)

    if not teams_match:
        # Try alternative format "Team1 - Team2"
        teams_match = re.search(r'([^:]+?)\s+-\s+([^:]+?)(?:\s+[-|]|$)', event_name, re.IGNORECASE)

    # Crea una chiave di cache specifica per questa partita
    cache_key = None
    team1_original = None
    team2_original = None

    if teams_match:
        team1_original = teams_match.group(1).strip()
        team2_original = teams_match.group(2).strip()
        cache_key = f"{team1_original} vs {team2_original}"

        # Check if we already have this specific match in LOGO_CACHE (from web scraping)
        if cache_key in LOGO_CACHE:
            print(f"Logo trovato in cache (web) per: {cache_key}")
            return LOGO_CACHE[cache_key]

        load_local_logos() # Ensure local logos are loaded
        if LOCAL_LOGO_CACHE and team1_original and team2_original: # Ensure teams were extracted
            # Normalize team names for local file lookup (replace spaces with hyphens)
            team1_norm_local = team1_original.lower().replace(" ", "-")
            team2_norm_local = team2_original.lower().replace(" ", "-")

            for logo_url in LOCAL_LOGO_CACHE:
                logo_url_lower = logo_url.lower()
                # Check if both normalized team names are in the URL (case-insensitive)
                if team1_norm_local in logo_url_lower and team2_norm_local in logo_url_lower:
                     print(f"Logo trovato nel file locale per: {cache_key} -> {logo_url}")
                     # Add to main cache for future use
                     if cache_key:
                         LOGO_CACHE[cache_key] = logo_url
                     return logo_url
                # Check if at least one normalized team name is in the URL (partial match fallback)
                elif team1_norm_local in logo_url_lower or team2_norm_local in logo_url_lower:
                     print(f"Logo parziale trovato nel file locale per: {cache_key} -> {logo_url}")
                     # Add to main cache for future use
                     if cache_key:
                         LOGO_CACHE[cache_key] = logo_url
                     return logo_url

    # Verifica se l'evento è di Serie A o altre leghe supportate
    is_supported_league = any(league in event_name for league in [
        "Italy - Serie A", "La Liga", "Premier League", "Bundesliga", "Ligue 1",
        "Italy - Serie B", "Italy - Serie C", "UEFA Champions League",
        "UEFA Europa League", "Conference League", "Coppa Italia"
    ])

    if is_supported_league and team1_original and team2_original: # Richiede team estratti per Skystreaming
        print(f"Evento di lega supportata rilevato: {event_name}. Tentativo Skystreaming.")
        team1_norm_sky = normalize_team_name_for_skystreaming(team1_original)
        team2_norm_sky = normalize_team_name_for_skystreaming(team2_original)
        print(f"Squadre normalizzate per Skystreaming: '{team1_norm_sky}' vs '{team2_norm_sky}'")

        try:
            skystreaming_base_url = f"https://skystreaming.{SKYSTR}/"
            skystreaming_url_path = ""
            if "Italy - Serie A :" in event_name: skystreaming_url_path = "channel/video/serie-a"
            elif "La Liga :" in event_name: skystreaming_url_path = "channel/video/la-liga"
            elif "Premier League :" in event_name: skystreaming_url_path = "channel/video/english-premier-league"
            elif "Bundesliga :" in event_name: skystreaming_url_path = "channel/video/bundesliga"
            elif "Ligue 1 :" in event_name: skystreaming_url_path = "channel/video/ligue-1"
            elif "Italy - Serie B :" in event_name: skystreaming_url_path = "channel/video/a-serie-b"
            elif "Italy - Serie C :" in event_name: skystreaming_url_path = "channel/video/italia-serie-c"
            # Aggiungere qui altre leghe se necessario per Skystreaming

            urls_to_try_sky = []
            if skystreaming_url_path:
                urls_to_try_sky.append(f"{skystreaming_base_url}{skystreaming_url_path}")
            urls_to_try_sky.append(skystreaming_base_url) # Prova sempre la homepage

            headers_skystreaming = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }

            for current_url in set(urls_to_try_sky): # set per evitare doppioni se path è vuoto
                print(f"Cercando logo su {current_url}...")
                response = requests.get(current_url, headers=headers_skystreaming, timeout=10)
                soup = BeautifulSoup(response.text, 'html.parser')
                media_spans = soup.find_all('span', class_='mediabg')
                print(f"Trovati {len(media_spans)} span con class='mediabg' su {current_url}")

                for span in media_spans: # Corrispondenza esatta
                    span_text_lower = span.text.lower()
                    if (team1_norm_sky.lower() in span_text_lower and team2_norm_sky.lower() in span_text_lower) or \
                       (team1_original.lower() in span_text_lower and team2_original.lower() in span_text_lower):
                        style = span.get('style', '')
                        if 'background-image:url(' in style:
                            match_bg = re.search(r'background-image:url\((.*?)\)', style)
                            if match_bg:
                                logo_url = match_bg.group(1)
                                print(f"Trovato logo specifico (esatto) su {current_url}: {logo_url}")
                                if cache_key: LOGO_CACHE[cache_key] = logo_url
                                return logo_url
                for span in media_spans: # Corrispondenza parziale
                    span_text_lower = span.text.lower()
                    if (team1_norm_sky.lower() in span_text_lower or team2_norm_sky.lower() in span_text_lower) or \
                       (team1_original.lower() in span_text_lower or team2_original.lower() in span_text_lower):
                        style = span.get('style', '')
                        if 'background-image:url(' in style:
                            match_bg = re.search(r'background-image:url\((.*?)\)', style)
                            if match_bg:
                                logo_url = match_bg.group(1)
                                print(f"Trovato logo specifico (parziale) su {current_url}: {logo_url}")
                                if cache_key: LOGO_CACHE[cache_key] = logo_url
                                return logo_url
            print(f"Nessun logo trovato su Skystreaming per {event_name}.")
        except Exception as e_sky:
            print(f"Errore durante lo scraping da Skystreaming per {event_name}: {e_sky}")

    print(f"Nessun logo specifico trovato per {event_name}, uso il logo di default.")
    if cache_key:
        LOGO_CACHE[cache_key] = LOGO
    return LOGO


def generate_unique_ids(count, seed=42):
    random.seed(seed)
    return [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(count)]

def loadJSON(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        return json.load(file)

# Modify the existing get_stream_link function to use the new logic
def get_stream_link(dlhd_id, event_name="", channel_name="", max_retries=3):
    print(f"Getting stream link for channel ID: {dlhd_id} - {event_name} on {channel_name}...")

    raw_m3u8_url_found = None
    daddy_headers_str = "&h_user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    attempted_urls_for_id_live = [] # Initialize here for all live discovery paths

    # 1. Check pre-fetched cache first
    raw_m3u8_url_found = STREAM_LOCATION_CACHE.get(dlhd_id)

    if raw_m3u8_url_found:
        print(f"[✓] Stream for ID {dlhd_id} found in pre-fetch cache: {raw_m3u8_url_found}")
    else: # Not in pre-fetch cache
        if not dlhd_id.startswith(str(uuid.uuid4())[:5]): # Avoid live discovery for generated UUIDs if they are identifiable
            print(f"[!] ID {dlhd_id} not in pre-fetch cache or is a UUID. Attempting live discovery...")
            # Fallback to original discovery logic
            is_tennis_channel = channel_name and ("Tennis Stream" in channel_name or "Tennis Channel" in channel_name)
            should_try_tennis_url = is_tennis_channel or \
                                    (dlhd_id.startswith("15") and len(dlhd_id) == 4)

            if should_try_tennis_url:
                if not is_tennis_channel and dlhd_id.startswith("15") and len(dlhd_id) == 4:
                    print(f"[INFO] Channel ID {dlhd_id} matches 15xx pattern. Attempting tennis-specific URL (live).")
                if len(dlhd_id) >= 2:
                    # Modifica per rimuovere lo zero iniziale se il numero è < 10
                    last_digits_str = dlhd_id[-2:]
                    try:
                        last_digits_int = int(last_digits_str)
                        tennis_stream_path = f"wikiten{last_digits_int}/mono.m3u8" # Es. wikiten5/mono.m3u8
                    except ValueError: # Fallback se non è un numero, anche se zfill(2) dovrebbe prevenirlo
                        tennis_stream_path = f"wikiten{last_digits_str.zfill(2)}/mono.m3u8" # Es. wikiten05/mono.m3u8
                    candidate_url_tennis = f"{WIKIHZ_TENNIS_BASE_URL.rstrip('/')}/{tennis_stream_path.lstrip('/')}"
                    attempted_urls_for_id_live.append(candidate_url_tennis)
                    try:
                        response = requests.get(candidate_url_tennis, stream=True, timeout=HTTP_REQUEST_TIMEOUT) # Changed candidate_url to candidate_url_tennis
                        if response.status_code == 200:
                            print(f"[✓] Stream TENNIS (or 15xx ID) found for channel ID {dlhd_id} at: {candidate_url_tennis} (live discovery)")
                            raw_m3u8_url_found = candidate_url_tennis # Changed candidate_url to candidate_url_tennis
                        response.close()
                    except requests.exceptions.RequestException: pass
                else:
                    print(f"[WARN] Channel ID {dlhd_id} is too short for tennis logic (live discovery).")

            if not raw_m3u8_url_found: # Only if tennis check failed or wasn't applicable
                for base_url in NEW_KSO_BASE_URLS:
                    stream_path = f"premium{dlhd_id}/mono.m3u8"
                    candidate_url_kso = f"{base_url.rstrip('/')}/{stream_path.lstrip('/')}"
                    attempted_urls_for_id_live.append(candidate_url_kso)
                    try:
                        response = requests.get(candidate_url_kso, stream=True, timeout=HTTP_REQUEST_TIMEOUT)
                        if response.status_code == 200:
                            print(f"[✓] Stream found for channel ID {dlhd_id} at: {candidate_url_kso} (live discovery)")
                            raw_m3u8_url_found = candidate_url_kso # Changed candidate_url to candidate_url_kso
                            response.close(); break
                        response.close()
                    except requests.exceptions.RequestException: pass

            if raw_m3u8_url_found:
                STREAM_LOCATION_CACHE[dlhd_id] = raw_m3u8_url_found # Cache it if found via fallback live discovery
            else:
                print(f"[✗] No stream found for channel ID {dlhd_id} after live discovery.")
                return None
        else: # ID was likely a UUID, and not found in cache.
            print(f"[✗] ID {dlhd_id} (likely UUID) not found in cache and skipped live discovery.")
            return None

    # Apply proxy and headers if raw_m3u8_url_found is not None
    if raw_m3u8_url_found:
        url_with_headers = raw_m3u8_url_found + daddy_headers_str
        # get_stream_link will now consistently return the url with daddy_headers_str.
        return url_with_headers

    return None # Should be caught earlier if raw_m3u8_url_found was None

# --- Functions for Fetching and Parsing Index Pages ---
def fetch_and_parse_single_index_page(base_url, stream_type, http_headers):
    """
    Fetches and parses a single index page (e.g., a KSO base URL or WIKIHZ base URL).
    Populates INDEXED_KSO_PATHS or INDEXED_TENNIS_PATHS.
    stream_type: 'kso' or 'tennis'
    """
    print(f"Fetching index page: {base_url} for type: {stream_type}")
    try:
        response = requests.get(base_url, headers=http_headers, timeout=HTTP_REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)

        count_added = 0
        for link in links:
            href = link.get('href')
            if not href or href == "../" or href == "..": # Ignore irrelevant links
                continue

            path_segment_from_index = href.strip('/') # e.g., "premium123" or "wikiten5"

            if stream_type == 'kso':
                match = re.fullmatch(r'premium(\d+)', path_segment_from_index)
                if match:
                    stream_id = match.group(1)
                    # Store the first base_url where this stream_id is found
                    if stream_id not in INDEXED_KSO_PATHS:
                        INDEXED_KSO_PATHS[stream_id] = (base_url, href) # Store original href with slashes if present
                        count_added +=1
            elif stream_type == 'tennis':
                match = re.fullmatch(r'wikiten(\d+)', path_segment_from_index)
                if match:
                    # For tennis, the ID is the numeric part, e.g., '5' from 'wikiten5'
                    tennis_short_id = match.group(1)
                    if tennis_short_id not in INDEXED_TENNIS_PATHS:
                        INDEXED_TENNIS_PATHS[tennis_short_id] = href # Store original href
                        count_added += 1
        if count_added > 0:
            print(f"  Added {count_added} unique stream paths from {base_url}")

    except requests.exceptions.RequestException as e_req:
        print(f"  Error fetching or parsing index page {base_url}: {e_req}")
    except Exception as e_gen:
        print(f"  Generic error processing index page {base_url}: {e_gen}")

def fetch_all_index_pages():
    """Calls fetch_and_parse_single_index_page for all relevant base URLs."""
    print("\nFetching and parsing all index pages for stream discovery...")

    # For KSO streams (iterate through each defined base URL)
    for kso_base_url in NEW_KSO_BASE_URLS:
        fetch_and_parse_single_index_page(kso_base_url, 'kso', headers) # Assuming 'headers' is global

    # For Tennis streams (single base URL)
    fetch_and_parse_single_index_page(WIKIHZ_TENNIS_BASE_URL, 'tennis', headers)

    print("Finished fetching and parsing index pages.")
    print(f"  Total unique KSO stream IDs found in indices: {len(INDEXED_KSO_PATHS)}")
    print(f"  Total unique Tennis stream IDs found in index: {len(INDEXED_TENNIS_PATHS)}")
    # print(f"DEBUG KSO Index: {INDEXED_KSO_PATHS}") # Optional: for debugging
    # print(f"DEBUG Tennis Index: {INDEXED_TENNIS_PATHS}") # Optional: for debugging
# --- End of Index Page Functions ---


def _discover_single_id_location(id_info_tuple):
    """
    Worker for pre-caching. Finds the raw m3u8 URL for a single dlhd_id.
    id_info_tuple: (dlhd_id, channel_name_for_tennis_logic)
    Returns: (dlhd_id, found_raw_url) or (dlhd_id, None)
    """
    dlhd_id, channel_name_for_tennis_logic = id_info_tuple
    raw_m3u8_url = None
    attempted_urls_for_id_precaching = []

    # --- 1. Try Tennis URL Logic (using index first, then direct if needed) ---
    is_tennis_channel = channel_name_for_tennis_logic and \
                             ("Tennis Stream" in channel_name_for_tennis_logic or \
                              "Tennis Channel" in channel_name_for_tennis_logic)

    # Determine the potential short ID for tennis lookup (e.g., "5" from "wikiten5")
    # This could be the dlhd_id itself if it's short, or derived from it.
    potential_tennis_short_id = None
    if dlhd_id.isdigit(): # If dlhd_id is purely numeric
        if len(dlhd_id) <= 2: # e.g. "5", "12"
            potential_tennis_short_id = dlhd_id
        elif len(dlhd_id) > 2 : # e.g. "1505" -> try "05" -> "5"
            potential_tennis_short_id = dlhd_id[-2:].lstrip('0') if len(dlhd_id[-2:]) > 0 else dlhd_id[-1:]

    tennis_path_segment_from_index = None
    if potential_tennis_short_id and potential_tennis_short_id in INDEXED_TENNIS_PATHS:
        tennis_path_segment_from_index = INDEXED_TENNIS_PATHS[potential_tennis_short_id]

    if tennis_path_segment_from_index:
        # Path found in index, construct URL and verify
        # tennis_path_segment_from_index includes "wikitenX", ensure no double "wikitenX"
        candidate_url_tennis = f"{WIKIHZ_TENNIS_BASE_URL.rstrip('/')}/{tennis_path_segment_from_index.strip('/')}/mono.m3u8"
        attempted_urls_for_id_precaching.append(f"[TENNIS_INDEX] {candidate_url_tennis}")
        try:
            response = requests.get(candidate_url_tennis, stream=True, timeout=HTTP_REQUEST_TIMEOUT)
            if response.status_code == 200:
                raw_m3u8_url = candidate_url_tennis
            response.close()
        except requests.exceptions.RequestException: pass

    # Fallback to original tennis logic if not found via index or if index check failed,
    # and if it's a likely tennis channel by name or ID pattern.
    if not raw_m3u8_url and (is_tennis_channel or (dlhd_id.startswith("15") and len(dlhd_id) == 4)):
        # This part of the original logic tries to guess the path based on dlhd_id's last digits
        # This might be redundant if the index is comprehensive, but keep as fallback.
        guessed_tennis_short_id_str = dlhd_id[-2:] # e.g. "05" from "1505"
        try:
            guessed_tennis_short_id_int = int(guessed_tennis_short_id_str) # Convert to int to remove leading zero for path
            tennis_stream_path_guessed = f"wikiten{guessed_tennis_short_id_int}/mono.m3u8"
        except ValueError: # Fallback if not purely numeric
            try:
                # Try with zfill if original was like "5" -> "05"
                tennis_stream_path_guessed = f"wikiten{guessed_tennis_short_id_str.zfill(2)}/mono.m3u8"
            except: # Ultimate fallback, probably won't work
                tennis_stream_path_guessed = f"wikiten{guessed_tennis_short_id_str}/mono.m3u8"

        candidate_url_tennis_direct = f"{WIKIHZ_TENNIS_BASE_URL.rstrip('/')}/{tennis_stream_path_guessed.lstrip('/')}"
        # Avoid re-attempt if index already tried this exact URL (unlikely to be exact match here due to path construction)
        is_already_attempted = any(candidate_url_tennis_direct in url for url in attempted_urls_for_id_precaching if "[TENNIS_INDEX]" in url)
        if not is_already_attempted:
             attempted_urls_for_id_precaching.append(f"[TENNIS_DIRECT_GUESS] {candidate_url_tennis_direct}")
             try:
                 response = requests.get(candidate_url_tennis_direct, stream=True, timeout=HTTP_REQUEST_TIMEOUT)
                 if response.status_code == 200:
                     raw_m3u8_url = candidate_url_tennis_direct
                 response.close()
             except requests.exceptions.RequestException: pass

    # --- 2. Try Standard KSO Base URLs Logic (using index first, then direct) ---
    if not raw_m3u8_url:
        indexed_kso_info = INDEXED_KSO_PATHS.get(dlhd_id) # dlhd_id is the key like "123"
        if indexed_kso_info:
            kso_base_url_from_index, kso_path_segment_from_index = indexed_kso_info
            # kso_path_segment_from_index is like "premium123/" or "premium123"
            candidate_url_kso_indexed = f"{kso_base_url_from_index.rstrip('/')}/{kso_path_segment_from_index.strip('/')}/mono.m3u8"
            attempted_urls_for_id_precaching.append(f"[KSO_INDEX] {candidate_url_kso_indexed}")
            try:
                response = requests.get(candidate_url_kso_indexed, stream=True, timeout=HTTP_REQUEST_TIMEOUT)
                if response.status_code == 200:
                    raw_m3u8_url = candidate_url_kso_indexed
                response.close()
            except requests.exceptions.RequestException: pass

        # Fallback to original KSO direct attempts if not found via index or if index check failed
        if not raw_m3u8_url:
            for base_url_kso_fallback in NEW_KSO_BASE_URLS:
                stream_path_direct = f"premium{dlhd_id}/mono.m3u8"
                candidate_url_kso_direct = f"{base_url_kso_fallback.rstrip('/')}/{stream_path_direct.lstrip('/')}"

                # Avoid re-attempting if this exact URL was already tried via index
                is_already_attempted_via_index = False
                if indexed_kso_info:
                    # Reconstruct the URL that would have been tried from index to compare
                    idx_base, idx_path = indexed_kso_info
                    if idx_base == base_url_kso_fallback and f"{idx_path.strip('/')}/mono.m3u8" == stream_path_direct:
                         is_already_attempted_via_index = True

                if not is_already_attempted_via_index:
                    attempted_urls_for_id_precaching.append(f"[KSO_DIRECT_GUESS] {candidate_url_kso_direct}")
                    try:
                        response = requests.get(candidate_url_kso_direct, stream=True, timeout=HTTP_REQUEST_TIMEOUT)
                        if response.status_code == 200:
                            raw_m3u8_url = candidate_url_kso_direct
                            response.close(); break # Found with this fallback base_url
                        response.close()
                    except requests.exceptions.RequestException: pass

    if not raw_m3u8_url:
        # This print will help confirm if we are reaching this point for all IDs
        # print(f"  [DEBUG _discover_single_id_location] ID {dlhd_id} - No raw_m3u8_url found. Attempted: {attempted_urls_for_id_precaching}")
        return (dlhd_id, None) # Ensure a tuple is always returned
    return (dlhd_id, raw_m3u8_url) # Return the found URL with the ID

def populate_stream_location_cache(ids_to_probe_with_names):
    """
    Populates STREAM_LOCATION_CACHE by discovering stream URLs for given IDs in parallel.
    ids_to_probe_with_names: list of (dlhd_id, channel_name_for_tennis_logic) tuples
    """
    if not ids_to_probe_with_names:
        print("No unique IDs provided for cache population.")
        return

    print(f"Pre-populating stream location cache for {len(ids_to_probe_with_names)} unique IDs...")

    MAX_CACHE_WORKERS = 20 # Adjust as needed, can be higher for I/O bound tasks
    processed_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CACHE_WORKERS) as executor:
        future_to_id_info = {executor.submit(_discover_single_id_location, id_info): id_info for id_info in ids_to_probe_with_names}
        for future in concurrent.futures.as_completed(future_to_id_info):
            id_info_tuple = future_to_id_info[future]
            original_dlhd_id = id_info_tuple[0]
            processed_count += 1
            try:
                discovered_id, found_url = future.result()
                if found_url:
                    STREAM_LOCATION_CACHE[discovered_id] = found_url
                    # Optional: print(f"  [CACHE POPULATED] ID {discovered_id} found at {found_url} ({processed_count}/{len(ids_to_probe_with_names)})")
                # else:
                    # Optional: print(f"  [CACHE POPULATE FAILED] ID {discovered_id} not found by pre-fetcher. ({processed_count}/{len(ids_to_probe_with_names)})")
            except Exception as exc:
                print(f"  [CACHE POPULATE ERROR] ID {original_dlhd_id} generated an exception: {exc} ({processed_count}/{len(ids_to_probe_with_names)})")
    print(f"Stream location cache populated. Found locations for {len(STREAM_LOCATION_CACHE)} out of {len(ids_to_probe_with_names)} unique IDs.")

def clean_group_title(sport_key):
    """Clean the sport key to create a proper group-title"""
    # More robust HTML tag removal
    import re
    clean_key = re.sub(r'<[^>]+>', '', sport_key).strip()

    # If empty after cleaning, return original key
    if not clean_key:
        clean_key = sport_key.strip()

    # Convert to title case to standardize
    return clean_key.title()

def should_include_channel(channel_name, event_name, sport_key):
    """Check if channel should be included based on keywords"""
    combined_text = (channel_name + " " + event_name + " " + sport_key).lower()

    # Check if any exclusion keyword is present in the combined text
    for keyword in EXCLUDE_KEYWORDS_FROM_CHANNEL_INFO:
        if keyword.lower() in combined_text:
            return False  # Exclude the channel if keyword is found

    return True  # Include the channel if no exclusion keywords are found

def fetch_stream_details_worker(task_args):
    """
    Worker function to fetch stream link and prepare M3U8 line data.
    task_args is a tuple containing:
    (channelID, event_details, channel_name_str_for_get_link,
     tvg_id_val, tvg_name, event_logo, italian_sport_key, channel_name_str_for_extinf)
    """
    channelID, event_details, channel_name_str_for_get_link, \
    tvg_id_val, tvg_name, event_logo, italian_sport_key, \
    channel_name_str_for_extinf = task_args

    stream_url_dynamic_with_headers = get_stream_link(channelID, event_details, channel_name_str_for_get_link)

    if stream_url_dynamic_with_headers:
        # Estrai l'URL grezzo prima degli header aggiunti (se presenti)
        raw_stream_url_part = stream_url_dynamic_with_headers.split("&h_user-agent=")[0]
        return (channelID, raw_stream_url_part, tvg_id_val, tvg_name, event_logo,
                italian_sport_key, channel_name_str_for_extinf)
    return None

def fetch_and_parse_247_channels():
    """Fetches and parses 24/7 channel data from daddylive.dad."""
    all_247_channels_info = []
    try:
        print(f'Downloading {DADDY_LIVE_CHANNELS_URL} for 24/7 channels...')
        response = requests.get(DADDY_LIVE_CHANNELS_URL, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)

        for link in links:
            if "italy" in link.text.lower():  # Filter for "Italy"
                href = link['href']
                match_re = re.search(r'stream-(\d+)\.php', href)
                if match_re:
                    stream_number = match_re.group(1)
                    original_stream_name = link.text.strip()

                    # Clean channel name as in 247m3u.py
                    stream_name_cleaned = original_stream_name
                    replacements = {
                        "Italy": "", "ITALY": "",
                        "(251)": "", "(252)": "", "(253)": "", "(254)": "",
                        "(255)": "", "(256)": "", "(257)": "",
                        "HD+": "", "8": "" # "8" was in 247m3u.py, check if still needed
                    }
                    for old, new in replacements.items():
                        stream_name_cleaned = stream_name_cleaned.replace(old, new)
                    stream_name_cleaned = stream_name_cleaned.strip()

                    # If cleaning results in an empty name, use a placeholder or original
                    if not stream_name_cleaned:
                        stream_name_cleaned = f"Channel {stream_number}"

                    all_247_channels_info.append({
                        'id': stream_number,
                        'name': stream_name_cleaned,
                        'original_name': original_stream_name
                    })
        print(f"Found {len(all_247_channels_info)} potential 24/7 'Italy' channels from HTML page.")
    except requests.exceptions.RequestException as e_req:
        print(f'Error downloading or parsing 24/7 channels page: {e_req}')
    except Exception as e_gen:
        print(f'Generic error in fetch_and_parse_247_channels: {e_gen}')
    return all_247_channels_info

def prepare_247_channel_tasks(parsed_247_channels_list):
    """Prepares task arguments for 24/7 channels for the ThreadPoolExecutor."""
    tasks = []
    processed_247_ids_in_this_batch = set() # To avoid duplicates from the 24/7 list itself
    # Add DAZN 1 manually first
    dazn1_id = "877"
    dazn1_name = "DAZN 1"
    dazn1_original_name = "DAZN 1" # For get_stream_link context
    dazn1_logo = STATIC_LOGOS_247.get(dazn1_name.lower(), DEFAULT_247_LOGO)
    dazn1_tvg_id = STATIC_TVG_IDS_247.get(dazn1_name.lower(), dazn1_name)
    dazn1_group = STATIC_CATEGORIES_247.get(dazn1_name.lower(), DEFAULT_247_GROUP)
    tasks.append((
        dazn1_id, dazn1_name, dazn1_original_name, dazn1_tvg_id, dazn1_name,
        dazn1_logo, dazn1_group, f"{dazn1_name} (D)" # Changed suffix here
    ))
    processed_247_ids_in_this_batch.add(dazn1_id)
    for ch_info in parsed_247_channels_list:
        channel_id = ch_info['id']
        if channel_id not in processed_247_ids_in_this_batch: # Removed check against event_channel_ids_to_skip
            channel_name = ch_info['name']
            original_channel_name = ch_info['original_name']
            tvg_logo = STATIC_LOGOS_247.get(channel_name.lower().strip(), DEFAULT_247_LOGO)
            tvg_id = STATIC_TVG_IDS_247.get(channel_name.lower().strip(), channel_name)
            group_title = STATIC_CATEGORIES_247.get(channel_name.lower().strip(), DEFAULT_247_GROUP)
            tasks.append((
                channel_id, channel_name, original_channel_name, tvg_id, channel_name,
                tvg_logo, group_title, f"{channel_name} (D)" # Changed suffix here
            ))
            processed_247_ids_in_this_batch.add(channel_id)
        elif channel_id in processed_247_ids_in_this_batch:
            print(f"Skipping 24/7 channel {ch_info['name']} (ID: {channel_id}) as it was already added in this 24/7 batch.")
    return tasks

def generate_m3u_playlist():
    dadjson = loadJSON(DADDY_JSON_FILE)
    for day, day_data in dadjson.items():
        try:
            for sport_key, sport_events in day_data.items():
                for game in sport_events:
                    for channel_data_item in game.get("channels", []):
                        channel_id_str = None
                        channel_name_for_tennis = ""

                        if isinstance(channel_data_item, dict):
                            if "channel_id" in channel_data_item:
                                channel_id_str = str(channel_data_item['channel_id'])
                            if "channel_name" in channel_data_item:
                                channel_name_for_tennis = channel_data_item["channel_name"]
                            # else, channel_name_for_tennis remains empty
                        elif isinstance(channel_data_item, str) and channel_data_item.isdigit():
                            channel_id_str = channel_data_item
                            # channel_name_for_tennis remains empty

                        if channel_id_str:
                            if channel_id_str not in unique_ids_for_precaching:
                                unique_ids_for_precaching[channel_id_str] = channel_name_for_tennis
                            # If already present, we could update channel_name_for_tennis if the new one is more specific,
                            # but for simplicity, first encountered name is fine for the heuristic.
        except (KeyError, TypeError) as e:
            print(f"KeyError/TypeError during ID collection for pre-caching: {e}")
            pass

    # Fetch and parse index pages ONCE before starting any stream discovery
    fetch_all_index_pages()

    unique_ids_for_precaching = {} # Using dict to store ID -> channel_name (first one encountered for tennis logic)
    print("Collecting unique channel IDs for pre-caching (Events)...")
    print("Collecting unique channel IDs for pre-caching (24/7 Channels)...")
    parsed_247_channels_data = fetch_and_parse_247_channels()
    for ch_info in parsed_247_channels_data:
        ch_id = ch_info['id']
        ch_name_for_tennis_logic = ch_info.get('original_name', ch_info['name']) # Use original name for more context
        if ch_id not in unique_ids_for_precaching:
            unique_ids_for_precaching[ch_id] = ch_name_for_tennis_logic

    # Add DAZN1 ID for pre-caching
    dazn1_id_static = "877"
    dazn1_name_static = "DAZN 1"
    if dazn1_id_static not in unique_ids_for_precaching:
        unique_ids_for_precaching[dazn1_id_static] = dazn1_name_static

    ids_to_probe_tuples = [(id_val, name_val) for id_val, name_val in unique_ids_for_precaching.items()]
    populate_stream_location_cache(ids_to_probe_tuples)

    # Remove existing M3U8 file if it exists and write header
    if os.path.exists(M3U8_OUTPUT_FILE):
        os.remove(M3U8_OUTPUT_FILE)
    with open(M3U8_OUTPUT_FILE, 'w', encoding='utf-8') as file:
        file.write('#EXTM3U\n')

    # Counters
    total_events_in_json = 0
    skipped_events_by_category_filter = 0

    # First pass to gather category statistics
    category_stats = {} # For events
    for day, day_data in dadjson.items():
        try:
            for sport_key, sport_events in day_data.items():
                clean_sport_key = sport_key.replace("</span>", "").replace("<span>", "").strip()
                if clean_sport_key not in category_stats:
                    category_stats[clean_sport_key] = 0
                category_stats[clean_sport_key] += len(sport_events)
        except (KeyError, TypeError):
            pass

    # Print category statistics
    print("\n=== Available Categories ===")
    for category, count in sorted(category_stats.items()):
        excluded = "EXCLUDED" if category in excluded_categories else ""
        print(f"{category}: {count} events {excluded}")
    print("===========================\n")

    # Generate unique IDs for channels (if needed for fallback, though IDs from JSON/HTML are primary)
    # unique_ids_fallback = generate_unique_ids(NUM_CHANNELS) # This seems unused if IDs are from source

    # 4. Process and write 24/7 channels FIRST
    print("\nProcessing 24/7 Channels...")
    tasks_247 = prepare_247_channel_tasks(parsed_247_channels_data) # No longer pass event_channel_ids_processed_and_written

    processed_247_channels_count = 0
    MAX_WORKERS = 10 # Define MAX_WORKERS here or make it a global constant
    if tasks_247:
        print(f"Fetching stream details for {len(tasks_247)} 24/7 channels...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results_247 = list(executor.map(fetch_stream_details_worker, tasks_247))

        with open(M3U8_OUTPUT_FILE, 'a', encoding='utf-8') as file: # Append 24/7 channels
            for result_item in results_247:
                if result_item:
                    # Il secondo elemento è l'URL grezzo dello stream
                    _, raw_stream_url_247, tvg_id_val, tvg_name, event_logo, \
                    group_title_val, channel_name_str_for_extinf_247 = result_item

                    # URL-encode il raw_stream_url_247 per l'uso sicuro in un parametro query
                    safe_raw_url_247 = urllib.parse.quote_plus(raw_stream_url_247)
                    new_final_url_247 = f"{PZPROXY}/porxy?url={safe_raw_url_247}"

                    file.write(f'#EXTINF:-1 tvg-id="{tvg_id_val}" tvg-name="{tvg_name}" tvg-logo="{event_logo}" group-title="{group_title_val}",{channel_name_str_for_extinf_247}\n')
                    file.write(f"{new_final_url_247}\n\n")
                    processed_247_channels_count += 1
        print(f"Finished processing 24/7 channels. Added {processed_247_channels_count} 24/7 channels.")
    else:
        print("No 24/7 channel tasks to process.")

    # Define categories to exclude for events
    excluded_categories = [
        "TV Shows", "Cricket", "Aussie rules", "Snooker", "Baseball",
        "Biathlon", "Cross Country", "Horse Racing", "Ice Hockey",
        "Waterpolo", "Golf", "Darts", "Badminton", "Handball"
    ]
    print("\nProcessing Event Channels from JSON...")
    for day, day_data in dadjson.items():
        try:
            # day_data is a dict where keys are sport_keys and values are lists of events
            for sport_key, sport_events in day_data.items():
                clean_sport_key = sport_key.replace("</span>", "").replace("<span>", "").strip()
                total_events_in_json += len(sport_events)

                # Skip only exact category matches
                if clean_sport_key in excluded_categories:
                    skipped_events_by_category_filter += len(sport_events)
                    continue

                for game in sport_events:
                    for channel in game.get("channels", []):
                        try:
                            # Clean and format day
                            clean_day = day.replace(" - Schedule Time UK GMT", "")
                            # Rimuovi completamente i suffissi ordinali (st, nd, rd, th)
                            clean_day = clean_day.replace("st ", " ").replace("nd ", " ").replace("rd ", " ").replace("th ", " ")
                            # Rimuovi anche i suffissi attaccati al numero (1st, 2nd, 3rd, etc.)
                            import re
                            clean_day = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', clean_day)

                            print(f"Original day: '{day}'")
                            print(f"Clean day after processing: '{clean_day}'")

                            day_parts = clean_day.split()
                            print(f"Day parts: {day_parts}")  # Debug per vedere i componenti della data

                            # Handle various date formats with better validation
                            day_num = None
                            month_name = None
                            year = None

                            if len(day_parts) >= 4:  # Standard format: Weekday Month Day Year
                                weekday = day_parts[0]
                                # Verifica se il secondo elemento contiene lettere (è il mese) o numeri (è il giorno)
                                if any(c.isalpha() for c in day_parts[1]):
                                    # Formato: Weekday Month Day Year
                                    month_name = day_parts[1]
                                    day_num = day_parts[2]
                                elif any(c.isalpha() for c in day_parts[2]):
                                    # Formato: Weekday Day Month Year
                                    day_num = day_parts[1]
                                    month_name = day_parts[2]
                                else:
                                    # Se non riusciamo a determinare, assumiamo il formato più comune
                                    day_num = day_parts[1]
                                    month_name = day_parts[2]
                                year = day_parts[3]
                                print(f"Parsed date components: weekday={weekday}, day={day_num}, month={month_name}, year={year}")
                            elif len(day_parts) == 3:
                                # Format could be: "Weekday Day Year" (missing month) or "Day Month Year"
                                if day_parts[0].lower() in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
                                    # It's "Weekday Day Year" format (missing month)
                                    day_num = day_parts[1]
                                    # Get current month for Rome timezone
                                    rome_tz = pytz.timezone('Europe/Rome')
                                    current_month = datetime.datetime.now(rome_tz).strftime('%B')
                                    month_name = current_month
                                    year = day_parts[2]
                                else:
                                    # Assume Day Month Year
                                    day_num = day_parts[0]
                                    month_name = day_parts[1]
                                    year = day_parts[2]
                            else:
                                # Use current date from Rome timezone
                                rome_tz = pytz.timezone('Europe/Rome')
                                now = datetime.datetime.now(rome_tz)
                                day_num = now.strftime('%d')
                                month_name = now.strftime('%B')
                                year = now.strftime('%Y')
                                print(f"Using current Rome date for: {clean_day}")

                            # Validate day_num - ensure it's a number and extract only digits
                            if day_num:
                                # Extract only digits from day_num
                                day_num_digits = re.sub(r'[^0-9]', '', str(day_num))
                                if day_num_digits:
                                    day_num = day_num_digits
                                else:
                                    # If no digits found, use current day
                                    rome_tz = pytz.timezone('Europe/Rome')
                                    day_num = datetime.datetime.now(rome_tz).strftime('%d')
                                    print(f"Warning: Invalid day number '{day_num}', using current day: {day_num}")
                            else:
                                # If day_num is None, use current day
                                rome_tz = pytz.timezone('Europe/Rome')
                                day_num = datetime.datetime.now(rome_tz).strftime('%d')
                                print(f"Warning: Missing day number, using current day: {day_num}")

                            # Get time from game data
                            time_str = game.get("time", "00:00")

                            # Converti l'orario da UK a CET (aggiungi 2 ore invece di 1)
                            time_parts = time_str.split(":")
                            if len(time_parts) == 2:
                                hour = int(time_parts[0])
                                minute = time_parts[1]
                                # Aggiungi due ore all'orario UK
                                hour_cet = (hour + 2) % 24
                                # Assicura che l'ora abbia due cifre
                                hour_cet_str = f"{hour_cet:02d}"
                                # Nuovo time_str con orario CET
                                time_str_cet = f"{hour_cet_str}:{minute}"
                            else:
                                # Se il formato dell'orario non è corretto, mantieni l'originale
                                time_str_cet = time_str

                            # Convert month name to number
                            month_map = {
                                "January": "01", "February": "02", "March": "03", "April": "04",
                                "May": "05", "June": "06", "July": "07", "August": "08",
                                "September": "09", "October": "10", "November": "11", "December": "12"
                            }

                            # Aggiungi controllo per il mese
                            if not month_name or month_name not in month_map:
                                print(f"Warning: Invalid month name '{month_name}', using current month")
                                rome_tz = pytz.timezone('Europe/Rome')
                                current_month = datetime.datetime.now(rome_tz).strftime('%B')
                                month_name = current_month

                            month_num = month_map.get(month_name, "01")  # Default to January if not found

                            # Ensure day has leading zero if needed
                            if len(str(day_num)) == 1:
                                day_num = f"0{day_num}"

                            # Create formatted date time
                            year_short = str(year)[-2:]  # Extract last two digits of year
                            formatted_date_time = f"{day_num}/{month_num}/{year_short} - {time_str_cet}"

                            # Also create proper datetime objects for EPG
                            # Make sure we're using clean numbers for the date components
                            try:
                                # Ensure all date components are valid
                                if not day_num or day_num == "":
                                    rome_tz = pytz.timezone('Europe/Rome')
                                    day_num = datetime.datetime.now(rome_tz).strftime('%d')
                                    print(f"Using current day as fallback: {day_num}")

                                if not month_num or month_num == "":
                                    month_num = "01"  # Default to January
                                    print(f"Using January as fallback month")

                                if not year or year == "":
                                    rome_tz = pytz.timezone('Europe/Rome')
                                    year = datetime.datetime.now(rome_tz).strftime('%Y')
                                    print(f"Using current year as fallback: {year}")

                                if not time_str or time_str == "":
                                    time_str = "00:00"
                                    print(f"Using 00:00 as fallback time")

                                # Ensure day_num has proper format (1-31)
                                try:
                                    day_int = int(day_num)
                                    if day_int < 1 or day_int > 31:
                                        day_num = "01"  # Default to first day of month
                                        print(f"Day number out of range, using 01 as fallback")
                                except ValueError:
                                    day_num = "01"  # Default to first day of month
                                    print(f"Invalid day number format, using 01 as fallback")

                                # Ensure day has leading zero if needed
                                if len(str(day_num)) == 1:
                                    day_num = f"0{day_num}"

                                date_str = f"{year}-{month_num}-{day_num} {time_str}:00"
                                print(f"Attempting to parse date: '{date_str}'")
                                start_date_utc = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

                                # Convert to Amsterdam timezone
                                amsterdam_timezone = pytz.timezone("Europe/Amsterdam")
                                start_date_amsterdam = start_date_utc.replace(tzinfo=pytz.UTC).astimezone(amsterdam_timezone)

                                # Format for EPG
                                mStartTime = start_date_amsterdam.strftime("%Y%m%d%H%M%S")
                                mStopTime = (start_date_amsterdam + datetime.timedelta(days=2)).strftime("%Y%m%d%H%M%S")
                            except ValueError as e:
                                # Definisci date_str qui se non è già definita
                                error_msg = str(e)
                                if 'date_str' not in locals():
                                    date_str = f"Error with: {year}-{month_num}-{day_num} {time_str}:00"

                                print(f"Date parsing error: {error_msg} for date string '{date_str}'")
                                # Use current time as fallback
                                amsterdam_timezone = pytz.timezone("Europe/Amsterdam")
                                now = datetime.datetime.now(amsterdam_timezone)
                                mStartTime = now.strftime("%Y%m%d%H%M%S")
                                mStopTime = (now + datetime.timedelta(days=2)).strftime("%Y%m%d%H%M%S")

                            # Build channel name with new date format
                            if isinstance(channel, dict) and "channel_name" in channel:
                                channelName = formatted_date_time + "  " + channel["channel_name"]
                            else:
                                channelName = formatted_date_time + "  " + str(channel)

                            # Extract event name for the tvg-id
                            event_name_short = game["event"].split(":")[0].strip() if ":" in game["event"] else game["event"].strip()
                            event_details = game["event"]  # Keep the full event details for tvg-name

                        except Exception as e:
                            print(f"Error processing date '{day}': {e}")
                            print(f"Game time: {game.get('time', 'No time found')}")
                            continue

                        # Derive channelID and channel_name for get_stream_link arguments
                        channelID_for_task = None
                        channel_name_for_get_link_arg = "" # For tennis heuristic in get_stream_link

                        if isinstance(channel, dict):
                            if "channel_id" in channel:
                                channelID_for_task = str(channel['channel_id'])
                            if "channel_name" in channel:
                                channel_name_for_get_link_arg = channel["channel_name"]
                            else: # dict but no channel_name
                                channel_name_for_get_link_arg = str(channel) # Fallback
                        elif isinstance(channel, str): # channel is a string
                            if channel.isdigit(): # Assume it's an ID
                                channelID_for_task = channel
                                channel_name_for_get_link_arg = f"Channel {channel}" # Placeholder name
                            else: # Assume it's a name, no ID from this structure
                                channel_name_for_get_link_arg = channel
                                # channelID_for_task remains None

                        if not channelID_for_task: # If no usable ID, generate UUID as per original logic
                            channelID_for_task = str(uuid.uuid4())
                            print(f"  Generated UUID {channelID_for_task} as fallback ID for channel: {channel}")

                        # Check if channel should be included based on keywords
                        if should_include_channel(channelName, event_details, clean_sport_key):
                            # channel_name_str_for_extinf is what appears after the comma in #EXTINF
                            # channel_name_str_for_extinf è quello che appare dopo la virgola in #EXTINF
                            if isinstance(channel, dict) and "channel_name" in channel:
                                channel_name_str_for_extinf = channel["channel_name"]
                            else:
                                channel_name_str_for_get_link = str(channel)
                                channel_name_str_for_extinf = str(channel)

                            time_only = time_str_cet if time_str_cet else "00:00"
                            tvg_name = f"{time_only} {event_details} - {day_num}/{month_num}/{year_short}"
                            event_logo = get_dynamic_logo(game["event"], clean_sport_key) # Chiamata sequenziale, ok
                            italian_sport_key = translate_sport_to_italian(clean_sport_key)
                            tvg_id_val = f"{event_name_short} - {event_details.split(':', 1)[1].strip() if ':' in event_details else event_details}"

                            tasks_for_workers.append((
                                channelID_for_task, event_details, channel_name_for_get_link_arg, # Use derived ID and name for get_stream_link
                                tvg_id_val, tvg_name, event_logo, italian_sport_key,
                                channel_name_str_for_extinf
                            ))
                        else:
                            excluded_event_channels_by_keyword +=1

        except KeyError as e:
            print(f"KeyError: {e} - Key may not exist in JSON structure")

    processed_event_channels_count = 0
    excluded_event_channels_by_keyword = 0
    tasks_for_workers = [] # Initialize tasks_for_workers for events
    # Process tasks in parallel and write to M3U8 file
    # Determina un numero di worker appropriato, es. 10 o 20 per I/O bound tasks
    # Puoi sperimentare con questo valore.
    MAX_WORKERS = 10
    if tasks_for_workers:
        event_channel_ids_processed_and_written = set() # Store IDs of events written to M3U
        print(f"Fetching stream details for {len(tasks_for_workers)} event channels...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            event_results = list(executor.map(fetch_stream_details_worker, tasks_for_workers))

        with open(M3U8_OUTPUT_FILE, 'a', encoding='utf-8') as file: # Append event channels
            for result_item in event_results:
                if result_item:
                    original_channel_id, raw_stream_url, tvg_id_val, tvg_name, event_logo, \
                    italian_sport_key, channel_name_str_for_extinf = result_item

                    event_channel_ids_processed_and_written.add(original_channel_id) # Populate the set
                    
                    # URL-encode il raw_stream_url per l'uso sicuro in un parametro query
                    safe_raw_url = urllib.parse.quote_plus(raw_stream_url)
                    new_final_url = f"{PZPROXY}/porxy?url={safe_raw_url}"
                                        
                    file.write(f'#EXTINF:-1 tvg-id="{tvg_id_val}" tvg-name="{tvg_name}" tvg-logo="{event_logo}" group-title="{italian_sport_key}",{channel_name_str_for_extinf}\n')
                    file.write(f"{new_final_url}\n\n")
                    processed_event_channels_count += 1
        print(f"Finished processing event channels. Added {processed_event_channels_count} event channels.")
    else:
        event_channel_ids_processed_and_written = set() # Initialize empty if no event tasks
        print("No event channel tasks to process.")


    print(f"\n=== Processing Summary ===")
    print(f"Total events found in JSON: {total_events_in_json}")
    print(f"Events skipped due to category filters: {skipped_events_by_category_filter}")
    print(f"Event channels included in M3U8: {processed_event_channels_count}")
    print(f"Event channels excluded by keyword filter: {excluded_event_channels_by_keyword}")
    print(f"Keywords used for event channel exclusion: {EXCLUDE_KEYWORDS_FROM_CHANNEL_INFO}")
    print(f"---")
    print(f"24/7 channels (incl. DAZN1) processed for M3U8: {processed_247_channels_count}")
    total_channels_in_m3u8 = processed_event_channels_count + processed_247_channels_count
    print(f"---")
    print(f"Total channels in M3U8 ({M3U8_OUTPUT_FILE}): {total_channels_in_m3u8}")
    print(f"===========================\n")
    return total_channels_in_m3u8

def main():
    # Process events and generate M3U8
    total_processed_channels = generate_m3u_playlist()

    # Verify if any valid channels were created
    if total_processed_channels == 0:
        print(f"No valid channels found from events or 24/7 sources for {M3U8_OUTPUT_FILE}.")
    else:
        print(f"{M3U8_OUTPUT_FILE} generated with a total of {total_processed_channels} channels.")

if __name__ == "__main__":
    main()
