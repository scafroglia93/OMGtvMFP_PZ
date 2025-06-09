import os
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import urlparse # Importato ma non più usato, può essere rimosso se non serve altrove

def update_proxy_links(input_m3u8_filepath, output_m3u8_filepath, env_filepath):
    """
    Legge un file M3U8 di input, sostituisce un placeholder con un URL base dal file .env
    e scrive il risultato in un file M3U8 di output.
    """
    # Carica le variabili dal file .env specificato
    load_dotenv(dotenv_path=env_filepath)

    # Ottieni le configurazioni dal file .env
    proxy_base_url = os.getenv("MPDPROXYMFP") # Modificato per coerenza con il placeholder

    # Validazione delle variabili d'ambiente necessarie
    if not proxy_base_url:
        print(f"Errore: La variabile MPDPROXYMFP non è stata trovata nel file {env_filepath}")
        return

    print(f"Utilizzo del proxy base URL: {proxy_base_url}")

    # Configurazione per le sostituzioni
    placeholder = "{MPDPROXYMFP}" # Placeholder da cercare nel file di input
    
    lines_to_write = []
    updated_count = 0
    m3u8_path = Path(input_m3u8_filepath)

    try:
        with open(m3u8_path, 'r', encoding='utf-8') as f:
            lines = f.readlines() # Legge dal file di input

        for line_number, original_line in enumerate(lines, 1):
            stripped_line = original_line.strip()
            processed_line = original_line

            if not stripped_line or stripped_line.startswith("#"):
                lines_to_write.append(original_line)
                continue
            
            modified_content = stripped_line

            # Sostituisci il placeholder se presente
            if placeholder in stripped_line:
                modified_content = stripped_line.replace(placeholder, proxy_base_url.rstrip('/'))
                if modified_content != stripped_line:
                    processed_line = modified_content + '\n'
                    updated_count += 1

            lines_to_write.append(processed_line)

        # Scrivi le modifiche nel file di output, sovrascrivendolo
        with open(output_m3u8_filepath, 'w', encoding='utf-8') as f:
            f.writelines(lines_to_write)

        if updated_count > 0:
            print(f"File {Path(output_m3u8_filepath).name} creato/aggiornato con successo. {updated_count} placeholder sostituiti.")
        else:
            print(f"Nessun link da aggiornare trovato in {m3u8_path.name} con i criteri specificati.")

    except FileNotFoundError:
        print(f"Errore: Il file {m3u8_path} non è stato trovato.")
    except Exception as e:
        print(f"Si è verificato un errore: {e}")

if __name__ == "__main__":
    import sys
    
    # Definisci i percorsi relativi allo script
    script_dir = Path(__file__).resolve().parent
    
    # File di input fisso
    input_m3u8_file = script_dir / "FILEmpd.m3u8" # Nome del file di input come specificato
    
    # File di output (può essere specificato come argomento o default a "mpd.m3u8")
    output_m3u8_filename = sys.argv[1] if len(sys.argv) > 1 else "mpd.m3u8"
    output_m3u8_file = script_dir / output_m3u8_filename
    env_file = script_dir / ".env"      # Assumendo che .env sia nella stessa cartella

    update_proxy_links(input_m3u8_file, output_m3u8_file, env_file)
