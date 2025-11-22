import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from matplotlib import pyplot as plt

def scrape_flowrate():
    return 0

def scrape_waterlevel():
    url = "https://www.hnd.bayern.de/pegel/isar/muenchen-himmelreichbruecke-16515005/tabelle"

    # Header setzen, um nicht geblockt zu werden
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # Wir suchen die Tabelle mit der Klasse "tblsort", wie in deinem HTML gesehen
        table = soup.find("table", class_="tblsort")

        data = []

        if table:
            # Wir suchen direkt im tbody nach Zeilen
            tbody = table.find("tbody")
            rows = tbody.find_all("tr")

            for row in rows:
                cols = row.find_all("td")

                # Dein HTML zeigt genau 2 Spalten pro Zeile
                if len(cols) >= 2:
                    try:
                        # Spalte 1: Datum und Zeit kombiniert
                        raw_timestamp = cols[0].text.strip()

                        # Spalte 2: Wert
                        raw_value = cols[1].text.strip()

                        # Umwandlung in Python-Objekte
                        dt_obj = datetime.strptime(raw_timestamp, "%d.%m.%Y %H:%M")
                        val_int = int(raw_value)

                        data.append({"timestamp": dt_obj, "wasserstand": val_int})
                    except ValueError:
                        continue

        # DataFrame erstellen
        df = pd.DataFrame(data)

        if not df.empty:
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)  # Sicherstellen, dass die Zeitreihe chronologisch ist

            print("Daten erfolgreich geladen:")
            print(df.head())
            print(df.info())

            # df.to_csv("pegel_muenchen.csv")
            plt.plot(df)
        else:
            print("Keine Daten gefunden.")

    else:
        print("Seite konnte nicht abgerufen werden.")

    return 0

def scrape_temperature():
    return 0

def scrape_humidity():
    return 0

def scrape_arrivals():
    return 0

def scrape_departures():
    return 0




