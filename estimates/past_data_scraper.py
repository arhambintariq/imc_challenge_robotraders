import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from matplotlib import pyplot as plt

from constants import *


def get_waterflow():
    url = "https://www.hnd.bayern.de/pegel/isar/muenchen-himmelreichbruecke-16515005/tabelle?methode=abfluss&"
    df = scrape_waterflow(url)
    return df["data"]

def get_waterlevel():
    url = "https://www.hnd.bayern.de/pegel/isar/muenchen-himmelreichbruecke-16515005/tabelle"
    df = scrape_waterlevel(url)
    return df["data"]

def get_temperature():
    url = "https://www.timeanddate.com/weather/germany/munich/historic"
    df = scrape_weather_df(url)
    return df["temp"]

def get_humidity():
    url = "https://www.timeanddate.com/weather/germany/munich/historic"
    df = scrape_weather_df(url)
    return df["humidity"]
def get_arrivals():
    return 0

def get_departures():
    return 0

def scrape_weather_df(url: str) -> pd.DataFrame:
    """
    Scrapes a timeanddate.com weather table and returns a pandas DataFrame
    with columns: ["time", "temp", "humidity"].
    """

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    tables = soup.find_all("table")

    records = []

    for table in tables:
        rows = table.find_all("tr")

        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]

            if len(cols) < 4:
                continue

            # --- extract time ---
            time = None
            for c in cols:
                # detect hh:mm pattern
                if ":" in c and len(c.split(":")) == 2:
                    time = c
                    break

            if not time:
                continue

            # --- extract temperature ---
            temp = None
            for c in cols:
                if "°" in c:
                    num = c.split("°")[0].strip()
                    try:
                        temp = float(num)
                        break
                    except:
                        pass

            # --- extract humidity ---
            humidity = None
            for c in cols:
                if c.endswith("%"):
                    try:
                        humidity = float(c.replace("%", ""))
                        break
                    except:
                        pass

            if temp is not None and humidity is not None:
                records.append((time, temp, humidity))

    # convert to DataFrame
    df = pd.DataFrame(records, columns=["time", "temp", "humidity"])
    # celsius to fahrenheit
    df["temp"] = df["temp"] * 1.8 + 32
    # save it for later
    df.to_csv(WEATHER_DATA_NAME)
    if not df.empty:
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)  # Chronologisch sortieren

        # print("Daten erfolgreich geladen:")
        # print(df.head())
        # print("\nInfo:")
        # print(df.info())
        df.to_csv(WATERFLOW_DATA_NAME)

        return df
    else:
        print("Keine Daten gefunden oder Tabelle leer.")
        return None

def scrape_waterflow(url):
    # Header setzen, um wie ein Browser zu wirken
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Wirft Fehler bei 404 oder 500 Codes
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen der URL: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')

    # Wir suchen nach der Tabelle, die 'tblsort' in den Klassen hat
    # BeautifulSoup findet dies auch, wenn 'tablesorter' etc. dabei steht.
    table = soup.find("table", class_="tblsort")

    data = []

    if table:
        # Suche im Body der Tabelle, um Header zu überspringen
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")

            for row in rows:
                cols = row.find_all("td")

                # Prüfen ob Zeile Daten enthält (mind. 2 Spalten)
                if len(cols) >= 2:
                    try:
                        # Spalte 1: Datum (z.B. "22.11.2025 03:00")
                        raw_timestamp = cols[0].text.strip()

                        # Spalte 2: Wert (z.B. "25,8")
                        raw_value = cols[1].text.strip()

                        # WICHTIG: Deutsches Komma durch Punkt ersetzen für Python float
                        clean_value = raw_value.replace(',', '.')

                        # Umwandlung
                        dt_obj = datetime.strptime(raw_timestamp, "%d.%m.%Y %H:%M")

                        # Float statt Int, da Dezimalstellen vorhanden sind
                        val_float = float(clean_value)

                        data.append({"timestamp": dt_obj, "data": val_float})
                    except ValueError as e:
                        # Falls Parsing fehlschlägt (z.B. leerer String), überspringen wir die Zeile
                        # print(f"Parsing Fehler in Zeile: {e}")
                        continue

    # DataFrame erstellen
    df = pd.DataFrame(data)

    if not df.empty:
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)  # Chronologisch sortieren

        # print("Daten erfolgreich geladen:")
        # print(df.head())
        # print("\nInfo:")
        # print(df.info())
        df.to_csv(WATERFLOW_DATA_NAME)

        return df
    else:
        print("Keine Daten gefunden oder Tabelle leer.")
        return None

def scrape_waterlevel(url):
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

                        data.append({"timestamp": dt_obj, "data": val_int})
                    except ValueError:
                        continue

        # DataFrame erstellen
        df = pd.DataFrame(data)

        if not df.empty:
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)  # Sicherstellen, dass die Zeitreihe chronologisch ist

            # print("Daten erfolgreich geladen:")
            # print(df.head())
            # print(df.info())
            # plt.plot(df)
            df.to_csv(WATERLEVEL_DATA_NAME)

        else:
            print("Keine Daten gefunden.")
            return None

    else:
        print("Seite konnte nicht abgerufen werden.")
        return None

    # naive prediction
    return df




