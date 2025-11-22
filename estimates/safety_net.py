import pandas as pd
from typing import List
import numpy as np
from datetime import datetime, time

from estimates.markets import *
from estimates.predictions import *
from estimates.weather_forecast import get_raw_data

PRIOR_FLOW = 23
PRIOR_LEVEL = 138

# --------------------------------------------------------------------
# Helper: load CSV and get full column (used for markets needing lists)
# --------------------------------------------------------------------
def load_series(csv_path: str) -> List[float]:
    """
    Loads a CSV timestamp | value and returns the list of values.
    """
    df = pd.read_csv(csv_path)
    return df.iloc[:, 1].astype(float).tolist()


# --------------------------------------------------------------------
# Market 1 – Eisbach flow * water level
# --------------------------------------------------------------------
def predict_market_1() -> int:
    # Zeitpunkt heute um 10:00 Uhr
    heute_zehn = datetime.combine(datetime.today(), time(10, 0))

    # aktueller Zeitpunkt
    jetzt = datetime.now()

    # vergangene Stunden (abgerundet)
    stunden = int((jetzt - heute_zehn).total_seconds() // 3600)

    weighted_flow = (1 - (stunden/24)) * PRIOR_FLOW + (stunden/24)*get_waterflow().iloc[-1]

    weighted_level = (1 - (stunden / 24)) * PRIOR_LEVEL + (stunden / 24) * get_waterlevel().iloc[-1]

    return market_1_settlement(
        flow_rate=weighted_flow,
        water_level=weighted_level
    )


# --------------------------------------------------------------------
# Market 2 – Eisbach extrema option
# --------------------------------------------------------------------
def predict_market_2() -> float:

    # Zeitpunkt heute um 10:00 Uhr
    heute_zehn = datetime.combine(datetime.today(), time(10, 0))

    # aktueller Zeitpunkt
    jetzt = datetime.now()

    # vergangene Stunden (abgerundet)
    stunden = int((jetzt - heute_zehn).total_seconds() // 3600)

    # --- Daten laden ---
    wl_actual = get_waterlevel().tail(24-stunden)
    wf_actual = get_waterflow().tail(24-stunden)

    wl = list(wl_actual)
    wl += [wl[-1]] * (24 - len(wl))

    wf = list(wf_actual)
    wf += [wf[-1]] * (24 - len(wf))

    return market_2_call_value(water_levels=wl, flow_rates=wf, strike=5000)


# --------------------------------------------------------------------
# Market 3 – Weather sum
# --------------------------------------------------------------------
def predict_market_3() -> int:
    temps = predict_temperature()
    hums  = predict_humidity()
    return market_3_settlement(temps, hums)


# --------------------------------------------------------------------
# Market 4 – Weather interaction (requires triplets)
# For naive prediction, we approximate:
#   (temp, median, mean) → (last_temp, last_temp, last_temp)
# --------------------------------------------------------------------
def predict_market_4() -> int:
    temps_series = predict_temperature()  # Series oder Liste
    hums_series = predict_humidity()  # Series oder Liste

    # Sicherstellen, dass wir einen DataFrame haben
    temps_df = pd.DataFrame({"data": temps_series})
    hums_df = pd.DataFrame({"data": hums_series})

    # Rolling Werte berechnen
    temps_df["mean"] = temps_df["data"].rolling(window=48, min_periods=1).mean()
    temps_df["median"] = temps_df["data"].rolling(window=48, min_periods=1).median()

    hums_df["mean"] = hums_df["data"].rolling(window=48, min_periods=1).mean()
    hums_df["median"] = hums_df["data"].rolling(window=48, min_periods=1).median()

    # Neuesten 48 Werte extrahieren
    temps_last48 = temps_df.tail(48)
    hums_last48 = hums_df.tail(48)

    # Tupel in der Form (value, median, mean)
    temps = list(zip(
        temps_last48["data"],
        temps_last48["median"],
        temps_last48["mean"]
    ))

    hums = list(zip(
        hums_last48["data"],
        hums_last48["median"],
        hums_last48["mean"]
    ))

    # → Jetzt korrekt callen
    result = market_4_settlement(temps, hums)
    return result


# --------------------------------------------------------------------
# Market 5 – Airport arrivals + departures
# --------------------------------------------------------------------
def predict_market_5() -> int:
    arr = predict_arrivals()
    dep = predict_departures()

    return market_5_settlement(arr, dep)


# --------------------------------------------------------------------
# Market 6 – Airport metric
# --------------------------------------------------------------------
def predict_market_6() -> int:
    arr = predict_arrivals()
    dep = predict_departures()
    return market_6_settlement(arr, dep)


# --------------------------------------------------------------------
# Market 7 – ETF
# --------------------------------------------------------------------
def predict_market_7() -> float:
    # Zeitpunkt heute um 10:00 Uhr
    heute_zehn = datetime.combine(datetime.today(), time(10, 0))

    # aktueller Zeitpunkt
    jetzt = datetime.now()

    # vergangene Stunden (abgerundet)
    stunden = int((jetzt - heute_zehn).total_seconds() // 3600)

    weighted_flow = (1 - (stunden / 24)) * PRIOR_FLOW + (stunden / 24) * get_waterflow().iloc[-1]

    weighted_level = (1 - (stunden / 24)) * PRIOR_LEVEL + (stunden / 24) * get_waterlevel().iloc[-1]

    filtered_dataframe = get_raw_data()
    filtered_dataframe.set_index("date", inplace=True)
    temp = filtered_dataframe["temperature_2m"].tail(1).iloc[-1]
    hum = filtered_dataframe["relative_humidity_2m"].tail(1).iloc[-1]

    flow = weighted_flow
    water = weighted_level
    airport_value = predict_market_6()

    print(f"{flow} {water} {temp} {hum} {airport_value}")

    return market_7_etf_settlement(flow, water, temp, hum, airport_value)
