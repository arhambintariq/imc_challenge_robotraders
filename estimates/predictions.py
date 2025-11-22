import pandas as pd

from estimates.past_data_scraper import *


def load_last_value(csv_path: str) -> float:
    """
    Load a CSV with format: timestamp | value
    Returns the last (most recent) value.
    """
    df = pd.read_csv(csv_path)
    if df.shape[1] < 2:
        raise ValueError("CSV must have at least two columns: timestamp | value")

    # assume the last row contains the most recent value
    last_value = df.iloc[-1, 1]
    return float(last_value)


# ---------------------------------------------------------
# Market 24 — Eisbach Flow Rate Prediction
# ---------------------------------------------------------
def predict_flow_rate(csv_path: str) -> float:
    data = get_waterflow()

    # dedicated prediction

    return data.iloc[-1]


# Market 24 — Eisbach Water Level Prediction
def predict_water_level(csv_path: str) -> float:
    data = get_waterlevel()

    # dedicated prediction

    return data.iloc[-1]


# ---------------------------------------------------------
# Market 26 & 27 — Munich Temperature Prediction
# ---------------------------------------------------------
def predict_temperature():
    data = get_temperature()
    return data.tail(48)


# Market 26 & 27 — Munich Humidity Prediction
def predict_humidity():
    data = get_humidity()
    return data.tail(48)


# ---------------------------------------------------------
# Market 28 & 29 — Airport Arrivals Prediction
# ---------------------------------------------------------
def predict_arrivals():
    return []


# Market 28 & 29 — Airport Departures Prediction
def predict_departures():
    return []
