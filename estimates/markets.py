import math
from typing import List, Tuple


# ---------------------------------------------------------
# Eisbach 1
# ---------------------------------------------------------
def market_1_settlement(flow_rate: float, water_level: float) -> int:
    """
    Settlement = round(flow_rate * water_level)
    """
    return round(flow_rate * water_level)


# ---------------------------------------------------------
# Eisbach 2
# ---------------------------------------------------------
def market_2_settlement(flow_rates: List[float], water_levels: List[float]) -> int:
    """
    Settlement = max(wl) - max(fr)  *  min(wl) - min(fr), rounded
    """
    if flow_rates.empty or water_levels.empty:
        raise ValueError("Lists cannot be empty")

    max_wl = max(water_levels)
    max_fr = max(flow_rates)
    min_wl = min(water_levels)
    min_fr = min(flow_rates)

    result = (max_wl - max_fr) * (min_wl - min_fr)
    return round(result)


def market_2_call_value(flow_rates: List[float],
                        water_levels: List[float],
                        strike: float = 5000) -> float:
    """
    Value = max(0, settlement - strike)
    """
    settlement = market_2_settlement(flow_rates, water_levels)
    return max(0.0, settlement - strike)


# ---------------------------------------------------------
# Weather data 3
# ---------------------------------------------------------
def market_3_settlement(temps: List[float], humidities: List[float]) -> int:
    """
    Settlement = |sum_over_intervals (temp*2 + humidity)|
    """
    if len(temps) != len(humidities):
        raise ValueError("Temperature and humidity list must match")

    total = sum(t * 2 + h for t, h in zip(temps, humidities))
    return abs(total)


# ---------------------------------------------------------
# Weather data 4
# ---------------------------------------------------------
def market_4_settlement(
    temps: List[Tuple[float, float, float]],
    humidities: List[Tuple[float, float, float]]
):
    """
    Temps list contains tuples: (temp, median_temp, mean_temp)
    Humidities list contains tuples: (humidity, median_hum, mean_hum)

    Formula:
    sum( temp * (mean_t - median_t) * (mean_h - median_h) )
    Settlement = |result|
    """

    #print(temps)
    #print(humidities)
    #print(zip(temps, humidities))

    if len(temps) != len(humidities):
        raise ValueError("Temperature and humidity list must match")

    total = 0
    for (T, medT, meanT), (H, medH, meanH) in zip(temps, humidities):
        diff = T * (meanT - medT) * (meanH - medH)
        total += diff

    return abs(round(total))


# ---------------------------------------------------------
# Airport 5
# ---------------------------------------------------------
def market_5_settlement(arrivals: List[int], departures: List[int]) -> int:
    """
    Settlement = 3 * sum(arrivals + departures)
    """
    if len(arrivals) != len(departures):
        raise ValueError("Arrivals and departures list must match")

    # TODO

    total = 3 * 833

    return total


# ---------------------------------------------------------
# Airport 6
# ---------------------------------------------------------
def airport_metric(arrivals: int, departures: int) -> float:
    """
    Single interval metric:
    300 * (arrivals - departures) / (arrivals + departures) ** 1.5
    """
    denom = (arrivals + departures)
    if denom == 0:
        return 0.0

    # TODO

    return 300 * (arrivals - departures) / (denom ** 1.5)


def market_6_settlement(arrivals: List[int], departures: List[int]) -> int:
    """
    Settlement = | sum(interval_metrics) |, rounded down (int)
    """
    if len(arrivals) != len(departures):
        raise ValueError("Arrivals and departures list must match")

    metrics = [
        airport_metric(a, d)
        for a, d in zip(arrivals, departures)
    ]
    return abs(int(sum(metrics)))


# ---------------------------------------------------------
# ETF 7
# ---------------------------------------------------------
def market_7_etf_settlement(
    flow_rate: float,
    water_level: float,
    temperature: float,
    humidity: float,
    airport_metric_sum: float
) -> float:
    """
    ETF = 0.3*flow + 0.1*water + 0.2*temp + 0.1*humidity + 0.3*airport_metric
    Settlement = absolute value
    """
    etf = (
        0.3 * flow_rate +
        0.1 * water_level +
        0.2 * temperature +
        0.1 * humidity +
        0.3 * airport_metric_sum
    )
    return abs(etf)
