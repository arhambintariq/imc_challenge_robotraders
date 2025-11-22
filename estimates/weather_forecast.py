import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

def get_raw_data():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.08,
        "longitude": 11.28,
        "minutely_15": ["temperature_2m", "relative_humidity_2m"],
        "timezone": "Europe/Berlin",
        "forecast_days": 1,
        "temperature_unit": "fahrenheit",
        "forecast_minutely_15": 96,
        "past_minutely_15": 96,
    }
    responses = openmeteo.weather_api(url, params=params)

    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    minutely_15 = response.Minutely15()
    minutely_15_temperature_2m = minutely_15.Variables(0).ValuesAsNumpy()
    minutely_15_relative_humidity_2m = minutely_15.Variables(1).ValuesAsNumpy()

    minutely_15_data = {
        "date": pd.date_range(
            start=pd.to_datetime(minutely_15.Time(), unit="s", utc=True),
            end=pd.to_datetime(minutely_15.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=minutely_15.Interval()),
            inclusive="left"
        ).tz_convert("Europe/Berlin")  # Convert to Berlin timezone
    }

    minutely_15_data["temperature_2m"] = minutely_15_temperature_2m
    minutely_15_data["relative_humidity_2m"] = minutely_15_relative_humidity_2m

    minutely_15_dataframe = pd.DataFrame(data=minutely_15_data)

    # Define the start and end datetime for filtering
    start_time = pd.Timestamp("2025-11-22 10:15:00", tz="Europe/Berlin")
    end_time = pd.Timestamp("2025-11-23 10:00:00", tz="Europe/Berlin")

    # Filter the DataFrame for the specified date range
    filtered_dataframe = minutely_15_dataframe[(minutely_15_dataframe["date"] >= start_time) & 
                                                (minutely_15_dataframe["date"] <= end_time)]

    # print("\nFiltered DataFrame from 2025-11-22 10:00 to 2025-11-23 10:00:\n", filtered_dataframe)
    print(filtered_dataframe.describe())
    return filtered_dataframe

def get_3_weather_prediction():
    # Get the filtered data
    filtered_dataframe = get_raw_data()

    # Calculate the mean for each 30-minute interval
    filtered_dataframe.set_index("date", inplace=True)
    mean_values = filtered_dataframe.resample('30min').mean()
    print(mean_values)
    # Calculate the desired value: temperature * 2 + humidity
    mean_values['calculated_value'] = (mean_values['temperature_2m'] * 2) + mean_values['relative_humidity_2m']

    # Sum up the calculated values for all 48 bins
    total_sum = int(mean_values['calculated_value'].sum())

    print("\n3_Weather", total_sum)
    return total_sum

def get_4_weather_prediction(min_periods=1):
    """
    Compute sum over 30min bins of:
      temperature * (mean(temp) - median(temp) * (mean(humidity) - median(humidity)))

    Means/medians use an expanding window (everything up to the current 30min bin).
    """
    # get 15min filtered data and aggregate to 30min bins (mean of two 15min samples)
    df = get_raw_data()
    df = df.set_index("date")
    mean_values = df.resample("30min").mean()

    # expanding (everything up to current) statistics on the 30min series
    rm_temp = mean_values["temperature_2m"].expanding(min_periods=min_periods).mean()
    rd_temp = mean_values["temperature_2m"].expanding(min_periods=min_periods).median()
    rm_hum = mean_values["relative_humidity_2m"].expanding(min_periods=min_periods).mean()
    rd_hum = mean_values["relative_humidity_2m"].expanding(min_periods=min_periods).median()

    # elementwise expression and sum
    expr = mean_values["temperature_2m"] * ((rm_temp - rd_temp) * (rm_hum - rd_hum))
    total_sum = float(expr.sum())

    print(f"4_Weather: {total_sum}")
    return total_sum



if __name__ == "__main__":
    get_3_weather_prediction()
    get_4_weather_prediction()