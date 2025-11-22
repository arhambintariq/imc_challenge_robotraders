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
    # print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation: {response.Elevation()} m asl")
    # print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    # print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

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
    filtered_dataframe.set_index("date", inplace=True)
    filtered_dataframe = filtered_dataframe.resample('30min').mean()
    # print("\nFiltered DataFrame from 2025-11-22 10:00 to 2025-11-23 10:00:\n", filtered_dataframe)
    # print(filtered_dataframe.describe())
    # print(filtered_dataframe)
    return filtered_dataframe

def get_3_weather_prediction():
    # Get the filtered data
    df = get_raw_data()

    # Calculate the desired value: temperature * 2 + humidity
    df['calculated_value'] = (df['temperature_2m'] * 2) + df['relative_humidity_2m']

    # Sum up the calculated values for all 48 bins
    total_sum = int(df['calculated_value'].sum())

    print("\n3_Weather", total_sum)
    return total_sum

def get_4_weather_prediction():
    """
    Compute sum over 30min bins of:
      temperature * ((mean(temp) - median(temp)) * (mean(humidity) - median(humidity)))

    Means/medians use an expanding window (everything up to the current 30min bin).
    """
    df = get_raw_data()
    # 1. Calculate Expanding Mean and Median for Temperature
    # min_periods=1 ensures the first row is calculated using just itself
    t_expanding_mean = df['temperature_2m'].expanding(min_periods=1).mean()
    t_expanding_median = df['temperature_2m'].expanding(min_periods=1).median()

    # 2. Calculate Expanding Mean and Median for Humidity
    h_expanding_mean = df['relative_humidity_2m'].expanding(min_periods=1).mean()
    h_expanding_median = df['relative_humidity_2m'].expanding(min_periods=1).median()

    # 3. Apply the specific formula per bin
    # Note: For the very first row, Mean == Median, so the result will be 0.
    term_temp_diff = t_expanding_mean - t_expanding_median
    term_hum_diff = h_expanding_mean - h_expanding_median
    
    df['bin_metric'] = (df['temperature_2m'] + df["relative_humidity_2m"]) * term_temp_diff * term_hum_diff

    # 4. Compute the final sum over all bins
    total_sum = df['bin_metric'].sum()
    print("\n4_Weather", abs(int(total_sum)))
    return total_sum



if __name__ == "__main__":
    get_3_weather_prediction()
    get_4_weather_prediction()