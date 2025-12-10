import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

latitudes = [53.7799, 52.2298, 54.3523, 52.4069, 50.0614, 51.1, 53.4289, 53.1333, 50.0413, 53.1235, 51.9355, 51.25]
longitudes = [20.4942, 21.0118, 18.6491, 16.9299, 19.9366, 17.0333, 14.553, 23.1643, 21.999, 18.0076, 15.5064, 22.5667]

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": latitudes,
    "longitude": longitudes,
    "start_date": "2025-04-30",
    "end_date": "2025-05-14",
    "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "cloud_cover_mean",
              "relative_humidity_2m_mean", "soil_moisture_7_to_28cm_mean", "soil_moisture_0_to_7cm_mean",
              "soil_temperature_0_to_7cm_mean", "soil_temperature_7_to_28cm_mean", "rain_sum", "precipitation_hours",
              "wind_speed_10m_max", "daylight_duration", "snowfall_sum", "surface_pressure_mean",
              "surface_pressure_max", "surface_pressure_min", "winddirection_10m_dominant",
              "et0_fao_evapotranspiration_sum", "wet_bulb_temperature_2m_mean", "wet_bulb_temperature_2m_min",
              "wet_bulb_temperature_2m_max", "wind_speed_10m_mean", "wind_gusts_10m_mean"],
    "timezone": "Europe/Berlin"
}

output = """temperature_2m_max,temperature_2m_min,temperature_2m_mean,cloud_cover_mean,relative_humidity_2m_mean,
soil_moisture_7_to_28cm_mean,soil_moisture_0_to_7cm_mean,soil_temperature_0_to_7cm_mean,
soil_temperature_7_to_28cm_mean,rain_sum,precipitation_hours,daylight_duration,snowfall_sum,surface_pressure_mean,
et0_fao_evapotranspiration_sum"""

responses = openmeteo.weather_api(url, params=params)


def find_city(lat, lon, city_dict, tolerance=0.0001):
    for city, coords in city_dict.items():
        if abs(coords["latitude"] - lat) < tolerance and abs(coords["longitude"] - lon) < tolerance:
            return city
    return "City not found"


city_coordinates = {
    "Olsztyn": {"latitude": 53.7799, "longitude": 20.4942},
    "Warszawa": {"latitude": 52.2298, "longitude": 21.0118},
    "Gdansk": {"latitude": 54.3523, "longitude": 18.6491},
    "Poznan": {"latitude": 52.4069, "longitude": 16.9299},
    "Krakow": {"latitude": 50.0614, "longitude": 19.9366},
    "Wroclaw": {"latitude": 51.1, "longitude": 17.0333},
    "Szczecin": {"latitude": 53.4289, "longitude": 14.553},
    "Bialystok": {"latitude": 53.1333, "longitude": 23.1643},
    "Rzeszow": {"latitude": 50.0413, "longitude": 21.999},
    "Lodz": {"latitude": 53.1235, "longitude": 18.0076},
    "Zielona Gora": {"latitude": 51.9355, "longitude": 15.5064},
    "Lublin": {"latitude": 51.25, "longitude": 22.5667}
}
main_dataframe = None
index = 0
# Process first location. Add a for-loop for multiple locations or weather models
for response in responses:
    city = find_city(latitudes[index], longitudes[index], city_coordinates)
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")

    daily = response.Daily()
    daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(2).ValuesAsNumpy()
    daily_cloud_cover_mean = daily.Variables(3).ValuesAsNumpy()
    daily_relative_humidity_2m_mean = daily.Variables(4).ValuesAsNumpy()
    daily_soil_moisture_7_to_28cm_mean = daily.Variables(5).ValuesAsNumpy()
    daily_soil_moisture_0_to_7cm_mean = daily.Variables(6).ValuesAsNumpy()
    daily_soil_temperature_0_to_7cm_mean = daily.Variables(7).ValuesAsNumpy()
    daily_soil_temperature_7_to_28cm_mean = daily.Variables(8).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(9).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(10).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(11).ValuesAsNumpy()
    daily_daylight_duration = daily.Variables(12).ValuesAsNumpy()
    daily_snowfall_sum = daily.Variables(13).ValuesAsNumpy()
    daily_surface_pressure_mean = daily.Variables(14).ValuesAsNumpy()
    daily_surface_pressure_max = daily.Variables(15).ValuesAsNumpy()
    daily_surface_pressure_min = daily.Variables(16).ValuesAsNumpy()
    daily_winddirection_10m_dominant = daily.Variables(17).ValuesAsNumpy()
    daily_et0_fao_evapotranspiration_sum = daily.Variables(18).ValuesAsNumpy()
    daily_wet_bulb_temperature_2m_mean = daily.Variables(19).ValuesAsNumpy()
    daily_wet_bulb_temperature_2m_min = daily.Variables(20).ValuesAsNumpy()
    daily_wet_bulb_temperature_2m_max = daily.Variables(21).ValuesAsNumpy()
    daily_wind_speed_10m_mean = daily.Variables(22).ValuesAsNumpy()
    daily_wind_gusts_10m_mean = daily.Variables(23).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}

    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["cloud_cover_mean"] = daily_cloud_cover_mean
    daily_data["relative_humidity_2m_mean"] = daily_relative_humidity_2m_mean
    daily_data["soil_moisture_7_to_28cm_mean"] = daily_soil_moisture_7_to_28cm_mean
    daily_data["soil_moisture_0_to_7cm_mean"] = daily_soil_moisture_0_to_7cm_mean
    daily_data["soil_temperature_0_to_7cm_mean"] = daily_soil_temperature_0_to_7cm_mean
    daily_data["soil_temperature_7_to_28cm_mean"] = daily_soil_temperature_7_to_28cm_mean
    daily_data["rain_sum"] = daily_rain_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["daylight_duration"] = daily_daylight_duration
    daily_data["snowfall_sum"] = daily_snowfall_sum
    daily_data["surface_pressure_mean"] = daily_surface_pressure_mean
    daily_data["surface_pressure_max"] = daily_surface_pressure_max
    daily_data["surface_pressure_min"] = daily_surface_pressure_min
    daily_data["winddirection_10m_dominant"] = daily_winddirection_10m_dominant
    daily_data["et0_fao_evapotranspiration_sum"] = daily_et0_fao_evapotranspiration_sum
    daily_data["wet_bulb_temperature_2m_mean"] = daily_wet_bulb_temperature_2m_mean
    daily_data["wet_bulb_temperature_2m_min"] = daily_wet_bulb_temperature_2m_min
    daily_data["wet_bulb_temperature_2m_max"] = daily_wet_bulb_temperature_2m_max
    daily_data["wind_speed_10m_mean"] = daily_wind_speed_10m_mean
    daily_data["wind_gusts_10m_mean"] = daily_wind_gusts_10m_mean
    daily_data["city"] = city

    daily_dataframe = pd.DataFrame(data=daily_data)
    if index == 0:
        main_dataframe = daily_dataframe
    else:
        main_dataframe = pd.concat([main_dataframe, daily_dataframe], ignore_index=True)

    index += 1

main_dataframe.to_csv(f'data_concat_10y.csv', index=False)
