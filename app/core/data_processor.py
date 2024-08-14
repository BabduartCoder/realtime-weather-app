from app.models.weather import WeatherData


class DataProcessor:
    def process_weather_data(self, data: WeatherData) -> dict:
        return {
            "city": data.city,
            "main": data.main,
            "temp": round(data.temp, 2),
            "feels_like": round(data.feels_like, 2),
            "dt": data.dt.isoformat()
        }

    def kelvin_to_celsius(self, kelvin_temp: float) -> float:
        """
        Convert temperature from Kelvin to Celsius.
        """
        return kelvin_temp - 273.15
