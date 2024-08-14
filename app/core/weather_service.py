import aiohttp
import asyncio
from app.config import settings
from app.models.weather import WeatherData
from app.utils.time_converter import unix_to_datetime
from typing import List
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


class WeatherService:
    BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

    async def get_weather_data(self, city: str) -> WeatherData:
        params = {
            "q": city,
            "appid": os.getenv('OPENWEATHERMAP_API_KEY'),
            "units": "metric"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(self.BASE_URL, params=params) as response:
                data = await response.json()

                return WeatherData(
                    city=city,
                    main=data["weather"][0]["main"],
                    temp=data["main"]["temp"],
                    feels_like=data["main"]["feels_like"],
                    dt=unix_to_datetime(data["dt"])
                )

    async def get_all_cities_weather(self) -> List[WeatherData]:
        tasks = [self.get_weather_data(city) for city in settings.CITIES]
        return await asyncio.gather(*tasks)
