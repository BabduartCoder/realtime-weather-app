import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from app.core.weather_service import WeatherService
from app.core.data_processor import DataProcessor
from app.core.aggregator import Aggregator
from app.models.weather import WeatherData
from app.config import settings


class TestWeatherSystem(unittest.TestCase):

    def setUp(self):
        self.weather_service = WeatherService()
        self.data_processor = DataProcessor()
        self.aggregator = Aggregator()

    @patch('app.core.weather_service.aiohttp.ClientSession.get')
    async def test_system_setup_and_api_connection(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "weather": [{"main": "Clear"}],
            "main": {"temp": 293.15, "feels_like": 292.15},
            "dt": 1628097600
        }
        mock_get.return_value.__aenter__.return_value = mock_response

        weather_data = await self.weather_service.get_weather_data("Test City")

        self.assertIsNotNone(weather_data)
        self.assertEqual(weather_data.city, "Test City")
        self.assertEqual(weather_data.main, "Clear")

    @patch('app.core.weather_service.aiohttp.ClientSession.get')
    async def test_data_retrieval_and_parsing(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "weather": [{"main": "Rain"}],
            "main": {"temp": 283.15, "feels_like": 282.15},
            "dt": 1628097600
        }
        mock_get.return_value.__aenter__.return_value = mock_response

        weather_data = await self.weather_service.get_weather_data("Test City")

        self.assertEqual(weather_data.main, "Rain")
        self.assertAlmostEqual(weather_data.temp, 283.15)
        self.assertAlmostEqual(weather_data.feels_like, 282.15)
        self.assertEqual(weather_data.dt, datetime.fromtimestamp(1628097600))

    def test_temperature_conversion(self):
        kelvin_temp = 293.15
        celsius_temp = self.data_processor.kelvin_to_celsius(kelvin_temp)
        self.assertAlmostEqual(celsius_temp, 20.0, places=1)

    def test_daily_weather_summary(self):
        city = "Test City"
        date = datetime.now().date()

        # Simulate weather updates for a day
        weather_data = [
            WeatherData(city=city, main="Clear", temp=20.0, feels_like=19.0, dt=datetime.now()),
            WeatherData(city=city, main="Clear", temp=25.0, feels_like=24.0, dt=datetime.now()),
            WeatherData(city=city, main="Rain", temp=18.0, feels_like=17.0, dt=datetime.now()),
        ]

        for data in weather_data:
            self.aggregator.add_data(data)

        summary = self.aggregator.get_daily_summaries(city, date, date)[0]

        self.assertEqual(summary['city'], city)
        self.assertAlmostEqual(summary['avg_temp'], 21.0, places=1)
        self.assertAlmostEqual(summary['max_temp'], 25.0)
        self.assertAlmostEqual(summary['min_temp'], 18.0)
        self.assertEqual(summary['dominant_condition'], "Clear")



if __name__ == '__main__':
    unittest.main()