from fastapi import APIRouter, Depends, Query
from app.core.weather_service import WeatherService
from app.core.data_processor import DataProcessor
from app.core.aggregator import Aggregator
from datetime import date

router = APIRouter()

weather_service = WeatherService()
data_processor = DataProcessor()
aggregator = Aggregator()


@router.get("/current-weather")
async def get_current_weather():
    weather_data = await weather_service.get_all_cities_weather()
    processed_data = [data_processor.process_weather_data(data) for data in weather_data]

    for data in weather_data:
        aggregator.add_data(data)

    return processed_data


@router.get("/weather-averages")
async def get_weather_averages():
    return aggregator.get_all_cities_average()


@router.get("/daily-summary/{city}")
async def get_daily_summary(city: str, start_date: date = Query(...), end_date: date = Query(...)):
    return aggregator.get_daily_summaries(city, start_date, end_date)


@router.get("/alerts/{city}")
async def get_alerts(city: str, limit: int = 10):
    return aggregator.get_alerts(city, limit)


@router.post("/set-threshold")
async def set_threshold(threshold_name: str, value: float):
    aggregator.set_threshold(threshold_name, value)
    return {"message": f"Threshold {threshold_name} set to {value}"}
