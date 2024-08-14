from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.api.routes import router
import asyncio
from app.core.weather_service import WeatherService
from app.core.aggregator import Aggregator
from app.config import settings

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(router)

weather_service = WeatherService()
aggregator = Aggregator()


async def update_weather_data():
    while True:
        weather_data = await weather_service.get_all_cities_weather()
        for data in weather_data:
            aggregator.add_data(data)
        await asyncio.sleep(settings.API_CALL_INTERVAL)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_weather_data())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
