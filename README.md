# Real-Time Weather Monitoring System

## Overview

This project is a real-time weather monitoring system that fetches data from the OpenWeatherMap API, processes it, and provides insights through daily summaries and alerts. It's built using FastAPI for the backend, with a simple HTML/JavaScript frontend for visualization.

## Features

- Real-time weather data retrieval for multiple cities
- Daily weather summaries including average, maximum, and minimum temperatures
- Dominant weather condition calculation
- Configurable alerting system for temperature thresholds
- Data visualization for current weather, historical trends, and alerts

## Tech Stack

- Backend: Python 3.8+, FastAPI
- Database: SQLite
- Frontend: HTML, JavaScript
- API: OpenWeatherMap

## Project Structure
realtime-weather-app/  
│  
├── app/  
│   ├── api/  
│   │   └── routes.py  
│   ├── core/  
│   │   ├── weather_service.py  
│   │   ├── data_processor.py  
│   │   └── aggregator.py  
│   ├── db/  
│   │   └── database.py  
│   ├── models/  
│   │   └── weather.py  
│   ├── config.py  
│   └── main.py  
├── static/  
│   └── index.html  
├── tests/  
│   └── test_weather_system.py  
├── requirements.txt  
└── README.md  

## Setup and Installation

1. Clone the repository:
```bash
git clone https://github.com/Omkar1279/realtime-weather-app.git
cd realtime-weather-app
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use venv\Scripts\activate
```

3. Install the required packages:
```bash
pip install -r requirements.txt
```

4. Set up your OpenWeatherMap API key:
Create a `.env` file in the root directory and add your API key:
```text
OPENWEATHERMAP_API_KEY=your_api_key_here
```
5. Run the application:
```bash
uvicorn app.main:app --reload
```
6. Open your web browser and navigate to `http://127.0.0.1:8000/static/index.html` to view the frontend.

## Usage

- The system automatically fetches weather data for the configured cities at regular intervals.
- View current weather conditions, daily summaries, and alerts on the frontend.
- Use the API endpoints to programmatically access weather data and set alert thresholds.

## API Endpoints

- GET `/current-weather`: Retrieve current weather for all configured cities
- GET `/weather-averages`: Get average weather data for all cities
- GET `/daily-summary/{city}`: Retrieve daily weather summary for a specific city
- GET `/alerts/{city}`: Get recent alerts for a specific city
- POST `/set-threshold`: Set a new alert threshold

## Running Tests

To run the test suite:
```bash
python -m unittest tests.test_weather_system
```