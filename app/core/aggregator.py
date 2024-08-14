from app.models.weather import WeatherData
from collections import defaultdict
from typing import List, Dict
from datetime import datetime, date
from app.db.database import get_db_connection
from statistics import mode


class Aggregator:
    def __init__(self):
        self.data: Dict[str, List[WeatherData]] = defaultdict(list)

    def add_data(self, weather_data: WeatherData):
        self.data[weather_data.city].append(weather_data)

    def get_city_average(self, city: str) -> dict:
        city_data = self.data[city]
        if not city_data:
            return {}

        avg_temp = sum(data.temp for data in city_data) / len(city_data)
        avg_feels_like = sum(data.feels_like for data in city_data) / len(city_data)

        return {
            "city": city,
            "avg_temp": round(avg_temp, 2),
            "avg_feels_like": round(avg_feels_like, 2),
            "data_points": len(city_data)
        }

    def get_all_cities_average(self) -> List[dict]:
        return [self.get_city_average(city) for city in self.data.keys()]

    def _update_daily_summary(self, city: str, date: date):
        daily_data = self.daily_data[city][date]
        if len(daily_data) > 0:
            avg_temp = sum(data.temp for data in daily_data) / len(daily_data)
            max_temp = max(data.temp for data in daily_data)
            min_temp = min(data.temp for data in daily_data)
            dominant_condition = mode(data.main for data in daily_data)

            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO daily_summaries 
                    (date, city, avg_temp, max_temp, min_temp, dominant_condition)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (date.isoformat(), city, avg_temp, max_temp, min_temp, dominant_condition))
                conn.commit()

    def _check_alerts(self, weather_data: WeatherData):
        if weather_data.temp > self.thresholds["high_temp"]:
            recent_data = self.data[weather_data.city][-2:]
            if len(recent_data) == 2 and all(data.temp > self.thresholds["high_temp"] for data in recent_data):
                self._trigger_alert(weather_data.city, "High Temperature",
                                    f"Temperature exceeds {self.thresholds['high_temp']}°C for two consecutive updates")

    def _trigger_alert(self, city: str, alert_type: str, message: str):
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO alerts (city, alert_type, message)
                VALUES (?, ?, ?)
            ''', (city, alert_type, message))
            conn.commit()
        print(f"ALERT for {city}: {alert_type} - {message}")  # Console alert

    def set_threshold(self, threshold_name: str, value: float):
        self.thresholds[threshold_name] = value

    def get_daily_summaries(self, city: str, start_date: date, end_date: date) -> List[dict]:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM daily_summaries
                WHERE city = ? AND date BETWEEN ? AND ?
                ORDER BY date
            ''', (city, start_date.isoformat(), end_date.isoformat()))
            return [
                {
                    "date": row[0],
                    "city": row[1],
                    "avg_temp": row[2],
                    "max_temp": row[3],
                    "min_temp": row[4],
                    "dominant_condition": row[5]
                }
                for row in cursor.fetchall()
            ]

    def get_alerts(self, city: str, limit: int = 10) -> List[dict]:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM alerts
                WHERE city = ?
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (city, limit))
            return [
                {
                    "id": row[0],
                    "city": row[1],
                    "alert_type": row[2],
                    "message": row[3],
                    "timestamp": row[4]
                }
                for row in cursor.fetchall()
            ]
