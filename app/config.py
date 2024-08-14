from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    OPENWEATHERMAP_API_KEY: str
    API_CALL_INTERVAL: int = 300  # 5 minutes in seconds
    CITIES: list = ["Delhi", "Mumbai", "Chennai", "Bangalore", "Kolkata", "Hyderabad"]

    class Config:
        env_file = ".env"


settings = Settings()
