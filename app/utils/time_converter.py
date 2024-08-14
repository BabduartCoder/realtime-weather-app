from datetime import datetime


def unix_to_datetime(unix_timestamp: int) -> datetime:
    return datetime.fromtimestamp(unix_timestamp)
