import os
import json
import datetime as dt
import requests

API_KEY   = os.environ["API_KEY"]
LOCATION  = os.environ.get("LOCATION","59.3293,18.0686")#"Stockholm")
DATE      = os.environ.get("DATE", dt.date.today().isoformat())

def fetch_weather(location: str, date: str) -> dict:
    url = "http://api.weatherapi.com/v1/history.json"
    params = {"key": API_KEY, "q": location, "dt": date}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def write_weather(data: dict):
    with open("weather_raw.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def main():
    print(f"Starting run: location={LOCATION} date={DATE}")
    data = fetch_weather(LOCATION, DATE)
    write_weather(data)
    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    main()