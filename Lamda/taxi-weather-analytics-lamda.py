import os
import json
import boto3
import urllib.request
from datetime import datetime, timedelta

def lambda_handler(event, context):
    # --- Config ---
    # Get S3 bucket name from environment variable, fallback to default if not set
    S3_BUCKET = os.getenv("S3_BUCKET", "taxi-weather-analytics-s3-bucket")
    # Get city name from environment variable, default to "New York"
    CITY = os.getenv("CITY", "New%20York")

    # Coordinates for New York City (latitude, longitude)
    LAT, LON = "40.7128", "-74.0060"

    # --- Date range: July 2025 (explicit) ---
    # Define the start and end dates for the weather data request
    start_date = datetime(2025, 7, 1).date()
    end_date   = datetime(2025, 7, 31).date()

    # --- Setup S3 client ---
    # Initialize a boto3 S3 client to interact with AWS S3
    s3 = boto3.client("s3")

    # Construct the Open-Meteo API URL for historical weather data
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={LAT}&longitude={LON}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&timezone=America/New_York"
    )

    print(f"Fetching weather data from: {url}")

    try:
        # Make HTTP request to the weather API
        with urllib.request.urlopen(url) as response:
            # Parse JSON response into Python dictionary
            weather_data = json.loads(response.read().decode("utf-8"))

        # Define S3 folder structure: landing/nyc/weather/YYYY/MM
        folder_path = f"landing/nyc/weather/{start_date.year}/{start_date.month:02d}"
        # File will be named like: weather_2025-07.json
        filename = f"weather_{start_date.strftime('%Y-%m')}.json"
        # Full S3 key (path + filename)
        s3_path = f"{folder_path}/{filename}"

        # Upload the weather data JSON to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_path,
            Body=json.dumps(weather_data),
            ContentType="application/json"
        )

        print(f"✅ Uploaded {filename} to s3://{S3_BUCKET}/{s3_path}")

    except Exception as e:
        # Handle errors gracefully and return error status
        print(f"⚠️ Error fetching data: {e}")
        return {"status": "error", "message": str(e)}

    # Return success status if everything worked
    return {"status": "success"}