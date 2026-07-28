import io
import requests
from datetime import datetime, timezone
from src.utils.minio_client import get_minio_client

OPENSKY_URL = "https://opensky-network.org/api/states/all"

def ingest_opensky() -> None :
    minio_client = get_minio_client()

    # Ensure bucket exists
    if not minio_client.bucket_exists("bronze") :
        minio_client.make_bucket("bronze")

    print("Fetching live flight telemetry from OpenSky API...")
    
    response = requests.get(OPENSKY_URL,timeout=10)
    response.raise_for_status()

    json_bytes = response.content

    # Partition by UTC date and timestamp snapshot filename
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M%S")
    object_key = f"opensky/date={date_str}/states_{time_str}.json"

    #Upload raw JSON bytes to Minio

    minio_client.put_object(
        bucket_name="bronze",
        object_name=object_key,
        data=io.BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json"
    )
    
    print(f"Uploaded to bronze/{object_key}")


if __name__ == "__main__" :     
    
    ingest_opensky()

    