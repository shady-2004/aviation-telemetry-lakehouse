import io
import requests
from src.utils.minio_client import get_minio_client

OURAIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"

def ingest_outairports() -> None :
    minio_client = get_minio_client()

    # Ensure bucket exists
    if not minio_client.bucket_exists("bronze") :
        minio_client.make_bucket("bronze")

    print("Fetching Ourairports.com airports data...")
    
    response = requests.get(OURAIRPORTS_URL)
    response.raise_for_status()

    csv_bytes = response.content
    object_key = "ourairports/airports.csv"

    #Upload raw CSV bytes to Minio

    minio_client.put_object(
        bucket_name="bronze",
        object_name=object_key,
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv"
    )
    
    print(f"Uploaded to bronze/{object_key}")


if __name__ == "__main__" :     
    
    ingest_outairports()

    