import io
import pandas as pd
from src.utils.postgres_client import get_db_connection
from src.utils.minio_client import get_minio_client

def ingest_postgres () -> None:
    minio_client = get_minio_client()
    conn = get_db_connection()
    
    # Ensure bucket exists
    if not minio_client.bucket_exists("bronze") :
        minio_client.make_bucket("bronze")
        
    for table_name in ["raw_airlines", "raw_routes"]:
        df = pd.read_sql_query("SELECT * FROM " + table_name,con = conn)

        # DataFrame -> Parquet bytes in RAM
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        parquet_bytes = buffer.getvalue()

        object_key = f"postgres/{table_name.replace('raw_','')}.parquet"

        minio_client.put_object(
            bucket_name="bronze",
            object_name=object_key,
            data=io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
        )

        print(f"Uploaded to bronze/{object_key}")


if __name__ == "__main__":
    ingest_postgres()

        
        
        
        