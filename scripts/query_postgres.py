import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from seed_postgres import get_db_connection


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5433
POSTGRES_DB = "aviation_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

if __name__ == "__main__":
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM raw_airlines LIMIT 10")
    rows = cur.fetchall()
    print("Airlines Data:", rows)
    
    cur.execute("SELECT * FROM raw_routes LIMIT 10")
    rows = cur.fetchall()
    print("Routes Data:", rows)
    
    cur.close()
    conn.close()