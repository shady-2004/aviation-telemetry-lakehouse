import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from src.utils.postgres_client import get_db_connection

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