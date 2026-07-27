import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import requests

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5433
POSTGRES_DB = "aviation_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

AIRLINES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"
ROUTES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"


def get_db_connection():
    """Establishes connection to the local Postgres container."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def create_tables(conn):
    """Creates raw tables in Postgres if they do not exist."""
    print("Creating Postgres tables: 'raw_airlines' and 'raw_routes'...")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_airlines (
                airline_id INT PRIMARY KEY,
                name VARCHAR(255),
                alias VARCHAR(255),
                iata VARCHAR(10),
                icao VARCHAR(10),
                callsign VARCHAR(255),
                country VARCHAR(255),
                active VARCHAR(1)
            );

            CREATE TABLE IF NOT EXISTS raw_routes (
                airline VARCHAR(10),
                airline_id VARCHAR(10),
                source_airport VARCHAR(10),
                source_airport_id VARCHAR(10),
                destination_airport VARCHAR(10),
                destination_airport_id VARCHAR(10),
                codeshare VARCHAR(5),
                stops INT,
                equipment VARCHAR(255)
            );
        """)
        conn.commit()


def seed_airlines(conn):
    """Downloads and seeds airlines data."""
    print("Downloading OpenFlights Airlines data...")
    response = requests.get(AIRLINES_URL, timeout=15)
    response.raise_for_status()

    cols = ["airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active"]
    df = pd.read_csv(io.StringIO(response.text), header=None, names=cols, na_values="\\N")

    # Filter out invalid IDs or missing records
    df = df.dropna(subset=["airline_id"])
    df["airline_id"] = df["airline_id"].astype(int)

    query = """
        INSERT INTO raw_airlines (airline_id, name, alias, iata, icao, callsign, country, active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (airline_id) DO NOTHING;
    """
    records = df.where(pd.notnull(df), None).values.tolist()

    with conn.cursor() as cur:
        execute_batch(cur, query, records)
        conn.commit()

    print(f"Loaded {len(df)} records into 'raw_airlines'.")


def seed_routes(conn):
    """Downloads and seeds flight routes data."""
    print("Downloading OpenFlights Routes data...")
    response = requests.get(ROUTES_URL, timeout=15)
    response.raise_for_status()

    cols = [
        "airline", "airline_id", "source_airport", "source_airport_id",
        "destination_airport", "destination_airport_id", "codeshare", "stops", "equipment"
    ]
    df = pd.read_csv(io.StringIO(response.text), header=None, names=cols, na_values="\\N")

    query = """
        INSERT INTO raw_routes (airline, airline_id, source_airport, source_airport_id,
                                destination_airport, destination_airport_id, codeshare, stops, equipment)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    records = df.where(pd.notnull(df), None).values.tolist()

    with conn.cursor() as cur:
        # Clear existing data before re-seeding routes
        cur.execute("TRUNCATE TABLE raw_routes;")
        execute_batch(cur, query, records)
        conn.commit()

    print(f"Loaded {len(df)} records into 'raw_routes'.")


def main():
    try:
        conn = get_db_connection()
        create_tables(conn)
        seed_airlines(conn)
        seed_routes(conn)
        conn.close()
        print("Postgres database successfully seeded!")
    except Exception as e:
        print(f"Postgres seeding failed: {str(e)}")


if __name__ == "__main__":
    main()