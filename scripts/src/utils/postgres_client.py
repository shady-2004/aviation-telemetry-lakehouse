import psycopg2

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5433
POSTGRES_DB = "aviation_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

def get_db_connection():
    """Establishes connection to the local Postgres container."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
