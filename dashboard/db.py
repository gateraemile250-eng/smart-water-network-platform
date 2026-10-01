import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


# Load environment variables from the project PostgreSQL configuration.
load_dotenv("infrastructure/postgres/.env")


def get_engine():
    """
    Create and return a SQLAlchemy engine for the Smart Water
    PostgreSQL database.
    """

    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=os.getenv("POSTGRES_USER", "smart_water_user"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "smart_water"),
    )

    return create_engine(connection_url)