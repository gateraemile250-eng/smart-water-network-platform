"""PostgreSQL connection utilities for the smart water platform."""

import os
from pathlib import Path

import pg8000.dbapi
from dotenv import load_dotenv


ENV_FILE = Path("infrastructure/postgres/.env")


def get_database_connection():
    """Create a PostgreSQL connection using local environment settings."""

    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"Database environment file not found: {ENV_FILE}"
        )

    load_dotenv(ENV_FILE, override=True)

    required_variables = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise EnvironmentError(
            "Missing database environment variables: "
            + ", ".join(missing_variables)
        )

    return pg8000.dbapi.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )