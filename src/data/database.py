"""PostgreSQL connection utilities for the smart water platform."""

import os
from pathlib import Path

import pg8000.dbapi
from dotenv import load_dotenv


ENV_FILE = Path("infrastructure/postgres/.env")

REQUIRED_VARIABLES = [
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
]


def get_database_connection():
    """Create a PostgreSQL connection using environment settings.

    Values already present in the runtime environment (Docker, CI,
    cloud) always win. The local .env file is optional and is only
    used to fill in values that are not already set.
    """

    if ENV_FILE.exists():
        load_dotenv(ENV_FILE, override=False)

    missing_variables = [
        variable
        for variable in REQUIRED_VARIABLES
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise EnvironmentError(
            "Missing database environment variables: "
            + ", ".join(missing_variables)
            + f". Set them in the environment or in {ENV_FILE} "
            "(see infrastructure/postgres/.env.example)."
        )

    return pg8000.dbapi.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )