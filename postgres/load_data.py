import argparse
import json
import logging
import sys
from pathlib import Path

import boto3
import pandas as pd
from sqlalchemy import create_engine, insert, text
from sqlalchemy.orm import Session

from postgres.models import Airline, Airport, Flight


FLIGHTS_NROWS = 500_000
FLIGHTS_CHUNK_SIZE = 50_000


def configure_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(__name__)


logger = configure_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga datos a PostgreSQL RDS")
    parser.add_argument("--host", required=True, help="Endpoint primario de RDS")
    parser.add_argument("--secret-name", required=True, help="Secret de Secrets Manager")
    parser.add_argument("--data-dir", required=True, help="Directorio local con los CSVs")
    parser.add_argument("--region", default="us-east-1", help="Región AWS")
    return parser.parse_args()


def get_credentials(secret_name: str, region: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret["SecretString"])


def build_connection_url(host: str, creds: dict) -> str:
    return (
        f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
        f"@{host}:{creds['port']}/{creds['dbname']}"
    )


def truncate_tables(session: Session) -> None:
    session.execute(text("TRUNCATE TABLE flights, airports, airlines RESTART IDENTITY CASCADE"))
    session.commit()


def load_airlines(data_dir: Path) -> list[dict]:
    df = pd.read_csv(data_dir / "airlines.csv")
    df.columns = [c.strip().upper() for c in df.columns]

    records = df.rename(
        columns={
            "IATA_CODE": "iata_code",
            "AIRLINE": "airline",
        }
    ).to_dict(orient="records")

    assert len(records) > 0, "airlines.csv está vacío"
    return records


def load_airports(data_dir: Path) -> list[dict]:
    df = pd.read_csv(data_dir / "airports.csv")
    df.columns = [c.strip().upper() for c in df.columns]

    rename_map = {
        "IATA_CODE": "iata_code",
        "AIRPORT": "airport",
        "CITY": "city",
        "STATE": "state",
        "COUNTRY": "country",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
    }

    df = df.rename(columns=rename_map)

    for col in ["country", "latitude", "longitude"]:
        if col not in df.columns:
            df[col] = None

    records = df[
        ["iata_code", "airport", "city", "state", "country", "latitude", "longitude"]
    ].to_dict(orient="records")

    assert len(records) > 0, "airports.csv está vacío"
    return records


def flight_chunk_to_records(df: pd.DataFrame) -> list[dict]:
    df.columns = [c.strip().upper() for c in df.columns]

    records = df.rename(
        columns={
            "YEAR": "year",
            "MONTH": "month",
            "DAY": "day",
            "AIRLINE": "airline",
            "ORIGIN_AIRPORT": "origin_airport",
            "DESTINATION_AIRPORT": "destination_airport",
            "DEPARTURE_DELAY": "departure_delay",
            "ARRIVAL_DELAY": "arrival_delay",
            "CANCELLED": "cancelled",
            "CANCELLATION_REASON": "cancellation_reason",
            "DISTANCE": "distance",
            "AIR_SYSTEM_DELAY": "air_system_delay",
            "AIRLINE_DELAY": "airline_delay",
            "WEATHER_DELAY": "weather_delay",
            "LATE_AIRCRAFT_DELAY": "late_aircraft_delay",
            "SECURITY_DELAY": "security_delay",
        }
    )[
        [
            "year",
            "month",
            "day",
            "airline",
            "origin_airport",
            "destination_airport",
            "departure_delay",
            "arrival_delay",
            "cancelled",
            "cancellation_reason",
            "distance",
            "air_system_delay",
            "airline_delay",
            "weather_delay",
            "late_aircraft_delay",
            "security_delay",
        ]
    ].to_dict(orient="records")

    return records


def load_flights_in_chunks(session: Session, data_dir: Path) -> None:
    flights_path = data_dir / "flights.csv"
    total_inserted = 0

    for i, chunk in enumerate(
        pd.read_csv(flights_path, nrows=FLIGHTS_NROWS, chunksize=FLIGHTS_CHUNK_SIZE),
        start=1,
    ):
        records = flight_chunk_to_records(chunk)
        assert len(records) > 0, f"Chunk {i} de flights está vacío"

        session.execute(insert(Flight), records)
        session.commit()

        total_inserted += len(records)
        logger.info(
            "Chunk %d de flights insertado: %d filas | acumulado: %d",
            i,
            len(records),
            total_inserted,
        )

    assert total_inserted == FLIGHTS_NROWS, (
        f"Se insertaron {total_inserted} filas en flights y se esperaban {FLIGHTS_NROWS}"
    )


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    try:
        logger.info("Leyendo credenciales")
        creds = get_credentials(args.secret_name, args.region)

        logger.info("Creando engine")
        engine = create_engine(build_connection_url(args.host, creds))

        airlines_records = load_airlines(data_dir)
        airports_records = load_airports(data_dir)

        with Session(engine) as session:
            logger.info("Limpiando tablas antes de insertar")
            truncate_tables(session)

            logger.info("Insertando airlines: %d filas", len(airlines_records))
            session.execute(insert(Airline), airlines_records)
            session.commit()

            logger.info("Insertando airports: %d filas", len(airports_records))
            session.execute(insert(Airport), airports_records)
            session.commit()

            logger.info("Insertando flights en chunks de %d", FLIGHTS_CHUNK_SIZE)
            load_flights_in_chunks(session, data_dir)

        logger.info("Carga finalizada correctamente")

    except Exception:
        logger.exception("Falló la carga de datos")
        sys.exit(1)


if __name__ == "__main__":
    main()
