import argparse
import logging
import sys
from pathlib import Path

import awswrangler as wr
import pandas as pd


DATABASE = "flights_bronze"
CHUNK_SIZE = 500_000

TABLE_CONFIGS = {
    "flights": {
        "filename": "flights.csv",
        "table": "flights",
        "required_columns": [
            "YEAR",
            "MONTH",
            "DAY",
            "AIRLINE",
            "ORIGIN_AIRPORT",
            "DESTINATION_AIRPORT",
            "DEPARTURE_DELAY",
            "ARRIVAL_DELAY",
            "CANCELLED",
            "DISTANCE",
        ],
        "key_columns": ["YEAR", "MONTH", "DAY", "AIRLINE"],
    },
    "airlines": {
        "filename": "airlines.csv",
        "table": "airlines",
        "required_columns": ["IATA_CODE", "AIRLINE"],
        "key_columns": ["IATA_CODE"],
    },
    "airports": {
        "filename": "airports.csv",
        "table": "airports",
        "required_columns": ["IATA_CODE", "AIRPORT", "CITY", "STATE"],
        "key_columns": ["IATA_CODE"],
    },
}


def configure_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(__name__)


logger = configure_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga capa Bronze a S3 + Glue")
    parser.add_argument("--bucket", required=True, help="Bucket destino en S3")
    parser.add_argument("--data-dir", required=True, help="Directorio local con los CSVs")
    return parser.parse_args()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.strip().upper() for col in df.columns]
    return df


def validate_dataframe(
    df: pd.DataFrame,
    required_columns: list[str],
    key_columns: list[str],
    table_name: str,
) -> None:
    assert not df.empty, f"{table_name} está vacío"

    missing_columns = [col for col in required_columns if col not in df.columns]
    assert not missing_columns, f"{table_name} no contiene columnas requeridas: {missing_columns}"

    for col in key_columns:
        assert df[col].notna().all(), f"{table_name} tiene nulos en columna clave: {col}"


def prepare_target(bucket: str, table_name: str) -> str:
    path = f"s3://{bucket}/flights/bronze/{table_name}/"
    wr.catalog.delete_table_if_exists(database=DATABASE, table=table_name)
    wr.s3.delete_objects(path=path)
    return path


def write_full_table(df: pd.DataFrame, bucket: str, table_name: str) -> None:
    path = prepare_target(bucket=bucket, table_name=table_name)

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=DATABASE,
        table=table_name,
    )

    logger.info("Tabla %s cargada con %d filas en %s", table_name, len(df), path)


def process_small_table(data_dir: Path, bucket: str, config: dict) -> None:
    file_path = data_dir / config["filename"]
    table_name = config["table"]

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    logger.info("Leyendo archivo local: %s", file_path)
    df = pd.read_csv(file_path)
    df = normalize_columns(df)

    validate_dataframe(
        df=df,
        required_columns=config["required_columns"],
        key_columns=config["key_columns"],
        table_name=table_name,
    )

    write_full_table(df=df, bucket=bucket, table_name=table_name)


def process_flights_in_chunks(data_dir: Path, bucket: str, config: dict) -> None:
    file_path = data_dir / config["filename"]
    table_name = config["table"]

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    path = prepare_target(bucket=bucket, table_name=table_name)

    total_rows = 0
    wrote_any_chunk = False

    logger.info("Leyendo flights por chunks desde: %s", file_path)

    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=CHUNK_SIZE), start=1):
        chunk = normalize_columns(chunk)

        validate_dataframe(
            df=chunk,
            required_columns=config["required_columns"],
            key_columns=config["key_columns"],
            table_name=table_name,
        )

        mode = "overwrite" if i == 1 else "append"

        wr.s3.to_parquet(
            df=chunk,
            path=path,
            dataset=True,
            mode=mode,
            database=DATABASE,
            table=table_name,
        )

        rows = len(chunk)
        total_rows += rows
        wrote_any_chunk = True

        logger.info("Chunk %d de flights cargado con %d filas", i, rows)

    assert wrote_any_chunk, "No se escribió ningún chunk de flights"
    logger.info("Tabla %s cargada con %d filas en %s", table_name, total_rows, path)


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    try:
        logger.info("Iniciando Bronze ETL")
        logger.info("Bucket destino: %s", args.bucket)
        logger.info("Directorio local: %s", data_dir)

        wr.catalog.create_database(name=DATABASE, exist_ok=True)

        process_flights_in_chunks(
            data_dir=data_dir,
            bucket=args.bucket,
            config=TABLE_CONFIGS["flights"],
        )
        process_small_table(
            data_dir=data_dir,
            bucket=args.bucket,
            config=TABLE_CONFIGS["airlines"],
        )
        process_small_table(
            data_dir=data_dir,
            bucket=args.bucket,
            config=TABLE_CONFIGS["airports"],
        )

        logger.info("Bronze ETL finalizado correctamente")

    except Exception:
        logger.exception("Error en Bronze ETL")
        sys.exit(1)


if __name__ == "__main__":
    main()
