import argparse
import logging
import sys

import awswrangler as wr
import pandas as pd


DATABASE = "flights_silver"
BRONZE_TABLE_PATH = "flights/bronze/flights/"
CHUNK_SIZE = 1_000_000


def configure_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(__name__)


logger = configure_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construye capa Silver")
    parser.add_argument("--bucket", required=True, help="Bucket destino en S3")
    return parser.parse_args()


def bronze_flights_path(bucket: str) -> str:
    return f"s3://{bucket}/{BRONZE_TABLE_PATH}"


def silver_path(bucket: str, table_name: str) -> str:
    return f"s3://{bucket}/flights/silver/{table_name}/"


def validate_output(df: pd.DataFrame, table_name: str, key_columns: list[str]) -> None:
    assert not df.empty, f"{table_name} está vacío"
    for col in key_columns:
        assert col in df.columns, f"{table_name} no contiene la columna clave {col}"
        assert df[col].notna().all(), f"{table_name} tiene nulos en columna clave: {col}"


def prepare_target(bucket: str, table_name: str) -> str:
    path = silver_path(bucket, table_name)
    wr.catalog.delete_table_if_exists(database=DATABASE, table=table_name)
    wr.s3.delete_objects(path=path)
    return path


def read_flights_chunks(bucket: str, columns: list[str]):
    path = bronze_flights_path(bucket)
    return wr.s3.read_parquet(
        path=path,
        dataset=True,
        columns=columns,
        chunked=CHUNK_SIZE,
    )


def finalize_average(sum_col: pd.Series, count_col: pd.Series) -> pd.Series:
    result = sum_col / count_col.replace(0, pd.NA)
    return result.fillna(0)


def build_flights_daily(bucket: str) -> pd.DataFrame:
    logger.info("Construyendo flights_daily")

    columns = [
        "YEAR",
        "MONTH",
        "DAY",
        "DEPARTURE_DELAY",
        "ARRIVAL_DELAY",
        "CANCELLED",
    ]

    partials = []

    for i, chunk in enumerate(read_flights_chunks(bucket, columns), start=1):
        chunk = chunk.copy()
        chunk["CANCELLED"] = chunk["CANCELLED"].fillna(0)

        chunk["is_delayed"] = (chunk["DEPARTURE_DELAY"].fillna(0) > 0).astype(int)
        chunk["is_cancelled"] = (chunk["CANCELLED"] == 1).astype(int)

        dep_valid = chunk["DEPARTURE_DELAY"].where(chunk["CANCELLED"] != 1)
        arr_valid = chunk["ARRIVAL_DELAY"].where(chunk["CANCELLED"] != 1)

        grouped = (
            chunk.assign(
                dep_delay_sum=dep_valid.fillna(0),
                dep_delay_count=dep_valid.notna().astype(int),
                arr_delay_sum=arr_valid.fillna(0),
                arr_delay_count=arr_valid.notna().astype(int),
            )
            .groupby(["YEAR", "MONTH", "DAY"], as_index=False)
            .agg(
                total_flights=("YEAR", "size"),
                total_delayed=("is_delayed", "sum"),
                total_cancelled=("is_cancelled", "sum"),
                dep_delay_sum=("dep_delay_sum", "sum"),
                dep_delay_count=("dep_delay_count", "sum"),
                arr_delay_sum=("arr_delay_sum", "sum"),
                arr_delay_count=("arr_delay_count", "sum"),
            )
        )

        partials.append(grouped)
        logger.info("Chunk %d procesado para flights_daily", i)

    result = (
        pd.concat(partials, ignore_index=True)
        .groupby(["YEAR", "MONTH", "DAY"], as_index=False)
        .agg(
            total_flights=("total_flights", "sum"),
            total_delayed=("total_delayed", "sum"),
            total_cancelled=("total_cancelled", "sum"),
            dep_delay_sum=("dep_delay_sum", "sum"),
            dep_delay_count=("dep_delay_count", "sum"),
            arr_delay_sum=("arr_delay_sum", "sum"),
            arr_delay_count=("arr_delay_count", "sum"),
        )
    )

    result["avg_departure_delay"] = finalize_average(result["dep_delay_sum"], result["dep_delay_count"])
    result["avg_arrival_delay"] = finalize_average(result["arr_delay_sum"], result["arr_delay_count"])

    result = result[
        [
            "YEAR",
            "MONTH",
            "DAY",
            "total_flights",
            "total_delayed",
            "total_cancelled",
            "avg_departure_delay",
            "avg_arrival_delay",
        ]
    ].sort_values(["YEAR", "MONTH", "DAY"]).reset_index(drop=True)

    validate_output(result, "flights_daily", ["YEAR", "MONTH", "DAY"])
    return result


def build_flights_monthly(bucket: str) -> pd.DataFrame:
    logger.info("Construyendo flights_monthly")

    columns = [
        "MONTH",
        "AIRLINE",
        "DEPARTURE_DELAY",
        "ARRIVAL_DELAY",
        "CANCELLED",
    ]

    partials = []

    for i, chunk in enumerate(read_flights_chunks(bucket, columns), start=1):
        chunk = chunk.copy()
        chunk["CANCELLED"] = chunk["CANCELLED"].fillna(0)

        chunk["is_delayed"] = (chunk["DEPARTURE_DELAY"].fillna(0) > 0).astype(int)
        chunk["is_cancelled"] = (chunk["CANCELLED"] == 1).astype(int)

        arr_valid = chunk["ARRIVAL_DELAY"].where(chunk["CANCELLED"] != 1)
        on_time = ((chunk["ARRIVAL_DELAY"] <= 15) & (chunk["ARRIVAL_DELAY"].notna())).astype(int)

        grouped = (
            chunk.assign(
                arr_delay_sum=arr_valid.fillna(0),
                arr_delay_count=arr_valid.notna().astype(int),
                on_time_count=on_time,
            )
            .groupby(["MONTH", "AIRLINE"], as_index=False)
            .agg(
                total_flights=("MONTH", "size"),
                total_delayed=("is_delayed", "sum"),
                total_cancelled=("is_cancelled", "sum"),
                arr_delay_sum=("arr_delay_sum", "sum"),
                arr_delay_count=("arr_delay_count", "sum"),
                on_time_count=("on_time_count", "sum"),
            )
        )

        partials.append(grouped)
        logger.info("Chunk %d procesado para flights_monthly", i)

    result = (
        pd.concat(partials, ignore_index=True)
        .groupby(["MONTH", "AIRLINE"], as_index=False)
        .agg(
            total_flights=("total_flights", "sum"),
            total_delayed=("total_delayed", "sum"),
            total_cancelled=("total_cancelled", "sum"),
            arr_delay_sum=("arr_delay_sum", "sum"),
            arr_delay_count=("arr_delay_count", "sum"),
            on_time_count=("on_time_count", "sum"),
        )
    )

    result["avg_arrival_delay"] = finalize_average(result["arr_delay_sum"], result["arr_delay_count"])
    result["on_time_pct"] = (result["on_time_count"] / result["total_flights"].replace(0, pd.NA) * 100).fillna(0)

    result = result[
        [
            "MONTH",
            "AIRLINE",
            "total_flights",
            "total_delayed",
            "total_cancelled",
            "avg_arrival_delay",
            "on_time_pct",
        ]
    ].sort_values(["MONTH", "AIRLINE"]).reset_index(drop=True)

    validate_output(result, "flights_monthly", ["MONTH", "AIRLINE"])
    return result


def build_flights_by_airport(bucket: str) -> pd.DataFrame:
    logger.info("Construyendo flights_by_airport")

    columns = [
        "ORIGIN_AIRPORT",
        "DEPARTURE_DELAY",
        "CANCELLED",
        "AIR_SYSTEM_DELAY",
        "AIRLINE_DELAY",
        "WEATHER_DELAY",
        "LATE_AIRCRAFT_DELAY",
        "SECURITY_DELAY",
    ]

    partials = []

    for i, chunk in enumerate(read_flights_chunks(bucket, columns), start=1):
        chunk = chunk.copy()
        chunk["CANCELLED"] = chunk["CANCELLED"].fillna(0)

        chunk["is_delayed"] = (chunk["DEPARTURE_DELAY"].fillna(0) > 0).astype(int)
        chunk["is_cancelled"] = (chunk["CANCELLED"] == 1).astype(int)

        dep_valid = chunk["DEPARTURE_DELAY"].where(chunk["CANCELLED"] != 1)

        total_delay_minutes = (
            chunk["AIR_SYSTEM_DELAY"].fillna(0)
            + chunk["AIRLINE_DELAY"].fillna(0)
            + chunk["WEATHER_DELAY"].fillna(0)
            + chunk["LATE_AIRCRAFT_DELAY"].fillna(0)
            + chunk["SECURITY_DELAY"].fillna(0)
        )

        grouped = (
            chunk.assign(
                dep_delay_sum=dep_valid.fillna(0),
                dep_delay_count=dep_valid.notna().astype(int),
                weather_delay_sum=chunk["WEATHER_DELAY"].fillna(0),
                total_delay_minutes_sum=total_delay_minutes,
            )
            .groupby(["ORIGIN_AIRPORT"], as_index=False)
            .agg(
                total_departures=("ORIGIN_AIRPORT", "size"),
                total_delayed=("is_delayed", "sum"),
                total_cancelled=("is_cancelled", "sum"),
                dep_delay_sum=("dep_delay_sum", "sum"),
                dep_delay_count=("dep_delay_count", "sum"),
                weather_delay_sum=("weather_delay_sum", "sum"),
                total_delay_minutes_sum=("total_delay_minutes_sum", "sum"),
            )
        )

        partials.append(grouped)
        logger.info("Chunk %d procesado para flights_by_airport", i)

    result = (
        pd.concat(partials, ignore_index=True)
        .groupby(["ORIGIN_AIRPORT"], as_index=False)
        .agg(
            total_departures=("total_departures", "sum"),
            total_delayed=("total_delayed", "sum"),
            total_cancelled=("total_cancelled", "sum"),
            dep_delay_sum=("dep_delay_sum", "sum"),
            dep_delay_count=("dep_delay_count", "sum"),
            weather_delay_sum=("weather_delay_sum", "sum"),
            total_delay_minutes_sum=("total_delay_minutes_sum", "sum"),
        )
    )

    result["avg_departure_delay"] = finalize_average(result["dep_delay_sum"], result["dep_delay_count"])
    result["pct_weather_delay"] = (
        result["weather_delay_sum"] / result["total_delay_minutes_sum"].replace(0, pd.NA) * 100
    ).fillna(0)

    result = result[
        [
            "ORIGIN_AIRPORT",
            "total_departures",
            "total_delayed",
            "total_cancelled",
            "avg_departure_delay",
            "pct_weather_delay",
        ]
    ].sort_values(["ORIGIN_AIRPORT"]).reset_index(drop=True)

    validate_output(result, "flights_by_airport", ["ORIGIN_AIRPORT"])
    return result


def write_daily(df: pd.DataFrame, bucket: str) -> None:
    table_name = "flights_daily"
    path = prepare_target(bucket, table_name)

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        database=DATABASE,
        table=table_name,
        partition_cols=["MONTH"],
        compression="snappy",
    )

    logger.info("Tabla %s escrita en %s", table_name, path)


def write_standard(df: pd.DataFrame, bucket: str, table_name: str) -> None:
    path = prepare_target(bucket, table_name)

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=DATABASE,
        table=table_name,
        compression="snappy",
    )

    logger.info("Tabla %s escrita en %s", table_name, path)


def main() -> None:
    args = parse_args()

    try:
        logger.info("Iniciando Silver ETL")
        logger.info("Bucket destino: %s", args.bucket)

        wr.catalog.create_database(name=DATABASE, exist_ok=True)

        flights_daily = build_flights_daily(args.bucket)
        write_daily(flights_daily, args.bucket)

        flights_monthly = build_flights_monthly(args.bucket)
        write_standard(flights_monthly, args.bucket, "flights_monthly")

        flights_by_airport = build_flights_by_airport(args.bucket)
        write_standard(flights_by_airport, args.bucket, "flights_by_airport")

        logger.info("Silver ETL finalizado correctamente")

    except Exception:
        logger.exception("Error en Silver ETL")
        sys.exit(1)


if __name__ == "__main__":
    main()
