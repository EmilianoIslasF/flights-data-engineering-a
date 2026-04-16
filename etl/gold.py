import argparse
import logging
import sys

import awswrangler as wr


DATABASE = "flights_gold"
TABLE = "vuelos_analitica"


def configure_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(__name__)


logger = configure_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construye capa Gold con CTAS en Athena")
    parser.add_argument("--bucket", required=True, help="Bucket destino en S3")
    return parser.parse_args()


def gold_table_path(bucket: str) -> str:
    return f"s3://{bucket}/flights/gold/{TABLE}/"


def athena_output_path(bucket: str) -> str:
    return f"s3://{bucket}/athena-results/"


def build_ctas_sql(bucket: str) -> str:
    output_path = gold_table_path(bucket)

    return f"""
    CREATE TABLE {DATABASE}.{TABLE}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = '{output_path}'
    ) AS
    SELECT
        f.year,
        f.month,
        f.day,
        f.origin_airport,
        ap_orig.airport AS origin_airport_name,
        ap_orig.city AS origin_city,
        ap_orig.state AS origin_state,
        f.destination_airport,
        ap_dest.airport AS destination_airport_name,
        al.airline AS airline_name,
        f.departure_delay,
        f.arrival_delay,
        f.cancelled,
        f.cancellation_reason,
        f.distance,
        f.air_system_delay,
        f.airline_delay,
        f.weather_delay,
        f.late_aircraft_delay,
        f.security_delay
    FROM flights_bronze.flights f
    LEFT JOIN flights_bronze.airlines al
        ON f.airline = al.iata_code
    LEFT JOIN flights_bronze.airports ap_orig
        ON f.origin_airport = ap_orig.iata_code
    LEFT JOIN flights_bronze.airports ap_dest
        ON f.destination_airport = ap_dest.iata_code
    """


def create_gold_table(bucket: str) -> None:
    query = build_ctas_sql(bucket)

    wr.athena.read_sql_query(
        sql=query,
        database=DATABASE,
        s3_output=athena_output_path(bucket),
        ctas_approach=False,
    )


def validate_gold_table(bucket: str) -> None:
    preview = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {DATABASE}.{TABLE} LIMIT 5",
        database=DATABASE,
        s3_output=athena_output_path(bucket),
        ctas_approach=False,
    )

    assert not preview.empty, "La tabla Gold quedó vacía"
    assert preview["airline_name"].notna().any(), "No se resolvió airline_name"
    assert preview["origin_airport_name"].notna().any(), "No se resolvió origin_airport_name"
    assert preview["destination_airport_name"].notna().any(), "No se resolvió destination_airport_name"

    logger.info("Validación OK. Preview de vuelos_analitica:")
    logger.info("\n%s", preview.to_string(index=False))


def main() -> None:
    args = parse_args()

    try:
        logger.info("Iniciando Gold ETL")
        logger.info("Bucket destino: %s", args.bucket)

        wr.catalog.create_database(name=DATABASE, exist_ok=True)
        wr.catalog.delete_table_if_exists(database=DATABASE, table=TABLE)
        wr.s3.delete_objects(path=gold_table_path(args.bucket))

        create_gold_table(args.bucket)
        validate_gold_table(args.bucket)

        logger.info("Gold ETL finalizado correctamente")

    except Exception:
        logger.exception("Error en Gold ETL")
        sys.exit(1)


if __name__ == "__main__":
    main()
