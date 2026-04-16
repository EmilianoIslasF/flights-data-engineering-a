import argparse
import json
import logging
import sys

import boto3
from sqlalchemy import create_engine, text


def configure_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(__name__)


logger = configure_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prueba conexión a PostgreSQL RDS")
    parser.add_argument("--host", required=True, help="Endpoint primario de RDS")
    parser.add_argument("--secret-name", required=True, help="Secret de Secrets Manager")
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


def main() -> None:
    args = parse_args()

    try:
        logger.info("Leyendo credenciales desde Secrets Manager")
        creds = get_credentials(args.secret_name, args.region)

        logger.info("Creando engine de SQLAlchemy")
        engine = create_engine(build_connection_url(args.host, creds))

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        current_database() AS db_name,
                        current_user AS user_name,
                        version() AS postgres_version
                    """
                )
            ).mappings().one()

        logger.info("Conexión exitosa")
        print(dict(row))

    except Exception:
        logger.exception("Falló la prueba de conexión")
        sys.exit(1)


if __name__ == "__main__":
    main()
