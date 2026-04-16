# Flights Data Engineering End-to-End

Pipeline de datos end-to-end sobre el dataset de vuelos domésticos de Estados Unidos en 2015. El proyecto implementa una arquitectura **Medallion (Bronze → Silver → Gold)** sobre **S3 + Athena + AWS Glue**, un modelo relacional en **PostgreSQL sobre RDS**, consultas analíticas en **DBeaver**, y un notebook con análisis estadístico y series de tiempo.

## Objetivo

El objetivo de este proyecto es construir un pipeline reproducible que permita:

- Ingestar los datos crudos de vuelos.
- Transformarlos a tablas analíticas en Athena.
- Modelarlos también en PostgreSQL.
- Responder preguntas analíticas con SQL.
- Realizar regresión lineal con `statsmodels`.
- Generar pronósticos mensuales con `StatsForecast`.

## Dataset

El dataset contiene tres archivos principales:

- `flights.csv`: ~5.8 millones de vuelos.
- `airlines.csv`: catálogo de aerolíneas.
- `airports.csv`: catálogo de aeropuertos.

Columnas clave de `flights.csv` usadas en el proyecto:

- **Fecha:** `YEAR`, `MONTH`, `DAY`
- **Aerolínea:** `AIRLINE`
- **Origen y destino:** `ORIGIN_AIRPORT`, `DESTINATION_AIRPORT`
- **Retrasos:** `DEPARTURE_DELAY`, `ARRIVAL_DELAY`
- **Cancelación:** `CANCELLED`, `CANCELLATION_REASON`
- **Distancia:** `DISTANCE`
- **Componentes de retraso:** `AIR_SYSTEM_DELAY`, `AIRLINE_DELAY`, `WEATHER_DELAY`, `LATE_AIRCRAFT_DELAY`, `SECURITY_DELAY`

## Arquitectura del proyecto

### 1. Bronze
Ingesta de los tres archivos fuente y almacenamiento en S3 como datasets Parquet registrados en AWS Glue, preservando la estructura base de la fuente para servir como capa de verdad del pipeline.

### 2. Silver
Transformación a **Parquet + Snappy** y construcción de tres agregaciones:
- `flights_daily`
- `flights_monthly`
- `flights_by_airport`

### 3. Gold
Construcción de `flights_gold.vuelos_analitica` con un CTAS en Athena, uniendo vuelos con catálogos de aerolíneas y aeropuertos para producir una tabla analítica desnormalizada.

### 4. PostgreSQL
Modelado relacional de `airlines`, `airports` y `flights` en RDS PostgreSQL usando SQLAlchemy 2.0. La carga se hace contra el endpoint primario y las consultas analíticas contra la read replica.

### 5. Analítica
Consultas SQL en DBeaver y Jupyter, regresión lineal OLS con `statsmodels`, y pronóstico de demanda mensual con `StatsForecast`.

### Resultados principales

- **Regresión OLS**
  - R²: **1.0000**
  - RMSE: **0.0000**
  - *Nota:* Los componentes del retraso explican casi por completo `arrival_delay`, lo que genera un ajuste prácticamente perfecto pero con multicolinealidad fuerte.

- **Pronóstico**
  - Mejor modelo en test: **AutoARIMA**
  - MAE AutoARIMA: **9512.33**
  - MAE AutoETS: **9530.91**
  - MAE AutoTheta: **18735.26**
  - *Nota:* Los intervalos de confianza sugieren alta incertidumbre por contar con solo un año de datos.

---

## Evidencia Gráfica

### Glue Catalog (Bronze, Silver y Gold)
<img width="1659" height="279" alt="image" src="https://github.com/user-attachments/assets/1acb2747-e65c-4423-807a-b62730f7092a" />

**Bronze**
<img width="1646" height="449" alt="image" src="https://github.com/user-attachments/assets/b80307ef-a9a5-4f46-bfcd-0f17f4f7a467" />

**Silver**
<img width="1648" height="418" alt="image" src="https://github.com/user-attachments/assets/62adcebe-a5eb-43e8-8595-62dd96a27f43" />

**Gold**
<img width="1646" height="403" alt="image" src="https://github.com/user-attachments/assets/63c99294-2bae-4795-985b-db5c958ac231" />

### Athena y CloudFormation
**Consulta en Athena**
<img width="1616" height="895" alt="Captura de pantalla 2026-04-09 200100" src="https://github.com/user-attachments/assets/0c4f2662-d827-4d05-b4a6-6a21cca53305" />

### SQLAlchemy
**Conexión con engine**
<img width="1083" height="146" alt="Captura de pantalla 2026-04-10 182831" src="https://github.com/user-attachments/assets/29cd9447-cb5c-4a47-a683-42fa1f123d20" />

**Creación de tablas**
<img width="623" height="165" alt="Captura de pantalla 2026-04-10 183614" src="https://github.com/user-attachments/assets/3190ee2a-2c7a-41f6-8240-31246c34a700" />

### DBeaver (Conexión a Read Replica)
**P1**
<img width="921" height="575" alt="image" src="https://github.com/user-attachments/assets/4662ae3b-c3fb-4853-aa51-5f674eca565d" />

**P2**
<img width="921" height="743" alt="image" src="https://github.com/user-attachments/assets/d3398d4f-a27b-40c8-b3d8-1d16c69a5912" />

**P3**
<img width="921" height="806" alt="image" src="https://github.com/user-attachments/assets/236dcb5d-582c-4f72-aaff-6793fd2c32d4" />

**P4**
<img width="921" height="595" alt="image" src="https://github.com/user-attachments/assets/f5f7c0fa-d4e0-4d9a-ac0e-f8232838775a" />

**P5**
<img width="921" height="693" alt="image" src="https://github.com/user-attachments/assets/e88fc48b-227e-432d-94ad-24f104857c6c" />

**W1**
<img width="921" height="705" alt="image" src="https://github.com/user-attachments/assets/f5d81439-97a1-4e43-8b46-381cb1ad70c7" />

---

## Estructura del repositorio

```text
.
├── .gitignore
├── .python-version
├── README.md
├── etl
│   ├── bronze.py
│   ├── gold.py
│   └── silver.py
├── flights_analytics.ipynb
├── infra
│   └── rds-flights.yaml
├── main.py
├── postgres
│   ├── __init__.py
│   ├── create_tables.py
│   ├── load_data.py
│   ├── models.py
│   └── test_connection.py
├── pyproject.toml
└── uv.lock
```

## Requisitos

- Python 3.12+
- [`uv`](https://docs.astral.sh/uv/)
- AWS CLI configurado
- Acceso a AWS S3, Glue y Athena
- Jupyter Lab / Notebook
- DBeaver Community Edition

## Instalación

### Sincronización del entorno
Sincroniza el entorno del proyecto con `uv`:

```bash
uv sync
```

### Descarga de datos
Los datos fuente se descargan desde el bucket público del curso:

```bash
aws s3 cp s3://itam-analytics-dante/flights-hwk/flights.zip . --no-sign-request
unzip flights.zip -d data/
```


---

## Ejecución del ETL

El pipeline sigue una arquitectura Bronze → Silver → Gold sobre S3, Glue y Athena. En esta implementación, los datos fuente se leen desde CSV, pero las capas almacenadas en S3 quedan en Parquet; Silver además usa compresión Snappy.

### Capa Bronze
Carga los archivos fuente a S3 y registra las tablas en Glue.

```bash
uv run python etl/bronze.py --bucket <tu-bucket> --data-dir data/flights
```
**Salida esperada:**
- Base `flights_bronze` en Glue.
- Tablas `flights`, `airlines`, `airports`.
- Datasets en `s3://<tu-bucket>/flights/bronze/...`

### Capa Silver
Transforma Bronze y construye las tablas agregadas (`flights_daily`, `flights_monthly`, `flights_by_airport`).

```bash
uv run python etl/silver.py --bucket <tu-bucket>
```
**Salida esperada:**
- Base `flights_silver` en Glue.
- `flights_daily` particionada por `MONTH`.
- Tablas en Parquet + Snappy en `s3://<tu-bucket>/flights/silver/...`

### Capa Gold
Construye la tabla analítica final en Athena.

```bash
uv run python etl/gold.py --bucket <tu-bucket>
```
**Salida esperada:**
- Base `flights_gold`.
- Tabla `vuelos_analitica`.
- CTAS exitoso sobre Athena.

---

## Modelo relacional en PostgreSQL

Además del data lake analítico, el proyecto modela los datos en una base relacional PostgreSQL sobre RDS. 

La infraestructura se define en: `infra/rds-flights.yaml`

El stack de CloudFormation crea:
- Instancia RDS PostgreSQL
- Read Replica
- Subnet group
- Security group
- Credenciales en Secrets Manager

### Flujo PostgreSQL
1. Crear el stack `itam-flights-rds` desde CloudFormation.
2. Recuperar `RdsEndpoint`, `RdsReplicaEndpoint` y `RdsSecretName`.
3. Probar conexión al endpoint primario.
4. Crear tablas con SQLAlchemy.
5. Cargar datos.
6. Consultar desde DBeaver usando la Read Replica.

### Ejecución de scripts

**1. Probar conexión**
```bash
uv run python -m postgres.test_connection \
  --host <RDS_ENDPOINT> \
  --secret-name <RDS_SECRET_NAME>
```

**2. Crear tablas**
```bash
uv run python -m postgres.create_tables \
  --host <RDS_ENDPOINT> \
  --secret-name <RDS_SECRET_NAME>
```

**3. Cargar datos**
```bash
uv run python -m postgres.load_data \
  --host <RDS_ENDPOINT> \
  --secret-name <RDS_SECRET_NAME> \
  --data-dir data/flights
```

> **Nota:** Para `flights`, la carga relacional usa solo los primeros 500,000 registros para mantener tiempos razonables de inserción.