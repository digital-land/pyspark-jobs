import os
import sys

import pg8000
import pytest
from sedona.spark import SedonaContext
from testcontainers.postgres import PostgresContainer

from jobs.utils.db_url import parse_database_url


@pytest.fixture(scope="session")
def spark():
    """Create a local Sedona-enabled Spark session shared across all tests."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SedonaContext.builder()
        .master("local[1]")
        .appName("TestSession")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1,"
            "org.postgresql:postgresql:42.7.4,"
            "io.delta:delta-spark_2.12:3.2.0",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    sedona = SedonaContext.create(session)
    yield sedona
    session.stop()


@pytest.fixture(scope="session")
def db_url():
    """Provide a PostGIS database URL.

    Uses DATABASE_URL env var if set (CI), otherwise starts a
    testcontainers PostGIS instance (local development).
    """
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        if "sslmode" not in env_url:
            env_url = f"{env_url}?sslmode=disable"
        yield env_url
    else:
        with PostgresContainer(
            image="postgis/postgis:14-master",
            username="postgres",
            password="postgres",
            dbname="test_db",
        ) as container:
            url = (
                container.get_connection_url()
                .replace("psycopg2", "pg8000")
                .replace("+pg8000", "")
            )
            yield f"{url}?sslmode=disable"


@pytest.fixture(scope="session")
def db_conn(db_url):
    """Provide a pg8000 connection to the test database."""
    params = parse_database_url(db_url)
    params.pop("ssl_context", None)
    params.pop("timeout", None)
    conn = pg8000.connect(**params)
    yield conn
    conn.close()


@pytest.fixture()
def clean_entity_table(db_conn):
    """Create the entity table before each test and truncate after."""
    cur = db_conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS entity (
            entity BIGINT,
            name TEXT,
            entry_date DATE,
            start_date DATE,
            end_date DATE,
            dataset TEXT,
            json JSONB,
            organisation_entity BIGINT,
            prefix TEXT,
            reference TEXT,
            typology TEXT,
            geojson JSONB,
            geometry GEOMETRY(MULTIPOLYGON, 4326),
            point GEOMETRY(POINT, 4326),
            quality TEXT
        );
        """
    )
    db_conn.commit()
    cur.close()

    yield

    cur = db_conn.cursor()
    cur.execute("TRUNCATE TABLE entity;")
    # Drop any leftover staging tables
    cur.execute(
        """
        DO $$
        DECLARE t TEXT;
        BEGIN
            FOR t IN SELECT tablename FROM pg_tables
                     WHERE schemaname = 'public'
                       AND tablename LIKE 'entity_staging_%'
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || t;
            END LOOP;
        END $$;
        """
    )
    db_conn.commit()
    cur.close()


# Mimicks table in actual Postgres db, see models here https://github.com/digital-land/digital-land.info/blob/main/application/db/models.py
@pytest.fixture()
def clean_old_entity_table(db_conn):
    """Create the old_entity table before each test and truncate after."""
    cur = db_conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS old_entity (
            old_entity BIGINT,
            status INT,
            entity BIGINT,
            notes TEXT,
            end_date DATE,
            entry_date DATE,
            start_date DATE,
            dataset TEXT
        );
        """
    )
    db_conn.commit()
    cur.close()

    yield

    cur = db_conn.cursor()
    cur.execute("TRUNCATE TABLE old_entity;")
    db_conn.commit()
    cur.close()


@pytest.fixture()
def clean_entity_subdivided_table(db_conn):
    """Create the entity_subdivided table before each test and truncate after."""
    cur = db_conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_subdivided (
            entity BIGINT,
            dataset TEXT,
            geometry_subdivided GEOMETRY(MULTIPOLYGON, 4326)
        );
        """
    )
    db_conn.commit()
    cur.close()

    yield

    cur = db_conn.cursor()
    cur.execute("TRUNCATE TABLE entity_subdivided;")
    # Drop any leftover staging tables
    cur.execute(
        """
        DO $$
        DECLARE t TEXT;
        BEGIN
            FOR t IN SELECT tablename FROM pg_tables
                     WHERE schemaname = 'public'
                       AND tablename LIKE 'entity_subdivided_staging_%'
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || t;
            END LOOP;
        END $$;
        """
    )
    db_conn.commit()
    cur.close()


@pytest.fixture()
def clean_provision_quality_table(db_conn):
    """Create the provision_quality table before each test, truncate after."""
    cur = db_conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS provision_quality (
            dataset TEXT NOT NULL,
            organisation TEXT NOT NULL,
            organisation_name TEXT,
            has_active_endpoint BOOLEAN NOT NULL,
            has_active_resource BOOLEAN NOT NULL,
            owns_entities BOOLEAN NOT NULL,
            is_designated_provider BOOLEAN NOT NULL,
            quality TEXT,
            entity_count BIGINT NOT NULL,
            quality_score DOUBLE PRECISION,
            PRIMARY KEY (dataset, organisation)
        );
        """
    )
    db_conn.commit()
    cur.close()

    yield

    cur = db_conn.cursor()
    cur.execute("TRUNCATE TABLE provision_quality;")
    cur.execute(
        """
        DO $$
        DECLARE t TEXT;
        BEGIN
            FOR t IN SELECT tablename FROM pg_tables
                     WHERE schemaname = 'public'
                       AND tablename LIKE 'provision_quality_staging_%'
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || t;
            END LOOP;
        END $$;
        """
    )
    db_conn.commit()
    cur.close()


@pytest.fixture()
def clean_dataset_quality_table(db_conn):
    """Create the dataset_quality table before each test, truncate after."""
    cur = db_conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_quality (
            dataset TEXT NOT NULL,
            authoritative_organisations INTEGER NOT NULL,
            some_organisations INTEGER NOT NULL,
            total_organisations INTEGER NOT NULL,
            total_entities BIGINT NOT NULL,
            quality_score DOUBLE PRECISION,
            PRIMARY KEY (dataset)
        );
        """
    )
    db_conn.commit()
    cur.close()

    yield

    cur = db_conn.cursor()
    cur.execute("TRUNCATE TABLE dataset_quality;")
    cur.execute(
        """
        DO $$
        DECLARE t TEXT;
        BEGIN
            FOR t IN SELECT tablename FROM pg_tables
                     WHERE schemaname = 'public'
                       AND tablename LIKE 'dataset_quality_staging_%'
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || t;
            END LOOP;
        END $$;
        """
    )
    db_conn.commit()
    cur.close()
