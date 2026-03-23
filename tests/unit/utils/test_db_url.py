"""
Unit tests for db_url utilities.
"""

import pytest

from jobs.utils.db_url import build_database_url, parse_database_url


# -- parse_database_url -------------------------------------------------------


def test_parse_database_url_standard():
    url = "postgresql://myuser:mypass@db.example.com:5432/mydb"
    result = parse_database_url(url)

    assert result["database"] == "mydb"
    assert result["host"] == "db.example.com"
    assert result["port"] == 5432
    assert result["user"] == "myuser"
    assert result["password"] == "mypass"
    assert result["timeout"] == 300
    assert result["ssl_context"] is not None


def test_parse_database_url_postgres_scheme():
    url = "postgres://user:pass@host:5432/db"
    result = parse_database_url(url)
    assert result["database"] == "db"


def test_parse_database_url_defaults_port_to_5432():
    url = "postgresql://user:pass@host/db"
    result = parse_database_url(url)
    assert result["port"] == 5432


def test_parse_database_url_special_characters_in_password():
    url = "postgresql://user:p%40ss%23word@host:5432/db"
    result = parse_database_url(url)
    assert result["password"] == "p@ss#word"


def test_parse_database_url_raises_on_unsupported_scheme():
    with pytest.raises(ValueError, match="Unsupported scheme"):
        parse_database_url("mysql://user:pass@host/db")


def test_parse_database_url_raises_on_missing_hostname():
    with pytest.raises(ValueError, match="hostname"):
        parse_database_url("postgresql:///db")


def test_parse_database_url_raises_on_missing_database():
    with pytest.raises(ValueError, match="database name"):
        parse_database_url("postgresql://user:pass@host:5432")


def test_parse_database_url_raises_on_trailing_slash_only():
    with pytest.raises(ValueError, match="database name"):
        parse_database_url("postgresql://user:pass@host:5432/")


def test_parse_database_url_sslmode_disable():
    url = "postgresql://user:pass@host:5432/db?sslmode=disable"
    result = parse_database_url(url)
    assert "ssl_context" not in result


def test_parse_database_url_ssl_enabled_by_default():
    url = "postgresql://user:pass@host:5432/db"
    result = parse_database_url(url)
    assert "ssl_context" in result


# -- build_database_url -------------------------------------------------------


def test_build_database_url_standard():
    conn_params = {
        "database": "mydb",
        "host": "db.example.com",
        "port": 5432,
        "user": "myuser",
        "password": "mypass",
    }
    result = build_database_url(conn_params)
    assert result == "postgresql://myuser:mypass@db.example.com:5432/mydb"


def test_build_database_url_encodes_special_characters():
    conn_params = {
        "database": "mydb",
        "host": "host",
        "port": 5432,
        "user": "user",
        "password": "p@ss#word",
    }
    result = build_database_url(conn_params)
    assert "p%40ss%23word" in result


def test_build_database_url_defaults_port():
    conn_params = {
        "database": "mydb",
        "host": "host",
        "user": "user",
        "password": "pass",
    }
    result = build_database_url(conn_params)
    assert ":5432/" in result
