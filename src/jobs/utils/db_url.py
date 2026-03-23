"""
Utilities for parsing and building PostgreSQL database URLs.

Supports the standard format: postgresql://user:password@host:port/dbname
"""

import ssl
from urllib.parse import parse_qs, quote_plus, unquote, urlparse


def parse_database_url(url):
    """
    Parse a PostgreSQL database URL into connection parameters.

    Args:
        url: URL in format postgresql://user:password@host:port/dbname

    Returns:
        dict with keys: database, host, port, user, password, timeout, ssl_context
    """
    parsed = urlparse(url)

    if parsed.scheme not in ("postgresql", "postgres"):
        raise ValueError(
            f"Unsupported scheme: {parsed.scheme}. Expected 'postgresql' or 'postgres'"
        )

    if not parsed.hostname:
        raise ValueError("Database URL must include a hostname")

    if not parsed.path or parsed.path == "/":
        raise ValueError("Database URL must include a database name")

    query_params = parse_qs(parsed.query)
    sslmode = query_params.get("sslmode", ["require"])[0]

    conn_params = {
        "database": parsed.path.lstrip("/"),
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "timeout": 300,
    }

    if sslmode != "disable":
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        conn_params["ssl_context"] = ssl_context

    return conn_params


def build_database_url(conn_params):
    """
    Build a PostgreSQL database URL from connection parameters.

    Args:
        conn_params: dict with keys: database, host, port, user, password

    Returns:
        URL string in format postgresql://user:password@host:port/dbname
    """
    user = quote_plus(conn_params["user"])
    password = quote_plus(conn_params["password"])
    host = conn_params["host"]
    port = conn_params.get("port", 5432)
    database = conn_params["database"]

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"
