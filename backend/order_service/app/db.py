import os
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, quote_plus

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

LOCALHOST_VALUES = {"localhost", "127.0.0.1"}

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "orders")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE")


def _ensure_ssl(database_url: str) -> str:
    parsed = urlparse(database_url)
    hostname = parsed.hostname

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    has_ssl = any(key.lower() == "sslmode" for key, _ in query_pairs)
    if not has_ssl and hostname and hostname not in LOCALHOST_VALUES:
        query_pairs.append(("sslmode", "require"))

    new_query = urlencode(query_pairs)

    username = parsed.username
    password = parsed.password
    auth = ""
    if username:
        auth = quote_plus(username)
        if password is not None:
            auth += f":{quote_plus(password)}"
        auth += "@"

    host_component = parsed.hostname or ""
    if parsed.port:
        host_component = f"{host_component}:{parsed.port}"
    netloc = f"{auth}{host_component}" if host_component else parsed.netloc

    return urlunparse(parsed._replace(netloc=netloc, query=new_query))


def _build_database_url() -> str:
    database_url_env = os.getenv("DATABASE_URL")
    if database_url_env:
        return _ensure_ssl(database_url_env)

    sslmode = POSTGRES_SSLMODE
    if (not sslmode) and POSTGRES_HOST not in LOCALHOST_VALUES:
        sslmode = "require"

    query = urlencode({"sslmode": sslmode}) if sslmode else ""
    query_fragment = f"?{query}" if query else ""

    user = quote_plus(POSTGRES_USER)
    password = quote_plus(POSTGRES_PASSWORD)

    constructed_url = (
        f"postgresql://{user}:{password}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}{query_fragment}"
    )
    return _ensure_ssl(constructed_url)


DATABASE_URL = _build_database_url()

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
