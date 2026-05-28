from sqlalchemy import create_engine

from app2.core.config import load_settings


def get_engine():
    settings = load_settings()
    db_uri = (
        f"postgresql+psycopg2://"
        f"{settings.postgres_user}:{settings.postgres_password}@"
        f"{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )
    return create_engine(db_uri)
