from sqlalchemy import text

from config import settings
from database import MSSQLDatabaseConnection, PandasSQLDataInserter


def create_inserter_objects() -> PandasSQLDataInserter:
    db_instance = MSSQLDatabaseConnection(
        settings.MSSQL_SERVER,
        settings.MSSQL_DATABASE,
        settings.MSSQL_USERNAME,
        settings.MSSQL_PASSWORD,
    )
    data_inserter = PandasSQLDataInserter(
        db_instance, max_retries=settings.INSERTER_MAX_RETRIES
    )

    return data_inserter


def fetch_isins() -> list[str]:
    db_instance = MSSQLDatabaseConnection(
        settings.MSSQL_SERVER,
        settings.MSSQL_DATABASE,
        settings.MSSQL_USERNAME,
        settings.MSSQL_PASSWORD,
    )
    query = settings.DB_ISIN_QUERY
    if db_instance.engine is None:
        db_instance.connect()

    with db_instance.engine.connect() as connection:
        result = connection.execute(text(query))
        isins = [row[0] for row in result]

    return isins
