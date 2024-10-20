from sqlalchemy import text

from config import settings
from database import PandasSQLDataInserter


def create_inserter_objects(db_connection) -> PandasSQLDataInserter:
    data_inserter = PandasSQLDataInserter(
        db_connection, max_retries=settings.INSERTER_MAX_RETRIES
    )

    return data_inserter


def fetch_isins(db_connection) -> list[str]:
    query = settings.DB_ISIN_QUERY 
    if db_connection.engine is None:
        db_connection.connect()

    with db_connection.engine.connect() as connection:
        result = connection.execute(text(query))
        isins = [row[0] for row in result]

    return isins
