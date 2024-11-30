from config import settings
from database import MSSQLDatabase


def init_db_instance():
    return MSSQLDatabase()


def fetch_isins() -> list[str]:
    db_instance = init_db_instance()
    query = settings.DB_ISIN_QUERY
    isins = db_instance.select_table(query)["isin"].tolist()
    return isins
