from client import Engine
from config import logger, settings
from database import MSSQLDatabaseConnection
from database.helper import create_inserter_objects
from transformer import transform


def main():
    logger.info("Preparing Database Connection")
    db_instance = MSSQLDatabaseConnection(
        settings.MSSQL_SERVER,
        settings.MSSQL_DATABASE,
        settings.MSSQL_USERNAME,
        settings.MSSQL_PASSWORD,
    )
    inserter = create_inserter_objects(db_instance)

    logger.info("Initializing Scraper Engine")
    engine = Engine(
        settings.APP_ID, settings.CLIENT_ID, settings.CLIENT_SECRET, db_instance
    )
    engine.run()
    logger.info("Transforming Data")
    transformed_dfs = transform(engine)
    logger.info("Preparing Database Inserter")

    for table_name, df in transformed_dfs.items():
        if df is None:
            logger.warning(f"No data to be inserted into {table_name}")
            continue

        logger.info(f"\n{df}")
        inserter.insert(df, table_name)
        df.to_csv(f"{table_name}.csv")

    db_instance.disconnect()
    logger.info("Application completed successfully")
    return


if __name__ == "__main__":
    main()
