from client import Engine
from config import logger, settings
from database.helper import init_db_instance
from transformer import transform


def main():
    logger.info("Preparing Database Connection")
    logger.info("Initializing Scraper Engine")
    engine = Engine(settings.APP_ID, settings.CLIENT_ID, settings.CLIENT_SECRET)
    engine.run()
    logger.info("Transforming Data")
    transformed_dfs = transform(engine)
    logger.info("Preparing Database Inserter")
    db_instance = init_db_instance()
    for table_name, df in transformed_dfs.items():
        if df is None:
            logger.warning(f"No data to be inserted into {table_name}")
            continue

        logger.info(f"\n{df}")
        db_instance.insert_table(df, table_name)

    logger.info("Application completed successfully")
    return


if __name__ == "__main__":
    main()
