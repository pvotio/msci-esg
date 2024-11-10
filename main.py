from client import Engine
from config import logger, settings
from database.helper import create_inserter_objects
from transformer import transform


def main():
    logger.info("Preparing Database Connection")
    inserter = create_inserter_objects()
    logger.info("Initializing Scraper Engine")
    engine = Engine(settings.APP_ID, settings.CLIENT_ID, settings.CLIENT_SECRET)
    engine.run()
    logger.info("Transforming Data")
    transformed_dfs = transform(engine)
    logger.info("Preparing Database Inserter")
    for table_name, df in transformed_dfs.items():
        if df is None:
            logger.warning(f"No data to be inserted into {table_name}")
            continue

        logger.info(f"\n{df}")
        df.to_csv(f"{table_name}.csv")
        inserter.insert(df, table_name)

    logger.info("Application completed successfully")
    return


if __name__ == "__main__":
    main()
