import json

from client import Engine
from config import logger, settings
from transformer import transform


def main():
    logger.info("Initializing Scraper Engine")
    engine = Engine(settings.CLIENT_ID, settings.CLIENT_SECRET)
    engine.run()
    logger.info("Transforming Data")
    transformed_dfs = transform(engine)
    logger.info("Preparing Database Inserter")

    for name, df in transformed_dfs.items():
        df.to_csv(f"{name}.csv")

    logger.info("Application completed successfully")
    return


if __name__ == "__main__":
    main()
