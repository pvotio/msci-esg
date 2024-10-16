import concurrent.futures
import datetime
import json
import traceback

from client import MSCI
from config import logger
from config.settings import (
    FUND_FIELDS,
    INSTRUMENT_FIELDS,
    ISSUER_FIELDS,
    INSTRUMENT_TIMEDELTA_DAYS,
)
from database.helper import fetch_isins


class Engine:

    def __init__(self, client_id, client_secret, db_instance):
        self.msci = MSCI(client_id, client_secret)
        self.db_instance = db_instance
        self.coverages = []
        self.issuers = {}
        self.issuers_history = []
        self.funds = []
        self.funds_history = []
        self.instruments_history = []

    def run(self):
        self.msci.login()
        logger.info("Login successful")
        self.db_isins = fetch_isins(self.db_instance)
        logger.info(f"Retrieved ISINs from database: {len(self.db_isins)}")
        self.coverages = self.msci.get_coverages()
        logger.info(f"Retrieved coverages: {self.coverages}")
        functions = [
            self.get_issuers,
            self.get_issuers_history,
            self.get_funds,
            self.get_funds_history,
            self.get_instruments_history,
        ]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(func) for func in functions]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        f"Error occurred during execution: {traceback.format_exc()}"
                    )

    def get_issuers(self):
        logger.info("Fetching issuers")

        def fetch_issuers(coverage):
            self.issuers[coverage] = []
            params = self.get_issuers_params(coverage)
            logger.info(f"Fetching issuers for coverage: {coverage}")
            self._get_issuers(params, coverage)

        with concurrent.futures.ThreadPoolExecutor() as executer:
            futures = [
                executer.submit(fetch_issuers, coverage) for coverage in self.coverages
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        f"Error occurred during issuer fetching: {traceback.format_exc()}"
                    )

        logger.info("Finished fetching issuers")

    def _get_issuers(self, params, coverage):
        cursor = 0
        total_count = 0
        while True:
            params["offset"] = cursor
            data = self.msci.get_issuers(params)
            issuers_data = data["result"]["issuers"]
            if not total_count:
                if "paging" in data:
                    total_count = data["paging"]["total_count"]
                else:
                    total_count = len(issuers_data)

            self.issuers[coverage].extend(issuers_data)
            cursor += params["limit"]
            if cursor >= total_count:
                logger.info(f"Finished fetching issuers for {coverage}")
                break

        return

    def get_issuers_history(self):
        pass

    def get_funds(self):
        logger.info("Fetching funds")
        params = self.get_funds_params()
        cursor = 0
        total_count = 0
        while True:
            params["offset"] = cursor
            data = self.msci.get_funds(params)
            funds_data = data["result"]["funds"]
            if not total_count:
                if "paging" in data:
                    total_count = data["paging"]["total_count"]
                else:
                    total_count = len(funds_data)

            logger.info(f"FUNDS progress: {len(self.funds)} / {total_count}")

            self.funds.extend(funds_data)
            cursor += params["limit"]
            if cursor >= total_count:
                logger.info("Finished fetching funds")
                break

    def get_funds_history(self):
        pass

    def get_instruments_history(self):
        logger.info(
            f"Fetching instrument history for last {INSTRUMENT_TIMEDELTA_DAYS} days"
        )
        with concurrent.futures.ThreadPoolExecutor() as executer:
            futures = [
                executer.submit(
                    self._get_instruments_history, self.db_isins[i : i + 100]
                )
                for i in range(0, len(self.db_isins), 100)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        f"Error occurred during instrument fetching: {traceback.format_exc()}"
                    )

        logger.info("Finished fetching instrument history")

    def _get_instruments_history(self, batch_isin):
        data = {
            "instrument_identifier_list": batch_isin,
            "factor_name_list": INSTRUMENT_FIELDS.split(","),
            "start_date": self.time_delta_date(INSTRUMENT_TIMEDELTA_DAYS),
            "end_date": self.today_date(),
            "data_sample_frequency": "business_month_end",
            "data_layout": "by_factor",
        }
        data = self.msci.get_instruments_history(json.dumps(data))["result"]["data"]
        logger.info(f"Fetched {len(data)} instrument(s)")
        self.instruments_history.extend(data)

    @staticmethod
    def get_issuers_params(coverage):
        return {
            "coverage": coverage,
            "factor_name_list": ISSUER_FIELDS,
            "limit": 1000,
        }

    @staticmethod
    def get_funds_params():
        return {
            "factor_name_list": FUND_FIELDS,
            "limit": 10000,
        }

    @staticmethod
    def today_date():
        return datetime.date.today().strftime("%Y-%m-%d")

    @staticmethod
    def time_delta_date(days):
        return (datetime.date.today() - datetime.timedelta(days)).strftime("%Y-%m-%d")
