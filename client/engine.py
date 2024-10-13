import concurrent.futures
import time
import traceback

from client import MSCI
from config import logger
from config.settings import FUND_FIELDS, ISSUER_FIELDS


class Engine:

    def __init__(self, client_id, client_secret):
        self.msci = MSCI(client_id, client_secret)
        self.coverages = []
        self.issuers = {}
        self.issuers_history = []
        self.funds = []
        self.funds_history = []
        self.instruments_history = []
        self.is_issuers_running = True
        self.is_funds_running = True

    def run(self):
        self.msci.login()
        logger.info("Login successful")
        self.coverages = self.msci.get_coverages()
        logger.info(f"Retrieved coverages: {self.coverages}")
        functions = [
            self.get_issuers,
            # self.get_issuers_history,
            # self.get_funds,
            # self.get_funds_history,
            # self.get_instruments_history,
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

        self.is_issuers_running = False
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
        while self.is_issuers_running:
            time.sleep(1)

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

        self.is_funds_running = False

    def get_funds_history(self):
        while self.is_funds_running:
            time.sleep(1)

    def get_instruments_history(self):
        pass

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
