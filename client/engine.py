import concurrent.futures
import datetime
import json
import traceback

from client import MSCI
from config import logger, settings
from database.helper import fetch_isins


class Engine:

    APP_ID_MAP = {
        "LIVE": ["get_issuers", "get_funds"],
        "INST_HIST": ["get_instruments_history"],
    }

    def __init__(self, app_id, client_id, client_secret, db_instance):
        if app_id not in self.APP_ID_MAP:
            raise ValueError(f"Invalid value for APP_ID: {app_id}")

        self.app_id = app_id
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
        logger.info(f"APP MODE: {self.app_id}")
        logger.info("Login successful")
        functions = [getattr(self, attr) for attr in self.APP_ID_MAP[self.app_id]]
        if self.app_id == "LIVE":
            self.coverages = self.msci.get_coverages()
            logger.info(f"Retrieved coverages: {self.coverages}")
        else:
            self.db_isins = fetch_isins(self.db_instance)
            logger.info(f"Retrieved ISINs from database: {len(self.db_isins)}")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(func) for func in functions]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception:
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
                except Exception:
                    logger.error(
                        f"Error occurred during issuer fetching: {traceback.format_exc()}"  # noqa: E501
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

            progress = int(len(self.funds) / total_count * 100)
            if not progress % 5:
                logger.info(f"Funds Progress: {progress}%")

            self.funds.extend(funds_data)
            cursor += params["limit"]
            if cursor >= total_count:
                logger.info("Finished fetching funds")
                break

    def get_instruments_history(self):
        logger.info(
            f"Fetching instruments history for last {settings.INSTRUMENT_TIMEDELTA_DAYS} days"  # noqa: E501
        )

        for i in range(0, len(self.db_isins), 100):
            try:
                self._get_instruments_history(self.db_isins[i: i + 100])
            except Exception:
                logger.error(f"self.db_isins[{i} : {i} + 100] {traceback.format_exc()}")
                continue

            progress = round(i / len(self.db_isins) * 100, 2)
            if not progress * 100 % 5:
                logger.info(f"Instruments Progress: {progress}%")

        notfetched_isins_count = len(self.db_isins) - len(self.instruments_history)
        if notfetched_isins_count:
            logger.warning(f"{notfetched_isins_count} ISINs were not returned by API.")

        logger.info("Finished fetching instruments history")

    def _get_instruments_history(self, batch_isin):
        data = {
            "instrument_identifier_list": batch_isin,
            "factor_name_list": settings.INSTRUMENT_FIELDS.split(","),
            "start_date": self.time_delta_date(settings.INSTRUMENT_TIMEDELTA_DAYS),
            "end_date": self.today_date(),
            "data_sample_frequency": "business_month_end",
            "data_layout": "by_factor",
        }
        data = self.msci.get_instruments_history(json.dumps(data))["result"]["data"]
        self.instruments_history.extend(data)

    @staticmethod
    def get_issuers_params(coverage):
        return {
            "coverage": coverage,
            "factor_name_list": settings.ISSUER_FIELDS,
            "limit": 1000,
        }

    @staticmethod
    def get_funds_params():
        return {
            "factor_name_list": settings.FUND_FIELDS,
            "limit": 10000,
        }

    @staticmethod
    def today_date():
        return datetime.date.today().strftime("%Y-%m-%d")

    @staticmethod
    def time_delta_date(days):
        return (datetime.date.today() - datetime.timedelta(days)).strftime("%Y-%m-%d")
