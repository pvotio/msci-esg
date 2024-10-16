import requests

from client import init_session
from config import logger, settings


class MSCI:

    AUTH_URL = "https://accounts.msci.com/oauth/token"
    BASE_URL = "https://api.msci.com/esg/data/v3.0/"

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.req = init_session(
            settings.INSERTER_MAX_RETRIES, settings.REQUEST_BACKOFF_FACTOR
        )
        self.token = None

    def request(self, method, *args, **kwargs):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "*/*",
            "Content-Type": "application/json",
        }
        kwargs["headers"] = headers

        try:
            response = self.req.request(method, *args, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {args[0]}: {str(e)}")
            raise

    def login(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "audience": "https://esg/data",
        }
        try:
            response = self.req.post(self.AUTH_URL, data=data)
            response.raise_for_status()
            self.token = response.json().get("access_token")
            if self.token:
                logger.info("Successfully authenticated and retrieved access token.")
            else:
                logger.error("Authentication failed: no access token in response.")
                raise ValueError("Authentication failed: No access token retrieved.")
            return self.token
        except requests.exceptions.RequestException as e:
            logger.error(f"Login failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during login: {str(e)}")
            raise

    def get_issuers(self, params=None):
        url = self.BASE_URL + "issuers"
        return self.request("get", url, params=params).json()

    def get_issuers_history(self, data):
        url = self.BASE_URL + "issuers/history"
        return self.request("post", url, data=data).json()

    def get_funds(self, params=None):
        url = self.BASE_URL + "funds"
        return self.request("get", url, params=params).json()

    def get_funds_history(self, data):
        url = self.BASE_URL + "funds/history"
        return self.request("post", url, data=data).json()

    def get_instruments_history(self, data):
        url = self.BASE_URL + "instruments/history"
        return self.request("post", url, data=data).json()

    def get_factors(self):
        url = self.BASE_URL + "metadata/factors"
        return self.request("get", url).json()

    def get_coverages(self):
        url = self.BASE_URL + "parameterValues/coverages"
        return self.request("get", url).json()["result"]["coverages"]
