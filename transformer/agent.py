import datetime

import pandas as pd

from config.settings import FUND_FIELDS, ISSUER_FIELDS


def transform_issuers(data):
    if not data:
        return None

    records = []
    for coverage, v in data.items():
        records.extend([{**row, "COVERAGE": coverage} for row in v])

    df = pd.DataFrame(records)
    columns = [*ISSUER_FIELDS.split(","), "COVERAGE"]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df[columns]
    df["timestamp_created_utc"] = datetime.datetime.utcnow()
    df.drop_duplicates(
        subset=["ISSUER_NAME", "ISSUERID", "ISSUER_ISIN"], keep="first", inplace=True
    )
    df.reset_index(inplace=True, drop=True)
    return df


def transform_funds(data):
    if not data:
        return None

    df = pd.DataFrame(data)
    df = df[FUND_FIELDS.split(",")]
    df = df.loc[:, ~df.columns.duplicated()]
    df["timestamp_created_utc"] = datetime.datetime.utcnow()
    return df


def transform(engine):
    return {
        "etl.msci_issuer": transform_issuers(engine.issuers),
        "etl.msci_funds": transform_funds(engine.funds),
    }
