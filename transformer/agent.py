import datetime

import pandas as pd

from config.settings import APP_ID, FUND_FIELDS, ISSUER_FIELDS


def transform_issuers(data):
    if not data:
        return None

    records = []
    for coverage, v in data.items():
        records.extend([{**row, "COVERAGE": coverage} for row in v])

    df = pd.DataFrame(records)
    columns = [*ISSUER_FIELDS.split(","), "COVERAGE"]
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
    df["timestamp_created_utc"] = datetime.datetime.utcnow()
    return df


def transform_instrument_history(data):
    if not data:
        return None

    result = []

    for isnt in data:
        base_row = {
            "instrument": isnt["requested_id"],
            "instrument_type": isnt.get("instrument_type", ""),
        }
        for factor in isnt["factors"]:
            name = factor["name"]
            values = factor["data_values"]
            for value in values:
                sub_row = {
                    **base_row,
                    "factor": name,
                    "value": value["value"],
                    "date": value["as_of_date"],
                }
                result.append(sub_row)

    df = pd.DataFrame(result)
    df_pivot = df.pivot_table(
        index=["instrument", "instrument_type", "date"],
        columns="factor",
        values="value",
        aggfunc="first",
    ).reset_index()

    df_pivot.columns.name = None
    df_pivot.columns = df_pivot.columns.tolist()
    df_pivot = df_pivot.sort_values(by=["instrument", "date"])
    df_pivot["timestamp_created_utc"] = datetime.datetime.utcnow()
    return df_pivot


def transform(engine):
    APP_ID_MAP = {
        "LIVE": {
            "etl.msci_issuer": (transform_issuers, engine.issuers),
            "etl.msci_funds": (transform_funds, engine.funds),
        },
        "INST_HIST": {
            "etl.msci_instrument_history": (
                transform_instrument_history,
                engine.instruments_history,
            )
        },
    }

    return {k: f(a) for k, (f, a) in APP_ID_MAP[APP_ID].items()}
