# -*- coding: utf-8 -*-

import logging

try:
    import pandas as pd

    NO_PANDAS = False
except ImportError:
    NO_PANDAS = True

from .flight import query as _flight_query
from .odbc import query as _odbc_query
from .util import run as _rest_query


def query(
    token,
    base_url,
    hostname,
    odbc_port,
    flight_port,
    username,
    password,
    ssl_verify,
    sql,
    pandas=True,
    method="flight",
    context=None,
):
    failed = False
    if method == "flight":
        try:
            return _flight_query(
                sql, hostname=hostname, port=flight_port, username=username, password=password, pandas=pandas
            )
        except Exception:
            logging.warning("Unable to run query as flight, downgrading to odbc")
            failed = True
    if method == "odbc" or failed:
        try:
            return _odbc_query(sql, hostname=hostname, port=odbc_port, username=username, password=password)
        except Exception:
            logging.warning("Unable to run query as odbc, downgrading to rest")
    results = _rest_query(token, base_url, sql, ssl_verify=ssl_verify)
    if pandas and not NO_PANDAS:
        return pd.DataFrame(results)
    return list(results)
