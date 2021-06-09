# -*- coding: utf-8 -*-

from .data import Root
from .endpoints import catalog as _catalog


def catalog(token, base_url, flight_endpoint, ssl_verify=True):
    cat = Root(token, base_url, flight_endpoint, ssl_verify=ssl_verify)
    data = _catalog(token, base_url, ssl_verify=ssl_verify)
    for item in data["data"]:
        cat.add(item)
    return cat
