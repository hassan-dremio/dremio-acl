# -*- coding: utf-8 -*-

import time
from concurrent.futures.thread import ThreadPoolExecutor

from ..error import DremioException
from ..model.endpoints import job_results, job_status, sql


executor = ThreadPoolExecutor(max_workers=8)

_job_states = {
    "NOT_SUBMITTED",
    "STARTING",
    "RUNNING",
    "COMPLETED",
    "CANCELED",
    "FAILED",
    "CANCELLATION_REQUESTED",
    "ENQUEUED",
}
_done_job_states = {"COMPLETED", "CANCELED", "FAILED"}


def run(token, base_url, query, context=None, sleep_time=0.1, ssl_verify=True):
    """ Run a single sql query

    This runs a single sql query against the rest api and returns a json document of the results

    :param token: API token from auth
    :param base_url: base url of Dremio instance
    :param query: valid sql query
    :param context: optional context in which to execute the query
    :param sleep_time: seconds to sleep between checking for finished state
    :param ssl_verify: verify ssl on web requests
    :raise: DremioException if job failed
    :raise: DremioUnauthorizedException if token is incorrect or invalid
    :return: json array of result rows

    :example:

    >>> run('abc', 'http://localhost:9047', 'select * from sys.options')
    [{'record':'1'}, {'record':'2'}]
    """
    assert sleep_time > 0
    job = sql(token, base_url, query, context, ssl_verify=ssl_verify)
    job_id = job["id"]
    while True:
        state = job_status(token, base_url, job_id, ssl_verify=ssl_verify)
        if state["jobState"] == "COMPLETED":
            row_count = state.get("rowCount", 0)
            break
        if state["jobState"] in {"CANCELED", "FAILED"}:
            # todo add info about why did it fail
            raise DremioException("job failed " + str(state), None)
        time.sleep(sleep_time)
    count = 0
    while count < row_count:
        result = job_results(token, base_url, job_id, count, ssl_verify=ssl_verify)
        count += 100
        yield result


def run_async(token, base_url, query, context=None, sleep_time=10, ssl_verify=True):
    """ Run a single sql query asynchronously

    This executes a single sql query against the rest api asynchronously and returns a future for the result

    :param token: API token from auth
    :param base_url: base url of Dremio instance
    :param query: valid sql query
    :param context: optional context in which to execute the query
    :param sleep_time: seconds to sleep between checking for finished state
    :param ssl_verify: verify ssl on web requests
    :raise: DremioException if job failed
    :raise: DremioUnauthorizedException if token is incorrect or invalid
    :return: concurrent.futures.Future for the result

    :example:

    >>> f = run_async('abc', 'http://localhost:9047', 'select * from sys.options')
    >>> f.result()
    [{'record':'1'}, {'record':'2'}]
    """
    return executor.submit(run, token, base_url, query, context, sleep_time, ssl_verify)


def refresh_metadata(token, base_url, table, ssl_verify=True):
    """ Refresh the metadata of a given PDS

    This requests a metadata refresh of a given Physical Dataset

    :param token: API token from auth
    :param base_url: base url of Dremio instance
    :param table: valid dremio table name
    :param ssl_verify: verify ssl on web requests
    :raise: DremioException if job failed
    :raise: DremioUnauthorizedException if token is incorrect or invalid
    :return: None

    :example:

    >>> refresh_metadata('abc', 'http://localhost:9047', 'source.pds')
    """
    res = []
    for x in run(
        token, base_url, "ALTER PDS {} REFRESH METADATA FORCE UPDATE".format(table), sleep_time=2, ssl_verify=ssl_verify
    ):
        res.append(x)
    return res
