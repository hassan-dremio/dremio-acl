# -*- coding: utf-8 -*-

import json
import os
import time

from confuse import ConfigValueError, NotFoundError

from .config import login as config_auth
from .basic import login as basic_auth

__all__ = ["basic_auth", "config_auth", "auth"]


def auth(base_url, config_dict):
    auth_type = config_dict["auth"]["type"].get()
    try:
        return _existing_token(config_dict)
    except KeyError:
        if auth_type == "basic":
            token = config_auth(base_url, config_dict)
            _write_token(token, config_dict)
            return token
    raise NotImplementedError("Auth type is unsupported " + auth_type)


def _write_token(token, config_dict):
    for source in config_dict.sources:
        if source.filename and not source.default:
            directory = os.path.dirname(source.filename)
            with open(os.path.join(directory, "auth.json"), "w") as f:
                json.dump(
                    {
                        "token": token,
                        "timestamp": time.time(),
                        "hostname": config_dict["hostname"].get(),
                        "user": config_dict["auth"]["username"].get(),
                    },
                    f,
                )
            return


def _existing_token(config_dict):
    for source in config_dict.sources:
        if source.filename and not source.default:
            directory = os.path.dirname(source.filename)
            if os.path.exists(os.path.join(directory, "auth.json")):
                with open(os.path.join(directory, "auth.json")) as f:
                    authfile = json.load(f)
                    if _is_valid(authfile, config_dict):
                        return authfile["token"]
    raise KeyError


def _is_valid(authfile, config_dict):
    hostname = authfile["hostname"]
    username = authfile["username"]
    try:
        expected_hostname = config_dict["hostname"].get()
        expected_username = config_dict["auth"]["username"].get()
    except (ConfigValueError, NotFoundError):
        return False
    if hostname != expected_hostname:
        return False
    if username != expected_username:
        return False
    now = time.time()
    then = authfile["timestamp"]
    return (then + 60 * 60 * 10) > now
