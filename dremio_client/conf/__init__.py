# -*- coding: utf-8 -*-

import yaml
from six import StringIO
from .config_parser import build_config
from .cli_helper import get_base_url_token

__all__ = ["build_config", "get_base_url_token", "to_dict"]


def to_dict(config):
    ys = config.dump(redact=True)
    fd = StringIO(ys)
    dct = yaml.safe_load(fd)
    return dct
