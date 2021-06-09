# -*- coding: utf-8 -*-

import os

import confuse


def _get_env_args():
    args = dict()
    for k, v in os.environ.items():
        if "DREMIO_" in k and k != "DREMIO_CLIENTDIR":
            name = k.replace("DREMIO_", "").lower().replace("_", ".")
            if name == "port" or name == "auth.timeout":
                v = int(v)
            elif name == "ssl":
                v = v.lower() in ["true", "1", "t", "y", "yes", "yeah", "yup", "certainly", "uh-huh"]
            args[name] = v
    return args


def build_config(args=None):
    config = confuse.Configuration("dremio_client", __name__)
    if args:
        config.set_args(args, dots=True)
    env_args = _get_env_args()
    config.set_args(env_args, dots=True)
    config["auth"]["password"].redact = True
    return config
