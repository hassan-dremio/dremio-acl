#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = ["Click>=7.0", "requests>=2.21.0", "confuse", "simplejson", "attrs", "six", "sqlparse", "colorama"]

setup_requirements = [
    "pytest-runner",
]

test_requirements = [
    "pytest>=3",
]

setup(
    author="Deane Harding",
    # python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Refresh the metadata and/or reflections associated with the PDSs in a specified data source",
    entry_points={"console_scripts": ["dremio_acl=dremio_acl.cli:cli"]},
    install_requires=requirements,
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="dremio_acl",
    name="dremio_acl",
    packages=find_packages(include=["dremio_acl", "dremio_acl.*","dremio_client",
            "dremio_client.flight",
            "dremio_client.auth",
            "dremio_client.model",
            "dremio_client.util",
            "dremio_client.conf",]),
    extras_require={
        ':python_version == "2.7"': ["futures"],
        ':python_version == "2.6"': ["futures"]
    },
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/deane-dremio/dremio_acl",
    version="0.3.0",
    zip_safe=False,
)
