#!/usr/bin/env python

from pip.req import parse_requirements
from setuptools import find_packages, setup

install_reqs = parse_requirements('requirements.txt', session=False)
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='asyncexec',
    version='0.2.2',
    packages=find_packages(),
    include_package_data=True,
    install_requires=reqs,
)
