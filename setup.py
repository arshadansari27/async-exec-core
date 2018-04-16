#!/usr/bin/env python

from setuptools import find_packages, setup

install_reqs = open('requirements.txt').readlines()
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='asyncexec',
    version='0.3.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=reqs,
)
