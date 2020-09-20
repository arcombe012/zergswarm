import os
import setuptools
from setuptools.config import read_configuration

cfg_dict = read_configuration("setup.cfg")


setuptools.setup(
    test_suite="nose.collector",
    package_data={'': ['settings.ini_template']},
    **cfg_dict["metadata"]
)
