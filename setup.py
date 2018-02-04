#!/usr/bin/env python

from os.path import exists
from setuptools import setup


long_description = open('README.md').read() if exists('README.md') else ''

setup(name='loca',
      version='0.0.1',
      description='',
      url='https://github.com/jhamman/LOCA_Downscaling_Analysis',
      license='Apache',
      packages=['loca'],
      long_description=long_description,
      zip_safe=False)
