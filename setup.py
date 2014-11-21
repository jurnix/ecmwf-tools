#/usr/bin/env python

from setuptools import setup
from setuptools import Extension


setup(
  name = 'ic3libs',
  version = '0.1',
  author = 'Albert Jornet',
  author_email = 'albert.jornet@ic3.cat',
  url = '',
  description = 'Basic IC3 libraries for automatization tasks.',
  license = 'Creative Commons',
  py_modules = ['datamanager','ecaccess','bash'],
)

