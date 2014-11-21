#/usr/bin/env python

from distutils.core import setup

setup(
  name = 'ic3lib',
  version = '0.1.1',
  author = 'Albert Jornet',
  author_email = 'albert.jornet@ic3.cat',
  url = 'https://github.com/jurnix/ic3tools',
  description = 'Basic IC3 libraries for automatization tasks.',
  license = 'Creative Commons',
  packages = ['automatization'],  # include all packages under src
  package_dir = {'':'src'},   # tell distutils packages are under src
  keywords = ["ic3", "ecmwf", "auto", "automatization"],

)

