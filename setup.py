#
# Copyright 2017 Tubular Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from codecs import open
import os
import re

from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))

# Get version
with open(os.path.join(here, 'sparkly/__init__.py'), 'rb') as init_py:
    version = re.search('__version__ = \'([\w.]+)\'', init_py.read().decode('utf-8')).group(1)

# Get the long description from the relevant file
with open(os.path.join(here, 'README.rst'), 'rb') as readme_rst:
    long_description = readme_rst.read().decode('utf-8')

# Get requirements
with open(os.path.join(here, 'requirements.txt')) as requirements_txt:
    requirements = [req for req in requirements_txt.readlines() if re.match(u'^[^#\-\s]', req)]


setup(
    name='sparkly',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=version,

    description='Helpers & syntax sugar for PySpark.',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/Tubular/sparkly',

    # Author details
    author='Tubular Engineering',
    author_email='dev@tubularlabs.com',

    # License
    license='Apache License 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],

    # What does your project relate to?
    keywords='sparkly spark pyspark',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    include_package_data=True,

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=requirements,
    extras_require={
        'kafka': ['kafka-python>=1.2.2,<1.3'],
        'test': ['cassandra-driver>=3.7,<3.8', 'PyMySQL>=0.7,<0.8', 'kafka-python>=1.2.2,<1.3'],
    },
)
